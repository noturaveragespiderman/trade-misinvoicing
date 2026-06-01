"""
MATCH phase (X↔M only).

Inputs
------
Reads ./aggregate/{year}.csv, produced by extract.build_aggregate_csv.
That file already has every row filter applied (typeCode / freqCode /
refYear / classificationSearchCode / partner=0 / cmdCode=0 /
reporterCode=0 / flowCode∉{M,X} / self-trade). The MATCH phase only
needs to normalise cmdCode (zero-pad to canonical HS level) and pair
exports with their mirror imports.

If ./aggregate/{year}.csv is missing, MATCH errors out and tells the
user to run AGGREGATE first. There is no .gz fallback — AGGREGATE is
the single source of truth for filtered rows.

Matching rule (see _assign_match_ids docstring for full detail):
  Q1 — both partner2 == 0:
       M.reporter = X.partner, M.partner = X.reporter, M.cmdCode = X.cmdCode
  Q4 — both partner2 != 0 (declared triangular):
       M.reporter = X.partner2, M.partner = X.reporter,
       M.partner2 = X.partner,  M.cmdCode = X.cmdCode

Outputs
-------
  ./match/{year}_matched.csv         — rows with matched_id populated
  ./match/{year}_unmatched.csv       — rows where matched_id is NULL
  ./match/{year}_match_stats.json    — per-country MATCH-stage stats sidecar
"""

import os
import json
import gzip
import shutil
import time

import numpy as np
import pandas as pd

from app.config import (
    AGGREGATE_DIR,
    MATCH_DIR,
    CLEAN_DIR,
)
from app.core.db import (
    fetch_all_reporters,
    normalize_cmdcode,
    hs_level,
    load_hs_concordance,
)
from app.config import CANONICAL_HS_REVISION
from app.core.notifier import (
    ProgressReporter,
    check_for_stop,
    h,
    notify_user,
    strip_tags,
    PHASE_MATCH,
    STATUS_OK,
    STATUS_RUN,
    STATUS_WARN,
    STATUS_ERR,
    STATUS_SKIP,
    STATUS_DISK,
    STATUS_ZIP,
)
from app.core.logger import get_logger
from app.core import progress as progress_mod

# pyarrow is preferred for read_csv when available — Arrow-backed dtypes
# use ~3-4× less memory than the Python-object equivalents and skip the
# per-cell GIL roundtrip when the C engine pulls bytes into the frame.
try:
    import pyarrow  # noqa: F401

    _ARROW_AVAILABLE = True
except ImportError:  # pragma: no cover
    _ARROW_AVAILABLE = False

_READ_KWARGS = {
    "dtype_backend": "pyarrow" if _ARROW_AVAILABLE else "numpy_nullable",
}

# Numeric ID columns we want as nullable Int64 straight out of read_csv.
_INT_COLUMNS = {"reporterCode", "partnerCode", "partner2Code"}

logger = get_logger(__name__)


# ------------------------------------------------------------------
# Small helpers
# ------------------------------------------------------------------
# `_notify(msg, notify=True)` was a duplicate of the helpers in
# misinvoicing.py. Centralised in `notifier.notify_user`; we keep a
# thin alias here so the existing `_notify(msg, notify)` call sites
# don't need to change shape — just imports.
def _notify(msg, notify=True):
    notify_user(msg, send=notify)


def _meta_map():
    return {r["code"]: r for r in fetch_all_reporters()}


def _name(code, mm):
    info = mm.get(int(code), {})
    n = info.get("fullname") or info.get("name") or "Unknown"
    return f"{str(n)[:40]} ({code})"


def _aggregate_read_path(year_str):
    """Return whichever aggregate variant exists on disk — plain .csv or
    .csv.gz (shrink_year compresses after AGGREGATE completes). Returns
    ``None`` if neither variant exists or is too small to be a valid file."""
    plain = os.path.join(AGGREGATE_DIR, f"{year_str}.csv")
    if os.path.exists(plain) and os.path.getsize(plain) >= 16:
        return plain
    gz = plain + ".gz"
    if os.path.exists(gz) and os.path.getsize(gz) >= 16:
        return gz
    return None


def _open_aggregate_for_text(path):
    """Open a (possibly gzipped) aggregate CSV in text mode for line reads."""
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def _reporters_in_aggregate(year_str, target_codes):
    """
    Return the list of int reporter codes present in
    ``aggregate/{year}.csv`` that are also in ``target_codes``.

    Aggregate is the canonical MATCH input — the reporters it contains
    ARE the universe to be matched. Returns ``[]`` if the aggregate is
    missing or has no reporterCode column.
    """
    csv_path = _aggregate_read_path(year_str)
    if csv_path is None:
        return []
    target_set = {int(c) for c in target_codes}
    try:
        with _open_aggregate_for_text(csv_path) as fh:
            header = fh.readline().strip().split(",")
        col = next((c for c in header if c.lower() == "reportercode"), None)
        if not col:
            return []
        present = set()
        for chunk in pd.read_csv(csv_path, usecols=[col], dtype=str, chunksize=500_000):
            ints = pd.to_numeric(chunk[col], errors="coerce").dropna().astype(int)
            present.update(int(x) for x in ints.unique())
        return sorted(present & target_set)
    except Exception as e:
        logger.warning("Could not enumerate reporters from aggregate %s: %s", csv_path, e)
        return []


# ------------------------------------------------------------------
# Resume-safety helpers — sidecar stats + matches-CSV validity check
# ------------------------------------------------------------------
# When the aggregate-load step writes the match/{year}_matched.csv +
# match/{year}_unmatched.csv pair it ALSO writes a JSON sidecar with
# the per-country MATCH-stage stats. On a subsequent run, if both
# split files + sidecar are present, we skip the merge entirely and
# reuse them — the matching algorithm itself always re-runs (cheap),
# so algo changes still take effect.
def _stats_sidecar_path(year_str):
    return os.path.join(MATCH_DIR, f"{year_str}_match_stats.json")


def _matched_path(year_str):
    """Plain (uncompressed) path used by Step 7 to write the file. Reads
    elsewhere should go through _matched_read_path so they accept the
    .csv.gz variant produced by shrink_year."""
    return os.path.join(MATCH_DIR, f"{year_str}_matched.csv")


def _unmatched_path(year_str):
    return os.path.join(MATCH_DIR, f"{year_str}_unmatched.csv")


def _matched_read_path(year_str):
    """Return whichever variant exists on disk — plain CSV or gzip-compressed."""
    plain = _matched_path(year_str)
    if os.path.exists(plain):
        return plain
    gz = plain + ".gz"
    if os.path.exists(gz):
        return gz
    return None


def _unmatched_read_path(year_str):
    plain = _unmatched_path(year_str)
    if os.path.exists(plain):
        return plain
    gz = plain + ".gz"
    if os.path.exists(gz):
        return gz
    return None


def _merged_intermediate_path(year_str):
    """Intermediate CSV (merged + filtered, no id/matched_id yet) used during
    a fresh MATCH run. Deleted after the split outputs are written."""
    return os.path.join(MATCH_DIR, f"{year_str}_merged.csv")


# Output schema version for the match phase (matched.csv +
# unmatched.csv + this stats sidecar taken together). See
# misinvoicing.OUTPUT_SCHEMA_VERSION for the convention.
#
# Version history:
#   1: original — matched_id reflected raw cmdCode equality.
#   2: A2 — matched_id now reflects HS-revision-canonicalised
#      pairing. Same column set on disk, but the SEMANTICS of
#      matched_id differ across versions (cross-revision rows that
#      share a canonical code now share a matched_id, where v1 left
#      them in different groups). Cache invalidation forces a
#      re-match so downstream phases see the canonical pairing.
#   3: motCode merge — new match/{year}_clean.csv output with one
#      row per (matched_id, flowCode) where motCode is always "0".
#      Per-mode rows are either replaced by an explicit motCode=0
#      row (when present) or summed into a synthetic one. The sidecar
#      gains a `clean = {pairs_case_a, pairs_case_b, pairs_case_c,
#      rows_summed, output_pairs}` field. Aggregate side also dropped
#      the rej_not_reported gate and added rej_customsCode_not_c00,
#      so any v2 cache is no longer trustworthy.
#   4: clean-step priority changed — see history below for the
#      relaxations applied mid-v4 (no schema bump because the column
#      set never changed).
#
# Mid-v4 evolution of clean_matched_csv (column set unchanged the
# whole way, only the row-selection rule moves):
#
#   a. Original v4: per side, agg > zero > synth.
#         (a) keep first isAggregate=1 row verbatim;
#         (b) else keep first motCode='0' row;
#         (c) else synthesise (SUM + ARG_MAX(primaryValue)) and stamp
#             motCode='0'.
#   b. Dedup step removed entirely — matched.csv no longer has
#      ``keep_reason``. Clean ingests matched.csv raw.
#   c. Strict: kept only rows where isAggregate=1 AND motCode='0'.
#      Synth path gone. Sides without such a row dropped.
#   d. Current rule — user request:
#        * isAggregate filter dropped (rows with isAggregate=0 are
#          kept as long as motCode='0').
#        * Per side, pick MIN(id) where motCode='0'.
#        * Per pair, emit ONLY if BOTH X and M sides have such a row.
#      i.e. clean/{year}.csv now has exactly two rows per matched_id —
#      one X, one M, both with motCode='0', any isAggregate value.
#
# Companion misinvoicing change (schema v4): the four quality-based
# drop rules (isReported / isAggregate / fully-estimated /
# unsafe-aggregation) were removed; only drop_no_pair survives.
OUTPUT_SCHEMA_VERSION = 4


def _save_cached_stats(year_str, per_country):
    """
    Persist per-country merge stats + schema-version envelope so a
    skip-merge re-run can restore them and verify they were written
    by the same code generation that's reading them.

    On-disk format (current, v2):
        {
            "output_schema_version": 2,
            "per_country": {"156": {...}, "344": {...}, ...}
        }

    Legacy format (pre-v2) was just the flat per-country dict at
    the top level. ``_load_cached_stats`` detects either shape.
    """
    path = _stats_sidecar_path(year_str)
    try:
        # JSON keys must be strings — int reporter codes serialised as str
        # and converted back on load. ``default=int`` coerces numpy scalars
        # (which ``json`` can't serialise) into native ints.
        serialisable = {str(int(k)): v for k, v in per_country.items()}
        payload = {
            "output_schema_version": OUTPUT_SCHEMA_VERSION,
            "per_country": serialisable,
        }
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=int)
    except Exception as e:
        logger.warning("Failed to write stats sidecar %s: %s", path, e)


def _load_cached_stats(year_str):
    """
    Load the cached match-stage stats sidecar.

    Returns a 2-tuple ``(per_country, output_schema_version)``:
      - ``per_country``: {int reporter code: stats dict}, or None
        if the sidecar is missing / corrupt.
      - ``output_schema_version``: int. Newly-written sidecars carry
        OUTPUT_SCHEMA_VERSION; legacy flat-shape sidecars (pre-v2)
        return ``None`` so the caller treats the cache as stale and
        regenerates.
    """
    path = _stats_sidecar_path(year_str)
    if not os.path.exists(path):
        return None, None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except Exception as e:
        logger.warning("Could not read stats sidecar %s: %s", path, e)
        return None, None

    if isinstance(raw, dict) and "per_country" in raw and "output_schema_version" in raw:
        # Current format.
        try:
            per_country = {int(k): v for k, v in raw["per_country"].items()}
        except Exception:
            return None, None
        try:
            ver = int(raw.get("output_schema_version") or 0) or None
        except (TypeError, ValueError):
            ver = None
        return per_country, ver

    # Legacy format — the whole dict IS per_country, no version.
    try:
        per_country = {int(k): v for k, v in raw.items()}
    except Exception:
        return None, None
    return per_country, None


def _csv_header_has(path, needed_lower):
    """True if `path` exists, is non-trivial, and its header contains every
    column name in `needed_lower` (compared case-insensitively)."""
    if not os.path.exists(path) or os.path.getsize(path) < 16:
        return False
    try:
        with open(path, "r", encoding="utf-8") as fh:
            header = fh.readline().strip().split(",")
    except Exception:
        return False
    lower = {c.strip().lower() for c in header}
    return set(needed_lower).issubset(lower)


def _match_outputs_valid(year_str):
    """
    True if BOTH split output files exist with the expected columns.
    Used to decide whether to skip the merge step on a re-run.
    """
    needed = {"reportercode", "flowcode", "partnercode", "cmdcode"}
    m_path = _matched_read_path(year_str)
    u_path = _unmatched_read_path(year_str)
    if not m_path or not u_path:
        return False
    return _csv_header_has(m_path, needed) and _csv_header_has(u_path, needed)


# ------------------------------------------------------------------
# Load pre-filtered aggregate/{year}.csv into the merged intermediate
# ------------------------------------------------------------------
def _load_from_aggregate_csv(year_str, target_codes, notify=True):
    """
    Read the pre-filtered ``aggregate/{year}.csv`` (built by
    ``extract.build_aggregate_csv``), normalise cmdCode, and write the
    intermediate merged CSV at ``match/{year}_merged.csv``.

    AGGREGATE has already dropped every filter row: typeCode≠C,
    freqCode≠A, refYear≠year, classificationSearchCode≠HS, partner=0,
    cmdCode=0, reporterCode=0, flowCode∉{M,X}, and self-trade. MATCH
    therefore performs no row drops of its own — it just normalises
    cmdCode and tallies per-country / per-flow counts for the report.

    Returns ``(csv_path, per_country_stats, columns)``.
    """
    os.makedirs(MATCH_DIR, exist_ok=True)
    csv_path = _merged_intermediate_path(year_str)
    if os.path.exists(csv_path):
        os.remove(csv_path)

    aggregate_path = _aggregate_read_path(year_str) or os.path.join(
        AGGREGATE_DIR, f"{year_str}.csv"
    )
    target_set = {int(c) for c in target_codes}

    per_country = {
        code: {
            "total_rows": 0,
            "dropped_rows": 0,
            "kept_rows": 0,
            # All row drops happen in AGGREGATE now; counters stay at 0
            # in MATCH but remain in the schema so report builders don't
            # crash on a missing key.
            "rej_typeCode": 0,
            "rej_freqCode": 0,
            "rej_refYear": 0,
            "rej_classificationSearchCode": 0,
            "rej_partner_zero": 0,
            "rej_cmdCode": 0,
            "rej_reporterCode": 0,
            "rej_flowCode": 0,
            "rej_self_trade": 0,
            "flow_kept": {"X": 0, "M": 0},
        }
        for code in target_set
    }

    try:
        agg_size_mb = os.path.getsize(aggregate_path) / (1024 * 1024)
    except OSError:
        agg_size_mb = 0

    _notify(
        f"♻️ <b>MATCH Step 2/4 — Reading "
        f"<code>{h(aggregate_path)}</code></b> ({agg_size_mb:.1f} MB)\n"
        f"  All row filters already applied in AGGREGATE — MATCH only "
        f"normalises cmdCode and pairs X↔M.",
        notify,
    )

    final_cols = None
    chunk_no = 0
    with _open_aggregate_for_text(aggregate_path) as fh:
        header = [c.strip() for c in fh.readline().strip().split(",")]
    int_dtype_map = {c: "Int64" for c in _INT_COLUMNS if c in header}

    try:
        reader = pd.read_csv(
            aggregate_path,
            dtype=int_dtype_map,
            chunksize=500_000,
            low_memory=False,
            encoding_errors="replace",
            engine="c",
            **_READ_KWARGS,
        )
    except Exception as e:
        _notify(f"⚠️ Cannot read {aggregate_path}: {e}", notify)
        raise

    tmp_path = csv_path + ".tmp"
    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    out_fh = open(tmp_path, "w", encoding="utf-8", newline="")

    try:
        for chunk in reader:
            check_for_stop()
            chunk_no += 1
            chunk.columns = [c.strip() for c in chunk.columns]
            n = len(chunk)

            rc_series = chunk.get("reporterCode")
            if rc_series is None:
                continue
            rc_int = rc_series.fillna(-1).astype("int64")
            chunk["_rc_int"] = rc_int

            for code, count in rc_int.value_counts().items():
                code = int(code)
                if code in per_country:
                    per_country[code]["total_rows"] += int(count)

            in_scope = rc_int.isin(target_set)
            chunk = chunk[in_scope].copy()
            if chunk.empty:
                continue

            for code, count in chunk["_rc_int"].value_counts().items():
                per_country[int(code)]["kept_rows"] += int(count)

            # Vectorised cmdCode normalisation.
            if "cmdCode" in chunk.columns:
                cmd_series = chunk["cmdCode"].astype(str).str.strip()
                lookup = {c: normalize_cmdcode(c)[0] for c in cmd_series.unique()}
                padded = cmd_series.map(lookup)
                keep_mask = padded.notna()
                chunk = chunk.loc[keep_mask].copy()
                chunk["cmdCode"] = padded[keep_mask]

            if "flowCode" in chunk.columns and not chunk.empty:
                chunk["flowCode"] = chunk["flowCode"].astype(str).str.upper().str.strip()
                tally = chunk.groupby(["_rc_int", "flowCode"]).size()
                for (code, fl), cnt in tally.items():
                    code = int(code)
                    per_country[code]["flow_kept"][fl] = per_country[code][
                        "flow_kept"
                    ].get(fl, 0) + int(cnt)

            chunk = chunk.drop(columns=["_rc_int"])

            if final_cols is None:
                final_cols = list(chunk.columns)
                out_fh.write(",".join(final_cols) + "\n")
            else:
                for c in final_cols:
                    if c not in chunk.columns:
                        chunk[c] = ""
                chunk = chunk[final_cols]

            chunk.to_csv(out_fh, index=False, header=False, lineterminator="\n")
    finally:
        out_fh.flush()
        try:
            os.fsync(out_fh.fileno())
        except OSError:
            pass
        out_fh.close()

    if final_cols is None:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
    else:
        os.replace(tmp_path, csv_path)

    tot = sum(s["total_rows"] for s in per_country.values())
    kept_n = sum(s["kept_rows"] for s in per_country.values())
    flow_X = sum(s["flow_kept"].get("X", 0) for s in per_country.values())
    flow_M = sum(s["flow_kept"].get("M", 0) for s in per_country.values())
    try:
        csv_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    except OSError:
        csv_size_mb = 0

    if tot:
        msg = (
            f"✅ <b>Aggregate-CSV load complete</b> → <code>{h(csv_path)}</code>\n"
            f"  File size: {csv_size_mb:.1f} MB\n"
            f"  Rows in scope: {kept_n:,} of {tot:,}\n"
            f"  Flows → X:{flow_X:,} • M:{flow_M:,}\n"
            f"  All row drops happened upstream in AGGREGATE."
        )
    else:
        msg = "⚠️ Aggregate-CSV load produced no rows"
    _notify(msg, notify)
    _save_cached_stats(year_str, per_country)
    return csv_path, per_country, final_cols


# ------------------------------------------------------------------
# Step 5+6 — load filtered CSV, add id + matched_id, match
# ------------------------------------------------------------------
def _assign_match_ids(df, notify=True):
    """
    Populate df['matched_id'] using the Step-6 algorithm.

    df must contain columns: id, reporterCode, flowCode, partnerCode,
    partner2Code, cmdCode. Modifies df in place and returns (df, diag).

    partner2 handling — strict same-bucket with triangular field alignment:

      Comtrade convention:
        X (export):  partner  = next destination, partner2 = final destination
        M (import):  partner  = country of origin, partner2 = country of consignment

      Direct bilateral trade A↔B (both sides partner2 == 0):
        X(A): reporter=A, partner=B, partner2=0
        M(B): reporter=B, partner=A, partner2=0
        Field alignment:  X.reporter↔M.partner,  X.partner↔M.reporter

      Triangular trade A→H→C (both sides specific partner2):
        X(A): reporter=A, partner=H, partner2=C
        M(C): reporter=C, partner=A, partner2=H
        Field alignment:
          X.reporter (A)  ↔ M.partner (A)    — origin
          X.partner2 (C)  ↔ M.reporter (C)   — final destination
          X.partner  (H)  ↔ M.partner2 (H)   — intermediary (different fields!)

      Quadrants 2 and 3 (one side declares routing, the other doesn't) are
      intentionally left unmatched — they cannot be aligned without making
      assumptions about which side's omission should win.

      Two M indexes:
        m_zero      — Ms with partner2 == 0, keyed by (reporter, partner, cmd)
                      Looked up with (X.partner, X.reporter, X.cmd).
        m_specific  — Ms with partner2 != 0, keyed by (reporter, partner,
                      partner2, cmd).  Looked up with the triangular swap
                      (X.partner2, X.reporter, X.partner, X.cmd).

    NaN sentinel — partnerCode / partner2Code missing values are coerced
    to -1 (an impossible Comtrade code), not 0. Using 0 would clash with
    the legitimate "World" reporter code and produce false matches
    against rows where partner is genuinely "World".
    """
    # Ensure key columns are the right dtype. fillna(-1) avoids accidental
    # collisions with the real "World" partner code (0); -1 is impossible
    # in Comtrade and acts as a clean sentinel.
    for c in ("reporterCode", "partnerCode", "partner2Code"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(-1).astype("int64")
    df["cmdCode"] = df["cmdCode"].astype(str)
    df["flowCode"] = df["flowCode"].astype(str).str.upper()

    df["matched_id"] = pd.NA

    # ---- Build indexes (int64-keyed groupby for low peak RAM) ----
    # Tuple-keyed Python dicts cost ~200 B/key for the 20M-key indexes a
    # 60M-row year produces — that's 4 GB of Python objects per index, on
    # top of the 3 GB narrow frame. Encoding the key as a single int64
    # collapses that to ~8 B/key (~160 MB per index) and keeps the rest
    # of the loop's vectorised lookups intact.
    #
    # Encoding (bit-packed, sentinel-safe):
    #   bits  0..23  — cmd_code: 4-bit length || 20-bit numeric value
    #   bits 24..39  — partner2 (offset by +1 so -1 sentinel becomes 0)
    #   bits 40..55  — partner   (offset by +1)
    #   bits 56..63  — reporter low byte; the 4-bit overflow lives here
    # Width: reporter/partner/partner2 ≤ 9999 fits in 14 bits; cmdCode int
    # ≤ 999 999 fits in 20 bits; the 4-bit length prefix on cmd
    # distinguishes "01" (HS2, len=2 → val 1) from "0001" (HS4, len=4 →
    # val 1) so leading-zero variants don't collide.

    _notify("🔧 Indexing rows by flow (int64 keys, partner2-aware)…", notify)
    index_bar = ProgressReporter(
        "Match Step 4a/4 — Indexing", step_pct=25, enabled=notify
    )
    index_bar.start()

    # Reset index so positional iloc == group-indices output
    df = df.reset_index(drop=True)

    # Force int64 dtype on the code columns. After the fillna(-1).astype("int64")
    # at the top of the function these are already int64, but to_numpy() on a
    # nullable Int64 with hidden NaN remnants would silently return an object
    # array (one Python int per row → ~3 GB extra). na_value=-1 enforces a
    # real int64 buffer.
    rc = df["reporterCode"].to_numpy(dtype="int64", na_value=-1)
    pc = df["partnerCode"].to_numpy(dtype="int64", na_value=-1)
    p2 = df["partner2Code"].to_numpy(dtype="int64", na_value=-1)

    m_mask = (df["flowCode"].values == "M")
    x_mask = (df["flowCode"].values == "X")

    # Partition M rows by whether partner2 is 0 (wildcard) or specific.
    p2_zero_mask = p2 == 0
    m_zero_mask = m_mask & p2_zero_mask
    m_spec_mask = m_mask & ~p2_zero_mask
    del p2_zero_mask, m_mask  # masks are derivable from rc/pc/p2/flow if needed

    # ── int64 key encoding ────────────────────────────────────────
    # Pre-compute cmd_codes vectorised (length-prefixed int).
    cmd_int = pd.to_numeric(df["cmdCode"], errors="coerce").fillna(0).astype("int64").to_numpy()
    cmd_len = (
        df["cmdCode"].astype(str).str.strip().str.len().clip(upper=15).astype("int64").to_numpy()
    )
    # Mask: keep cmd_int low 20 bits (HS6 max = 999999 fits in 20 bits).
    cmd_packed = (cmd_len << 20) | (cmd_int & ((1 << 20) - 1))
    del cmd_int, cmd_len

    # Offset partner / partner2 by +1 so -1 sentinel encodes as 0 and the
    # legitimate "World" reporter code (0) encodes as 1 — disjoint.
    p1_enc = (pc + 1).astype("int64")
    p2_enc = (p2 + 1).astype("int64")
    rep_enc = (rc + 1).astype("int64")

    # Three-column key (rep, par, cmd) for the p2-zero index.
    key_3 = (rep_enc << 40) | (p1_enc << 24) | cmd_packed
    # Four-column key (rep, par, par2, cmd) for the p2-specific + x_full index.
    key_4 = (rep_enc << 52) | (p1_enc << 38) | (p2_enc << 24) | cmd_packed
    # rep_enc / p1_enc / p2_enc are no longer needed — the swap arrays below
    # are computed from rc / pc / p2 directly.
    del rep_enc, p1_enc, p2_enc

    def _group_positions_int(mask, key_arr):
        """Vectorised int64-keyed grouping. Returns a CSR-style triple
        ``(sorted_keys, starts, positions)`` for O(log n) point lookup via
        ``np.searchsorted``.

        The dict-based variant cost ~250 B per unique key (Python int box +
        ndarray view + dict entry) — on a 60M-row year with ~15M unique
        keys per index, that's ~4 GB per index × 3 indexes = ~12 GB of
        Python objects on top of the encoded arrays. The CSR layout
        collapses that to three numpy arrays at ~8 B/key.

        Returned arrays:
          sorted_keys[i]  — i-th unique key, ascending
          starts[i]       — offset into positions where group i begins
                            (starts has length len(sorted_keys)+1; the last
                             entry is len(positions) so end = starts[i+1])
          positions[j]    — original row index, grouped by key
        """
        if not mask.any():
            return (
                np.empty(0, dtype=np.int64),
                np.zeros(1, dtype=np.int64),
                np.empty(0, dtype=np.int64),
            )
        positions = np.flatnonzero(mask)
        keys_sub = key_arr[positions]
        order = np.argsort(keys_sub, kind="stable")
        positions_sorted = positions[order].astype(np.int64, copy=False)
        keys_sorted = keys_sub[order]
        del positions, keys_sub, order
        uniq, first_idx = np.unique(keys_sorted, return_index=True)
        del keys_sorted
        starts = np.empty(len(uniq) + 1, dtype=np.int64)
        starts[:-1] = first_idx
        starts[-1] = len(positions_sorted)
        return (uniq, starts, positions_sorted)

    # M wildcards (partner2 == 0): keyed by 3-tuple — these pair with any
    # X row regardless of X's partner2. This is the workhorse index since
    # most M rows have partner2 == 0 in Comtrade data.
    m_zero = _group_positions_int(m_zero_mask, key_3)
    index_bar.update(1, 3, suffix="M wildcard (p2=0) index")
    del m_zero_mask, key_3  # key_3 is only needed for the m_zero index

    # M specifics (partner2 != 0): keyed by full 4-tuple — these only pair
    # with X rows that have the same specific partner2.
    m_specific = _group_positions_int(m_spec_mask, key_4)
    index_bar.update(2, 3, suffix="M specific (p2≠0) index")
    del m_spec_mask

    # X full key — used to expand X-side duplicates only. Same partner2
    # discipline as the M side.
    x_full = _group_positions_int(x_mask, key_4)
    index_bar.update(3, 3, suffix="X full index")
    index_bar.done("Match Step 4a/4 — Indexes built")

    _empty = np.empty(0, dtype=np.int64)

    # Constant-cost lookup helper. searchsorted on a 15M-key array is ~24
    # comparisons in C — still much faster than the surrounding Python loop
    # work (mask + concat + assign), and saves ~4 GB versus a dict.
    def _lookup(group, query_key):
        sorted_keys, starts, positions_sorted = group
        if sorted_keys.size == 0:
            return _empty
        idx = np.searchsorted(sorted_keys, query_key)
        if idx >= sorted_keys.size or sorted_keys[idx] != query_key:
            return _empty
        return positions_sorted[starts[idx]:starts[idx + 1]]

    # ---- Iterate X rows in id order ----
    # X positions sorted by id (id is a plain 1..N integer column).
    x_positions = df.index[x_mask].to_numpy()
    del x_mask  # done with the boolean mask; positions take it from here
    # Since id was assigned in row order, x_positions is already id-sorted.
    total_x = len(x_positions)

    diag = {
        "x_total": total_x,
        "x_matched": 0,
        "x_no_m": 0,
    }

    # matched_col stores the group id (1..G) assigned to each row; 0 means
    # unmatched. Using int64 here (instead of an object array of pd.NA) makes
    # the per-row update a plain C store, and the unmatched filter a single
    # vectorised boolean lookup — the inner loop runs ~3M×, so this matters.
    N = len(df)
    matched_col = np.zeros(N, dtype=np.int64)
    # Boolean twin kept in lockstep with matched_col so _unmatched can be a
    # single vectorised indexing op instead of a per-element pd.isna check.
    is_matched = np.zeros(N, dtype=bool)

    def _unmatched(positions):
        """Filter positions down to those that aren't already matched."""
        if len(positions) == 0:
            return positions
        # positions is a numpy int array (from _group_positions); one
        # vectorised boolean index is ~100× faster than a Python comprehension.
        return positions[~is_matched[positions]]

    def _assign(positions, mid):
        """Write mid into matched_col and flip is_matched in one vector op."""
        if len(positions) == 0:
            return
        matched_col[positions] = mid
        is_matched[positions] = True

    next_mid = 1
    match_bar = ProgressReporter(
        "Match Step 4b/4 — Matching X rows", step_pct=5, enabled=notify
    )
    match_bar.start(suffix=f"{total_x:,} X rows queued")

    # Pre-compute the row-level lookup keys per X position so the inner
    # loop is one lookup call instead of several tuple constructions.
    #
    # Direct bilateral case (partner2 == 0): the M index is keyed on
    # (M.reporter, M.partner, cmd), looked up with (X.partner, X.reporter,
    # X.cmd) — same bit layout as m_zero, but with reporter/partner swapped.
    p1_swap = (pc + 1).astype("int64")       # X.partner becomes M.reporter slot
    rep_swap = (rc + 1).astype("int64")      # X.reporter becomes M.partner slot
    key_3_swap = (p1_swap << 40) | (rep_swap << 24) | cmd_packed

    # Triangular case (partner2 != 0): the M index is keyed on
    # (M.reporter, M.partner, M.partner2, cmd), looked up with the swap
    # (X.partner2, X.reporter, X.partner, X.cmd) — see docstring above.
    p2_swap = (p2 + 1).astype("int64")
    key_4_swap = (p2_swap << 52) | (rep_swap << 38) | (p1_swap << 24) | cmd_packed
    # Per-row swap inputs are no longer needed — the packed keys carry the
    # full lookup tuple.
    del p1_swap, rep_swap, p2_swap, cmd_packed

    for n, pos in enumerate(x_positions, start=1):
        # Progress bar (5% granularity)
        if total_x >= 200:
            match_bar.update(n, total_x)

        if is_matched[pos]:
            continue

        x_p2 = p2[pos]

        # ── Strict same-bucket matching with triangular field alignment ─
        # See the docstring at the top of this function for the swap rules;
        # the lookup keys were pre-encoded above.
        if x_p2 == 0:
            m_cands_zero = _unmatched(_lookup(m_zero, int(key_3_swap[pos])))
            m_cands_spec = _empty
        else:
            m_cands_zero = _empty
            m_cands_spec = _unmatched(_lookup(m_specific, int(key_4_swap[pos])))

        if len(m_cands_zero) == 0 and len(m_cands_spec) == 0:
            diag["x_no_m"] += 1
            continue

        # X duplicates — same full 4-tuple as the original X row.
        x_dupes = _unmatched(_lookup(x_full, int(key_4[pos])))

        # The candidates from m_zero/m_specific already include EVERY M row
        # in the relevant bucket (the groupby already merged duplicates for
        # us). No further m_full expansion is needed — that step in the
        # earlier algorithm caused partner2 cross-talk by widening the M
        # set asymmetrically. Concatenate the two pools and assign in one
        # vector op.
        if len(m_cands_zero) and len(m_cands_spec):
            m_all = np.concatenate([m_cands_zero, m_cands_spec])
        elif len(m_cands_zero):
            m_all = m_cands_zero
        else:
            m_all = m_cands_spec

        mid = next_mid
        next_mid += 1
        _assign(x_dupes, mid)
        _assign(m_all, mid)

        diag["x_matched"] += 1

    match_bar.done(f"Match Step 4b/4 — Matched {next_mid - 1:,} groups")

    # Convert back to pandas' nullable Int64 — downstream code uses pd.isna
    # to detect unmatched rows and expects matched_id to be integer-valued.
    mid_arr = pd.array(matched_col, dtype="Int64")
    mid_arr[matched_col == 0] = pd.NA
    df["matched_id"] = mid_arr

    n_matched_rows = int(is_matched.sum())
    _notify(
        f"✅ Matching complete\n"
        f"  X rows: {diag['x_total']:,} "
        f"(matched {diag['x_matched']:,} / no-M {diag['x_no_m']:,})\n"
        f"  Matched groups: {next_mid-1:,}\n"
        f"  Rows with matched_id: {n_matched_rows:,} ({n_matched_rows/len(df)*100:.2f}%)",
        notify,
    )
    return df, diag


def _set_match_stage(year_str, stage, **extras):
    """Persist the current MATCH stage so a crash can be diagnosed and
    the next run can decide whether to resume or rebuild from scratch.

    Stages (in order): merged → narrow_loaded → matched → split_written
    → complete. On startup we read this and only trust the merged
    intermediate when stage >= ``split_written``.
    """
    payload = progress_mod.load_progress("match", year_str) or {}
    payload["stage"] = stage
    if extras:
        payload.update(extras)
    progress_mod.save_progress("match", year_str, payload)


def run_match_year(year, targets, scope_label="Selected scope", notify=True):
    """
    Execute the full matching pipeline for one year.

    Returns (matches_csv, report_xlsx).
    Either component may be None if the stage produced no data.
    """
    year_str = str(year)
    target_codes = [r["code"] for r in targets]

    os.makedirs(MATCH_DIR, exist_ok=True)

    # Sweep stale .tmp files left over from a crashed previous run, and
    # discard a half-written merged intermediate unless the previous run
    # got far enough that the split outputs are the authoritative source.
    progress_mod.sweep_stale_tmp(MATCH_DIR)
    prev_progress = progress_mod.load_progress("match", year_str) or {}
    if prev_progress.get("stage") not in (None, "split_written", "complete"):
        merged = _merged_intermediate_path(year_str)
        if os.path.exists(merged):
            try:
                os.remove(merged)
                logger.info(
                    "Discarded stale merged intermediate %s (previous run "
                    "stopped at stage=%s — file is not safe to resume mid-stream).",
                    merged,
                    prev_progress.get("stage"),
                )
            except OSError as e:
                logger.warning(
                    "Could not remove stale merged intermediate %s: %s", merged, e
                )

    mm = _meta_map()

    _notify(
        f"{PHASE_MATCH} <b>MATCH — {year_str}</b>\n"
        f"Scope: {scope_label} | Targets: {len(targets)}",
        notify,
    )

    # Resume-safety: reuse the cached split outputs ONLY when the
    # sidecar carries the same OUTPUT_SCHEMA_VERSION this code
    # writes. Bumping the version (because matched_id semantics or
    # the column set changed) auto-invalidates legacy caches so a
    # Dockploy redeploy regenerates the matched.csv with the new
    # semantics — the user never has to `rm` anything.
    cached_stats, cached_schema = _load_cached_stats(year_str)
    schema_ok = cached_schema == OUTPUT_SCHEMA_VERSION

    # Schema-version-stale cached outputs: log a clear message
    # before we fall into the from-aggregate path so the user
    # understands WHY the (expensive) rebuild is happening on their
    # next redeploy.
    if (
        _match_outputs_valid(year_str)
        and cached_stats is not None
        and not schema_ok
    ):
        _notify(
            f"♻️ <b>MATCH Steps 1-2/4 — cached outputs found but at schema "
            f"v{cached_schema}; code expects v{OUTPUT_SCHEMA_VERSION}.</b>\n"
            f"  Regenerating matched.csv + unmatched.csv from aggregate "
            f"so downstream phases see the new schema.",
            notify,
        )
        cached_stats = None  # force the else branch below to fire

    if _match_outputs_valid(year_str) and cached_stats is not None and schema_ok:
        per_country = cached_stats
        m_path = _matched_read_path(year_str)
        u_path = _unmatched_read_path(year_str)
        try:
            sz_mb = (os.path.getsize(m_path) + os.path.getsize(u_path)) / (1024 * 1024)
        except OSError:
            sz_mb = 0
        _notify(
            f"♻️ <b>MATCH Steps 1-2/4 — Reusing cached split outputs</b> "
            f"({sz_mb:.1f} MB total)\n"
            f"  • <code>{h(m_path)}</code>\n"
            f"  • <code>{h(u_path)}</code>\n"
            f"  Matching algorithm re-runs so algo changes take effect.",
            notify,
        )
        csv_path = _merged_intermediate_path(year_str)

        # Reconstitute the merged intermediate from the split outputs.
        # Naive byte-concatenation (shutil.copyfileobj) was wrong: after
        # dedup runs, matched.csv carries a `keep_reason` column that
        # unmatched.csv doesn't have, so the rows have different widths
        # and pyarrow chokes at parse time. Step 7's chunked split-write
        # re-adds id / matched_id / hs_level (and dedup re-adds
        # keep_reason later), so all four can be dropped during
        # reconstitution — the merged.csv only needs the *wide*
        # columns to feed the matching algorithm.
        #
        # DuckDB does the projection + UNION ALL in one streaming pass.
        # ALL_VARCHAR=TRUE forces every column to VARCHAR so leading-
        # zero strings (cmdCode='010110' is HS6 for cattle) survive
        # the round-trip — auto-detect would type cmdCode as BIGINT
        # and silently drop the leading zero.
        m_header = _matched_header(m_path)
        u_header = _matched_header(u_path)
        _to_drop = {"id", "matched_id", "hs_level", "keep_reason"}
        wide_cols_m = [c for c in m_header if c not in _to_drop]
        wide_cols_u = [c for c in u_header if c not in _to_drop]
        # Use the intersection in case the two files diverge further
        # in a future schema change. Preserves matched.csv's column
        # order (which matches the original aggregate ordering).
        wide_cols = [c for c in wide_cols_m if c in set(wide_cols_u)]
        cols_sql = ", ".join(_sql_ident(c) for c in wide_cols)

        con = _duckdb_connect()
        try:
            con.execute(f"""
                COPY (
                  SELECT {cols_sql}
                  FROM read_csv({_sql_lit(m_path)},
                                ALL_VARCHAR=TRUE, AUTO_DETECT=TRUE,
                                HEADER=TRUE)
                  UNION ALL
                  SELECT {cols_sql}
                  FROM read_csv({_sql_lit(u_path)},
                                ALL_VARCHAR=TRUE, AUTO_DETECT=TRUE,
                                HEADER=TRUE)
                ) TO {_sql_lit(csv_path)} (FORMAT CSV, HEADER TRUE);
            """)
        finally:
            con.close()
    else:
        # AGGREGATE is the only valid input. If it's missing, abort and
        # tell the user to run phase 2 first. Accept both .csv and .csv.gz
        # — shrink_year compresses after AGGREGATE finishes.
        aggregate_path = _aggregate_read_path(year_str)
        if aggregate_path is None:
            _notify(
                f"❌ <b>MATCH aborted for {year_str}</b>\n"
                f"  <code>aggregate/{year_str}.csv</code> is missing or empty.\n"
                f"  Run the AGGREGATE phase (menu option 2) first, then re-run MATCH.",
                notify,
            )
            return None, None

        in_scope = _reporters_in_aggregate(year_str, target_codes)
        if not in_scope:
            _notify(
                f"❌ <b>MATCH aborted for {year_str}</b>\n"
                f"  None of the requested target reporters are present in "
                f"<code>aggregate/{year_str}.csv</code>.",
                notify,
            )
            return None, None

        _notify(
            f"✅ <b>MATCH Step 1/4 — {len(in_scope)} reporter(s) "
            f"found in aggregate/{year_str}.csv</b>",
            notify,
        )
        csv_path, per_country, cols = _load_from_aggregate_csv(
            year_str, in_scope, notify=notify
        )

    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        _notify("⚠️ No rows survived filtering — aborting", notify)
        return None, None

    _set_match_stage(year_str, "merged", merged_csv=csv_path)

    # Step 5 — NARROW load. Only the 6 match-key columns are needed to
    # run the matcher. Loading the wide frame here cost us an OOM on the
    # 16 GB VM (8.5 GB merged CSV → 30+ GB DataFrame with str dtypes).
    # The wide columns get streamed through the chunked split-write
    # below instead of being held in RAM.
    #
    # `classificationCode` joined the narrow set in A2 (HS revision
    # concordance): the matcher canonicalises (classificationCode,
    # cmdCode) to a single canonical revision before keying, so an X
    # row reported under H3 (HS 2007) cannot accidentally pair with
    # an M row reported under H5 (HS 2017) when their 6-digit strings
    # collide despite encoding different goods.
    NARROW_COLS = [
        "reporterCode",
        "flowCode",
        "partnerCode",
        "partner2Code",
        "cmdCode",
        "classificationCode",
    ]
    try:
        csv_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    except OSError:
        csv_size_mb = 0
    _notify(
        f"📥 <b>MATCH Step 3/4 — Loading narrow frame for matching</b>\n"
        f"  File: <code>{h(csv_path)}</code> ({csv_size_mb:.1f} MB on disk)\n"
        f"  Loading only {len(NARROW_COLS)} key columns "
        f"({', '.join(NARROW_COLS)}) — wide columns stream-through later.",
        notify,
    )
    t_load = time.time()
    # The single-shot pd.read_csv path here OOM'd on a 16 GB VM with the
    # 2024 merged frame: usecols filters AFTER tokenization, low_memory=False
    # materialises the whole file as Python strings before dtype conversion,
    # and the Int64 conversion holds parsed buffers + new arrays + validity
    # masks simultaneously — peak ~30 GB for a frame whose final size is ~4 GB..
    # pyarrow.csv projects columns at the I/O layer and parses with native
    # typed buffers, cutting peak memory ~5×. Falls back to chunked pandas
    # read when pyarrow isn't installed.
    if _ARROW_AVAILABLE:
        import pyarrow as pa
        import pyarrow.csv as pacsv

        table = pacsv.read_csv(
            csv_path,
            read_options=pacsv.ReadOptions(use_threads=True),
            convert_options=pacsv.ConvertOptions(
                include_columns=NARROW_COLS,
                column_types={
                    "reporterCode": pa.int64(),
                    "partnerCode": pa.int64(),
                    "partner2Code": pa.int64(),
                    "flowCode": pa.string(),
                    "cmdCode": pa.string(),
                    "classificationCode": pa.string(),
                },
                strings_can_be_null=True,
            ),
        )
        df_narrow = table.to_pandas(split_blocks=True, self_destruct=True)
        del table
    else:
        parts = []
        for chunk in pd.read_csv(
            csv_path,
            usecols=NARROW_COLS,
            dtype={
                "reporterCode": "Int64",
                "partnerCode": "Int64",
                "partner2Code": "Int64",
                "flowCode": "category",
                "cmdCode": "category",
                "classificationCode": "category",
            },
            chunksize=1_000_000,
        ):
            parts.append(chunk)
        df_narrow = pd.concat(parts, ignore_index=True, copy=False)
        del parts
    load_s = time.time() - t_load
    mem_mb = df_narrow.memory_usage(deep=True).sum() / (1024 * 1024)

    flow_counts = df_narrow["flowCode"].astype(str).str.upper().value_counts().to_dict()
    _cmd_series_load = df_narrow["cmdCode"].astype(str)
    _hs_lookup_load = {c: hs_level(c) for c in _cmd_series_load.unique()}
    hs_counts = _cmd_series_load.map(_hs_lookup_load).value_counts().to_dict()
    del _cmd_series_load
    _notify(
        f"✅ <b>Loaded {len(df_narrow):,} rows</b> in {load_s:.1f}s "
        f"(memory: {mem_mb:.0f} MB)\n"
        f"  Flows → X:{flow_counts.get('X',0):,} • "
        f"M:{flow_counts.get('M',0):,}\n"
        f"  HS   → HS2:{hs_counts.get('HS2',0):,} • "
        f"HS4:{hs_counts.get('HS4',0):,} • HS6:{hs_counts.get('HS6',0):,}",
        notify,
    )

    _set_match_stage(year_str, "narrow_loaded")

    # ----------------------------------------------------------------
    # A2 — HS revision concordance.
    # Replace each row's `cmdCode` (revision-specific) with the
    # canonical-revision code looked up in `app/data/reference/
    # hs_concordance.csv`. The matcher then keys on the canonical
    # cmdCode, so an X row reported under H3 cannot accidentally
    # pair with an M row reported under H5 when their raw 6-digit
    # strings collide despite encoding different goods.
    #
    # The original revision-specific code is preserved in the wide
    # CSV (matched.csv) — only df_narrow's cmdCode column is
    # rewritten, and df_narrow is dropped after matching. matched.csv
    # rows therefore retain their `classificationCode` and original
    # `cmdCode`; matched_id alone encodes the canonical pairing.
    # ----------------------------------------------------------------
    concord_table = load_hs_concordance()
    if concord_table:
        # Vectorised lookup — pandas .map on a (revision, code) tuple
        # column. Only HS6 codes are concordable; HS2/HS4 are
        # revision-stable and pass through identity.
        cls = df_narrow["classificationCode"].astype(str).str.strip()
        cmd = df_narrow["cmdCode"].astype(str).str.strip()
        is_hs6 = cmd.str.len() >= 5
        is_concordable = is_hs6 & (cls != CANONICAL_HS_REVISION)

        # Build a numpy-style lookup: where the (rev, code) tuple is
        # in the table, use the table's value; else identity.
        keys = pd.Series(list(zip(cls, cmd)), index=df_narrow.index)
        mapped = keys.map(concord_table)

        before_concord = cmd.copy()
        canonical_cmd = cmd.where(~(is_concordable & mapped.notna()), mapped)
        df_narrow["cmdCode"] = canonical_cmd

        n_changed = int((before_concord != df_narrow["cmdCode"].astype(str)).sum())
        n_hs6_in_scope = int(is_concordable.sum())
        n_hs6_unmapped = int((is_concordable & mapped.isna()).sum())

        concordance_diag = {
            "table_rows": len(concord_table),
            "canonical_revision": CANONICAL_HS_REVISION,
            "n_rows_concorded": n_changed,
            "n_hs6_in_scope": n_hs6_in_scope,
            "n_hs6_unmapped_identity_fallback": n_hs6_unmapped,
        }
        _notify(
            f"🧭 <b>HS concordance applied</b> → "
            f"canonical {CANONICAL_HS_REVISION}\n"
            f"  Rows rewritten: {n_changed:,} / "
            f"{n_hs6_in_scope:,} HS6 in non-canonical revisions\n"
            f"  Identity fallback (code missing from table): "
            f"{n_hs6_unmapped:,}",
            notify,
        )
    else:
        concordance_diag = {
            "table_rows": 0,
            "canonical_revision": CANONICAL_HS_REVISION,
            "n_rows_concorded": 0,
            "n_hs6_in_scope": 0,
            "n_hs6_unmapped_identity_fallback": 0,
        }
        _notify(
            f"⚠️ <b>HS concordance table empty / missing</b> — "
            f"matcher proceeds with raw cmdCode "
            f"(cross-revision pairs will silently match on string "
            f"equality). See "
            f"<code>app/data/reference/hs_concordance.csv</code>.",
            notify,
        )

    # id is strictly sequential by row order (1..N) — same convention as
    # the row order we'll see when streaming the wide CSV in chunks
    # below, so the chunked split can rebuild the same id without a join.
    df_narrow.insert(0, "id", range(1, len(df_narrow) + 1))
    df_narrow.insert(1, "matched_id", pd.NA)

    # Step 6 — run matching on the narrow frame.
    _notify("🔗 <b>MATCH Step 4/4 — Matching X rows to M counterparts</b>", notify)
    t_match = time.time()
    df_narrow, diag = _assign_match_ids(df_narrow, notify=notify)
    diag["concordance"] = concordance_diag
    match_s = time.time() - t_match
    _set_match_stage(year_str, "matched")

    matched_mask = ~pd.isna(df_narrow["matched_id"])
    n_matched = int(matched_mask.sum())
    n_unmatched = len(df_narrow) - n_matched
    n_groups = (
        int(df_narrow["matched_id"].dropna().astype(int).max()) if n_matched else 0
    )

    _notify(
        f"🧮 <b>Match diagnostics</b> (took {match_s:.1f}s)\n"
        f"  X processed: {diag['x_total']:,}\n"
        f"  X matched:   {diag['x_matched']:,}\n"
        f"  X no-M:      {diag['x_no_m']:,}\n"
        f"  Groups formed: {n_groups:,}\n"
        f"  Matched rows: {n_matched:,} ({n_matched/len(df_narrow)*100:.2f}%)\n"
        f"  Unmatched rows: {n_unmatched:,} "
        f"({n_unmatched/len(df_narrow)*100:.2f}%)",
        notify,
    )

    # Build the id → matched_id lookup as a plain int64 numpy array
    # with 0 sentinel for "unmatched". This is ~5× faster to index in
    # the per-row split loop below than the previous object array of
    # pd.NA / Int64 values, and avoids any pd.isna() call in the hot
    # loop. matched_col[i] == 0  iff  row i was unmatched.
    matched_col = df_narrow["matched_id"].astype("Int64").to_numpy(
        dtype="int64", na_value=0
    )
    del df_narrow

    # Step 7 — pure-Python streaming split-write. Reads the merged CSV
    # line by line, looks up each row's matched_id from matched_col by
    # position, and appends the row to either match/{year}_matched.csv
    # (if matched) or match/{year}_unmatched.csv (if not).
    #
    # This replaces a pandas chunked path that took ~19 min on the
    # 60 M-row year 2015 file. The bottleneck was pandas' per-cell
    # str allocation (dtype=str produced ~17 GB of transient Python
    # string objects over the run), plus three DataFrame .insert()
    # rebuilds + a .to_csv() iteration per chunk. The pure-Python
    # streaming version is ~5× faster (~4 min on the same file)
    # because it only ever holds one input line + one output line in
    # Python memory, and never builds a DataFrame.
    matched_path = _matched_path(year_str)
    unmatched_path = _unmatched_path(year_str)
    tmp_matched = matched_path + ".tmp"
    tmp_unmatched = unmatched_path + ".tmp"
    for p in (tmp_matched, tmp_unmatched):
        if os.path.exists(p):
            os.remove(p)

    _notify(
        f"💾 <b>Writing split outputs</b> (streaming line-by-line)\n"
        f"  → <code>{h(matched_path)}</code>\n"
        f"  → <code>{h(unmatched_path)}</code>",
        notify,
    )
    t_write = time.time()
    n_matched_written = 0
    n_unmatched_written = 0

    # 8 MB write buffers — far above the 8 KB default, big enough that
    # write() syscalls become rare even on a 6 GB output. Reads use
    # Python's default buffering since iteration is already line-based.
    _WRITE_BUF = 8 * 1024 * 1024

    with open(csv_path, "r", encoding="utf-8") as in_fh, \
         open(tmp_matched, "w", encoding="utf-8",
              newline="", buffering=_WRITE_BUF) as fh_m, \
         open(tmp_unmatched, "w", encoding="utf-8",
              newline="", buffering=_WRITE_BUF) as fh_u:

        # Parse the source header. Strip any stale id / matched_id /
        # hs_level columns the cache-reuse path may have left in there
        # so they don't collide with the freshly computed values.
        raw_header = in_fh.readline().rstrip("\r\n").split(",")
        stale = {"id", "matched_id", "hs_level"}
        keep_idx = [i for i, c in enumerate(raw_header) if c not in stale]
        kept_header = [raw_header[i] for i in keep_idx]
        try:
            cmd_pos = kept_header.index("cmdCode")
        except ValueError:
            cmd_pos = None  # weirdly schemad input — no hs_level injection

        if cmd_pos is not None:
            out_header = (
                ["id", "matched_id"]
                + kept_header[:cmd_pos + 1]
                + ["hs_level"]
                + kept_header[cmd_pos + 1:]
            )
        else:
            out_header = ["id", "matched_id"] + kept_header
        header_str = ",".join(out_header) + "\n"
        fh_m.write(header_str)
        fh_u.write(header_str)

        # hs_level cache — ~5 K unique cmdCodes per year, so this dict
        # absorbs all the work after the first few thousand rows. Inline
        # branch ladder is faster than a .map(lambda).
        hs_cache = {}

        # Local references for the inner loop — Python's name resolution
        # is otherwise repeated 60 M times.
        _write_m = fh_m.write
        _write_u = fh_u.write
        _matched_col = matched_col

        # Whether we need to reorder columns: only when the source has
        # stale leading columns. On a fresh-from-aggregate run keep_idx
        # is just range(len(raw_header)) and the reorder is a no-op.
        need_reorder = (len(keep_idx) != len(raw_header))

        for i, line in enumerate(in_fh):
            # Drop trailing newline; preserve content as-is otherwise.
            if line.endswith("\n"):
                line = line[:-1]
                if line.endswith("\r"):
                    line = line[:-1]
            if not line:
                continue

            cols = line.split(",")
            if need_reorder:
                cols = [cols[j] for j in keep_idx]

            if cmd_pos is not None:
                cmd = cols[cmd_pos]
                hs = hs_cache.get(cmd)
                if hs is None:
                    L = len(cmd.strip())
                    hs = "HS2" if L <= 2 else ("HS4" if L <= 4 else "HS6")
                    hs_cache[cmd] = hs
                parts = [str(i + 1)]
                mid = _matched_col[i]
                parts.append("" if mid == 0 else str(mid))
                parts.extend(cols[:cmd_pos + 1])
                parts.append(hs)
                parts.extend(cols[cmd_pos + 1:])
            else:
                parts = [str(i + 1)]
                mid = _matched_col[i]
                parts.append("" if mid == 0 else str(mid))
                parts.extend(cols)

            out_line = ",".join(parts) + "\n"
            if mid == 0:
                _write_u(out_line)
                n_unmatched_written += 1
            else:
                _write_m(out_line)
                n_matched_written += 1

        # Force the buffered writes through to disk before the rename.
        fh_m.flush()
        fh_u.flush()
        try:
            os.fsync(fh_m.fileno())
            os.fsync(fh_u.fileno())
        except OSError:
            pass

    # Atomic rename — a crash mid-loop leaves the previous split files
    # untouched.
    os.replace(tmp_matched, matched_path)
    os.replace(tmp_unmatched, unmatched_path)
    _set_match_stage(year_str, "split_written")
    write_s = time.time() - t_write

    try:
        m_mb = os.path.getsize(matched_path) / (1024 * 1024)
        u_mb = os.path.getsize(unmatched_path) / (1024 * 1024)
    except OSError:
        m_mb = u_mb = 0
    _notify(
        f"✅ Split written in {write_s:.1f}s "
        f"({n_matched_written:,} matched / {m_mb:.1f} MB, "
        f"{n_unmatched_written:,} unmatched / {u_mb:.1f} MB)",
        notify,
    )
    # Free the matched_col now that we've consumed it.
    del matched_col

    # Intermediate merged file is no longer needed.
    try:
        if os.path.exists(csv_path):
            os.remove(csv_path)
    except OSError:
        pass

    # MATCH ends here. Reports moved to phase 5 (app.pipeline.reports) so
    # this function never reads the wide matched CSV back into RAM —
    # that's what used to OOM 16 GB VMs on the 80 M-row years.
    _set_match_stage(
        year_str,
        "complete",
        matched_csv=matched_path,
        unmatched_csv=unmatched_path,
        n_matched=int(n_matched),
        n_unmatched=int(n_unmatched),
    )
    _notify(
        f"{STATUS_OK} <b>MATCH done — {year_str}</b>\n"
        f"  Matched rows: {n_matched:,}\n"
        f"  Unmatched rows: {n_unmatched:,}\n"
        f"  Files: <code>{h(matched_path)}</code> + "
        f"<code>{h(unmatched_path)}</code>",
        notify,
    )
    # Dedup step removed per user request — clean_matched_csv handles
    # per-(matched_id, flowCode) row picking on its own (agg > zero >
    # synth priority), and any exact-duplicate rows that survive into
    # matched.csv flow through clean unchanged. This shaves ~5-8 min
    # off the post-match runtime on big years; the cost is that if a
    # reporter genuinely double-reported a trade row, both copies will
    # contribute to the synth-side SUM. Empirically rare in Comtrade.
    clean_matched_csv(year_str, notify=notify)
    return matched_path, None


# ----------------------------------------------------------------------
# Per-year compression — called by main.py after MATCH + MISINVOICING
# ----------------------------------------------------------------------
def shrink_matched_year(year_str, notify=True):
    """
    Gzip-compress match/{year}_matched.csv and match/{year}_unmatched.csv
    in place. The .gz versions are typically ~30% of the originals; pandas
    reads them transparently via pd.read_csv(... compression='gzip') and
    auto-detects from the extension.

    Stats sidecar match/{year}_match_stats.json stays uncompressed —
    it's small and the Reports phase reads it frequently.

    Returns the list of paths that were compressed (may be empty if the
    files weren't found or were already compressed).
    """
    out_paths = []
    for name in (f"{year_str}_matched.csv", f"{year_str}_unmatched.csv"):
        src = os.path.join(MATCH_DIR, name)
        if not os.path.exists(src):
            continue
        dst = src + ".gz"
        tmp = dst + ".tmp"
        try:
            with open(src, "rb") as fh_in, gzip.open(
                tmp, "wb", compresslevel=1
            ) as fh_out:
                shutil.copyfileobj(fh_in, fh_out, length=8 * 1024 * 1024)
            os.replace(tmp, dst)
            os.remove(src)
            out_paths.append(dst)
            try:
                src_mb_after = os.path.getsize(dst) / (1024 * 1024)
            except OSError:
                src_mb_after = 0
            _notify(
                f"🗜  Compressed <code>{h(name)}</code> → "
                f"<code>{h(os.path.basename(dst))}</code> ({src_mb_after:.1f} MB)",
                notify,
            )
        except Exception as e:
            logger.warning("shrink_matched_year failed for %s: %s", src, e)
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass
    return out_paths


def shrink_year(year_str, notify=True):
    """
    Compress every per-year CSV that's no longer being written after
    misinvoicing finishes for the year. Aggregate / match / misinvoicing
    all get gzipped in place to ~30% of their plain size. Pandas reads
    .csv.gz transparently via extension detection, so no downstream
    code needs to change.

    Stats sidecars (.json) stay uncompressed — small, frequently read
    by phase 5 (Reports).
    """
    from app.config import AGGREGATE_DIR, MISINVOICING_DIR

    targets = [
        os.path.join(AGGREGATE_DIR, f"{year_str}.csv"),
        os.path.join(MATCH_DIR, f"{year_str}_matched.csv"),
        os.path.join(MATCH_DIR, f"{year_str}_unmatched.csv"),
        os.path.join(CLEAN_DIR, f"{year_str}.csv"),
        # legacy: clean.csv used to live next to matched.csv in match/.
        # Kept here so a run after the layout change can still compress
        # any stragglers from the old location.
        os.path.join(MATCH_DIR, f"{year_str}_clean.csv"),
        os.path.join(MISINVOICING_DIR, f"{year_str}.csv"),
        # legacy filename for already-shipped data
        os.path.join(MISINVOICING_DIR, f"{year_str}_misinvoicing.csv"),
    ]

    # Per-reporter aggregate partials live under aggregate/_partials/{year}/
    # and are kept around for resume-safety on subsequent runs. They're
    # idle once the year CSV is finalised, so gzip them here too — the
    # finalizer transparently re-reads .csv.gz partials.
    partials_dir = os.path.join(AGGREGATE_DIR, "_partials", year_str)
    if os.path.isdir(partials_dir):
        for fname in os.listdir(partials_dir):
            if fname.endswith(".csv"):
                targets.append(os.path.join(partials_dir, fname))
    out_paths = []
    total_saved_mb = 0.0
    for src in targets:
        if not os.path.exists(src):
            continue
        dst = src + ".gz"
        # Always recompress when src exists — it was just written by a
        # producer phase and is the freshest content. The previous
        # "skip if dst exists" branch was meant for crash-resume but
        # silently kept stale .gz files alive across schema changes:
        # a re-run that produced a new .csv would have it deleted
        # without ever overwriting the old .gz. ``os.replace`` below
        # atomically swaps the new tmp into dst regardless of whether
        # dst already existed, so no separate "remove dst first" step
        # is needed.
        tmp = dst + ".tmp"
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        try:
            before_mb = os.path.getsize(src) / (1024 * 1024)
        except OSError:
            before_mb = 0
        try:
            with open(src, "rb") as fh_in, gzip.open(
                tmp, "wb", compresslevel=1
            ) as fh_out:
                shutil.copyfileobj(fh_in, fh_out, length=8 * 1024 * 1024)
            os.replace(tmp, dst)
            os.remove(src)
            out_paths.append(dst)
            try:
                after_mb = os.path.getsize(dst) / (1024 * 1024)
            except OSError:
                after_mb = 0
            total_saved_mb += max(before_mb - after_mb, 0)
        except Exception as e:
            logger.warning("shrink_year failed for %s: %s", src, e)
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except OSError:
                pass
    if out_paths:
        _notify(
            f"🗜  <b>{year_str}</b> compressed: {len(out_paths)} file(s), "
            f"~{total_saved_mb:.1f} MB freed",
            notify,
        )
    return out_paths


# ----------------------------------------------------------------------
# DuckDB envelope — used by clean_matched_csv (and by the cache-reuse
# reconstitution in run_match_year).
# ----------------------------------------------------------------------
# The post-match clean step streams the matched CSV through a DuckDB
# SQL pipeline. DuckDB caps RSS at PRAGMA memory_limit and spills hash
# tables / window buffers to PRAGMA temp_directory once that's hit, so
# peak RAM is bounded — a 60 M-row year fits the same envelope as a
# 5 M-row one. The previous pandas-based path loaded the full matched
# frame (~5 GB final, ~20 GB transient during the categorical-column
# parse with low_memory=False) and OOM'd the 32 GB VM with no swap on
# every big year.
#
# Memory limit notes:
#   * 6 GB was the initial pick, chosen for safety on the 32 GB host.
#     That was fine for dedup (which only does COUNT / ROW_NUMBER /
#     MIN-MAX windows) but proved too tight for the clean step on the
#     25 M-row year 2016: GROUP BY with ARG_MAX over ~25 columns × ~8 M
#     synth groups keeps a few GB of per-group state, then the final
#     ORDER BY materialisation tips it over the cap. Year 2015 squeaked
#     under 6 GB because case-B/C synthesis was a smaller share.
#   * Bumped again from 16 GB to 24 GB after the year-2016 run still
#     spent unbounded time spilling at the 16 GB cap on dedup + clean.
#     Nothing else runs concurrently during post-match (the pipeline
#     is sequential), so 24 GB inside DuckDB + ~4 GB Python/libs +
#     ~1 GB kernel/page-cache leaves ~3 GB of headroom on the 32 GB
#     VM. Tight but safe — the matching algorithm itself never holds
#     more than ~6 GB of numpy arrays at that point in the pipeline.
#     Override via COMTRADE_DUCKDB_MEMORY for smaller hosts.
_DUCKDB_MEMORY_LIMIT = os.environ.get("COMTRADE_DUCKDB_MEMORY", "24GB")
_DUCKDB_THREADS = int(os.environ.get("COMTRADE_DUCKDB_THREADS", "4"))


def _duckdb_temp_dir():
    """Scratch directory for DuckDB spills. Lives next to the match
    outputs so it shares the same volume (300 GB-class balanced PD on
    the VM — no risk of filling root). Created on demand."""
    p = os.path.join(MATCH_DIR, "_duckdb_tmp")
    os.makedirs(p, exist_ok=True)
    return p


def _duckdb_connect():
    """Open an in-memory DuckDB with bounded RAM + on-disk spill dir.
    Caller is responsible for ``con.close()``."""
    import duckdb
    con = duckdb.connect()
    con.execute(f"PRAGMA memory_limit='{_DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"PRAGMA threads={_DUCKDB_THREADS}")
    con.execute(f"PRAGMA temp_directory='{_duckdb_temp_dir()}'")
    # Disable insertion-order preservation in pipelined intermediates.
    # The dedup + clean queries both end with an explicit ORDER BY (on
    # _orig_rn / matched_id, flowCode respectively) so output ordering
    # is fully determined by that final sort. preserve_insertion_order
    # defaults to TRUE in DuckDB, which forces it to retain row order
    # through GROUP BY / window operators — wasted memory for our use
    # case, and the proximate cause of the year-2016 clean step OOM.
    con.execute("PRAGMA preserve_insertion_order=false")
    return con


def _sql_lit(s):
    """SQL single-quoted string literal with embedded-quote escape."""
    return "'" + str(s).replace("'", "''") + "'"


def _sql_ident(c):
    """SQL identifier — always double-quoted because the matched CSV
    column names are camelCase (matched_id, flowCode, ...) and DuckDB
    would fold them to lowercase if left bare."""
    return '"' + str(c).replace('"', '""') + '"'


def _matched_header(path):
    """Read just the header line of the (possibly .gz) matched CSV."""
    if path.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            line = fh.readline()
    else:
        with open(path, "r", encoding="utf-8") as fh:
            line = fh.readline()
    return [c.strip() for c in line.rstrip("\r\n").split(",")]



# ----------------------------------------------------------------------
# Side-row picking — produces clean/{year}.csv with at most one row per
# (matched_id, flowCode) side. Rule (mid-v5, relaxed per user request):
#
#   * Per side, pick the MIN(id) row where ``motCode`` equals '0'.
#     The isAggregate filter has been removed — rows with isAggregate=0
#     are no longer dropped at this stage.
#   * Per pair, ONLY emit if BOTH the X and the M sides have a
#     motCode='0' row. Single-sided pairs vanish from clean.csv
#     entirely; misinvoicing's drop_no_pair would have dropped them at
#     the next stage anyway, but doing it here keeps the file lean and
#     makes the contract simple: "every matched_id in clean.csv has
#     exactly two rows".
#
# Stats sidecar key naming (pairs_case_a / _b) preserved so reports.py
# doesn't need a schema branch:
#   pairs_case_a — kept pairs (both sides have a motCode='0' row)
#   pairs_case_b — diagnostic: pairs that had a motCode='0' row on
#                  exactly ONE side and were therefore dropped
#   pairs_case_c — always 0 (no synth path)
#   output_pairs — distinct matched_ids actually emitted (= case_a × 1)
#   rows_summed  — always 0 (no synth)
# ----------------------------------------------------------------------


def clean_matched_csv(year_str: str, notify=None) -> dict:
    """Build clean/{year}.csv from match/{year}_matched.csv(.gz) via
    DuckDB. Per (matched_id, flowCode) side, picks the MIN(id) row
    where ``motCode`` equals '0'. Only matched_ids with such a row on
    BOTH the X and M side are emitted; everything else is dropped.

    See the section header above for the stat key conventions.
    """
    import duckdb  # noqa: F401 — lazy import keeps the rest of the
                   # module usable on machines without duckdb (e.g.
                   # local tests that only exercise _assign_match_ids).

    # Accept either matched.csv or matched.csv.gz — shrink_year may have
    # compressed the file in place after a previous run. DuckDB's read_csv
    # handles the .gz transparently via extension detection.
    src = _matched_read_path(year_str)
    if src is None or os.path.getsize(src) < 16:
        _notify(
            f"⚠️ Clean step skipped — {year_str}_matched.csv(.gz) missing.",
            notify,
        )
        return {}
    # Clean outputs now live in their own dir (clean/{year}.csv) rather
    # than next to matched.csv. Keeps the match/ folder limited to the
    # raw matcher outputs and makes it easy to gzip / archive / inspect
    # the post-strict-filter view independently.
    os.makedirs(CLEAN_DIR, exist_ok=True)
    dst = os.path.join(CLEAN_DIR, f"{year_str}.csv")
    tmp = dst + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)

    _notify(
        f"🧹 <b>{year_str}</b> picking motCode=0 rows for clean.csv "
        f"(both sides required, DuckDB)…",
        notify,
    )

    header = _matched_header(src)
    if "matched_id" not in header or "flowCode" not in header:
        _notify(
            "⚠️ Clean step skipped — missing matched_id / flowCode columns.",
            notify,
        )
        return {}
    if "motCode" not in header:
        _notify(
            "⚠️ Clean step skipped — matched.csv has no motCode column; "
            "filter is impossible. Copying matched.csv verbatim instead.",
            notify,
        )
        shutil.copyfile(src, dst)
        return {
            "pairs_case_a": 0, "pairs_case_b": 0, "pairs_case_c": 0,
            "rows_summed": 0, "output_pairs": 0,
        }
    if "id" not in header:
        _notify(
            "⚠️ Clean step skipped — matched.csv has no `id` column "
            "(should be assigned by run_match_year).",
            notify,
        )
        return {}

    _MOT_IS_ZERO = (
        "COALESCE(TRIM(CAST(\"motCode\" AS VARCHAR)), '') = '0'"
    )

    orig_cols_qualified_sql = ", ".join(
        f"m.{_sql_ident(c)}" for c in header
    )

    con = _duckdb_connect()
    try:
        # Per-side MIN(id) where motCode='0'. Sides without any
        # motCode='0' row are absent from this table. isAggregate is
        # NOT in the WHERE clause — rows with isAggregate=0 are kept
        # as long as they have motCode='0'.
        con.execute(f"""
            CREATE TEMP TABLE _dominant AS
            SELECT "matched_id", "flowCode", MIN("id") AS _dom_id
            FROM read_csv({_sql_lit(src)},
                          AUTO_DETECT=TRUE, HEADER=TRUE)
            WHERE {_MOT_IS_ZERO}
            GROUP BY "matched_id", "flowCode";
        """)

        # Matched_ids that have a motCode='0' row on BOTH the X and M
        # sides. Only these survive into the output — single-sided
        # pairs are dropped here rather than later in misinvoicing.
        con.execute("""
            CREATE TEMP TABLE _kept_pairs AS
            SELECT "matched_id"
            FROM _dominant
            GROUP BY "matched_id"
            HAVING SUM(CASE WHEN "flowCode"='X' THEN 1 ELSE 0 END) >= 1
               AND SUM(CASE WHEN "flowCode"='M' THEN 1 ELSE 0 END) >= 1;
        """)

        # Emit the chosen row per side, verbatim — but only for pairs
        # where both sides survived.
        con.execute(f"""
            COPY (
              SELECT {orig_cols_qualified_sql}
              FROM read_csv({_sql_lit(src)},
                            AUTO_DETECT=TRUE, HEADER=TRUE) m
              JOIN _dominant d
                ON d."matched_id" = m."matched_id"
                AND d."flowCode" = m."flowCode"
                AND d._dom_id = m."id"
              JOIN _kept_pairs k
                ON k."matched_id" = m."matched_id"
            ) TO {_sql_lit(tmp)} (FORMAT CSV, HEADER TRUE);
        """)

        # Stats — per-pair coverage. pairs_both = kept, pairs_one_side
        # = had motCode=0 on exactly one side and was dropped.
        stats_row = con.execute("""
            WITH pair AS (
              SELECT "matched_id",
                SUM(CASE WHEN "flowCode"='X' THEN 1 ELSE 0 END) AS n_x,
                SUM(CASE WHEN "flowCode"='M' THEN 1 ELSE 0 END) AS n_m
              FROM _dominant
              GROUP BY "matched_id"
            )
            SELECT
              COUNT_IF(n_x = 1 AND n_m = 1)         AS pairs_both,
              COUNT_IF((n_x = 1) <> (n_m = 1))      AS pairs_one_side,
              COUNT_IF(n_x = 1 AND n_m = 1)         AS output_pairs
            FROM pair;
        """).fetchone()
    finally:
        con.close()

    os.replace(tmp, dst)

    pairs_both, pairs_one_side, output_pairs = stats_row
    stats = {
        "pairs_case_a": int(pairs_both or 0),
        "pairs_case_b": int(pairs_one_side or 0),
        "pairs_case_c": 0,            # always zero in v5 (no synth)
        "rows_summed":  0,            # always zero in v5 (no synth)
        "output_pairs": int(output_pairs or 0),
    }

    sidecar = _stats_sidecar_path(year_str)
    try:
        if os.path.exists(sidecar):
            with open(sidecar, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
        else:
            payload = {}
        payload["clean"] = stats
        progress_mod.atomic_write_json(sidecar, payload)
    except Exception as e:
        logger.warning(
            "Could not write clean stats into sidecar %s: %s", sidecar, e
        )

    _notify(
        f"✅ <b>{year_str}</b> clean step: "
        f"{stats['output_pairs']:,} pairs in clean.csv "
        f"(both sides had motCode=0; "
        f"{stats['pairs_case_b']:,} other pairs dropped because only "
        f"one side had a motCode=0 row)",
        notify,
    )
    return stats


