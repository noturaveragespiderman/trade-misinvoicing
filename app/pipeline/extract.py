"""
AGGREGATE phase: build ./aggregate/{year}.csv from cached .gz files.

APPEND-ONLY + CRASH-RESUMABLE: each reporter's filtered output is written
to its own partial file under ``aggregate/_partials/{year}/{reporter}.csv``
and committed atomically (.tmp + os.replace). After every reporter
completes, the year CSV is rebuilt by concatenating partials.

A small progress sidecar at ``aggregate/{year}_progress.json`` records
which reporters have finished. On the next run we read that JSON, drop
any reporters whose partial files have gone missing (validation), and
submit only the not-yet-completed reporters to the worker pool. A crash
mid-run resumes from the next not-yet-committed reporter; no rows are
ever processed twice.

Delimiter handling: Comtrade .gz files may be comma- OR tab-delimited.
The script auto-detects the separator from the first line of each file.

Row filters applied here (filtered columns are NOT stored):
  typeCode                 != 'C'        → drop services / non-commodity rows
  freqCode                 != 'A'        → drop monthly / sub-annual rows
  refYear                  != year       → drop cross-year contamination
  classificationSearchCode != 'HS'       → drop non-HS classifications
  partnerCode == 0 AND partner2Code == 0 → drop World-aggregate / no real partner
  cmdCode == 0 / null / non-numeric      → drop rows with no usable HS code
  reporterCode == 0 / null               → drop rows with no usable reporter
  flowCode not in {'M', 'X'}             → drop RX / RM / DM / numeric / blank
  reporterCode == partnerCode            → drop self-trade rows (a country
                                            cannot import from itself)
  customsCode != 'C00'                   → drop everything except the "all customs
                                            procedures" total, so downstream phases
                                            don't double-count component codes
  all four estimation flags True         → drop rows where qty, altQty, netWgt and
                                            grossWgt are all estimated (no reliable value)
  exact duplicate (all KEEP_COLUMNS)    → drop duplicate rows within same reporter chunk

Drop counters are tallied at the chunk level. Each ``rej_*`` total has a
sibling entry in ``stats["by_hs_level"]`` keyed by ``HS2`` / ``HS4`` /
``HS6`` / ``Unknown`` (last bucket catches filters that fire before
``cmdCode`` validation). The aggregate-side report consumes this
breakdown so every dropped row is traceable to a reason AND a level.

Columns written to the CSV: config.KEEP_COLUMNS (case-insensitive match).

Performance notes
-----------------
* Reads use a typed ``dtype=`` map (Int64 for partner/reporter codes;
  string for cmdCode), so per-chunk ``pd.to_numeric(...)`` calls and
  the eight-deep ``.astype(str).str.upper().str.strip()`` chains the
  old code paid for every filter are gone.
* Filter-key string columns are normalised exactly once per chunk
  (``.str.upper().str.strip()``), then compared with simple equality.
* ``cmdCode`` normalisation is now vectorised — we build a
  ``{raw → padded}`` lookup from the chunk's ``Series.unique()`` and
  ``.map()`` it back in C, instead of ``.apply(lambda c: ...)`` over
  millions of rows.
* Per-reporter writes go through a single kept-open ``csv.writer``,
  not a per-chunk ``df.to_csv(mode='a')`` reopen.
* Across .gz files we fan out to a ``ProcessPoolExecutor`` — gzip
  decompression and pandas filtering are CPU-bound, so multi-process
  parallelism gives near-linear speedup until disk bandwidth saturates.
"""

import glob
import gzip
import json
import os
import re
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

from app.config import (
    RAW_DIR,
    AGGREGATE_DIR,
    KEEP_COLUMNS,
    FILTER_COLUMNS,
)
from app.core.notifier import check_for_stop, StopPipelineException
from app.core.logger import get_logger
from app.core import progress as progress_mod

logger = get_logger(__name__)

# Direct exports / imports only — RX / RM / DM / numeric / blanks are
# dropped at this stage so the matcher gets a clean, narrowed input.
KEEP_FLOWS = {"M", "X"}

# pyarrow-backed dtypes ("string[pyarrow]", Arrow-backed nullable ints)
# use ~3-4× less memory than the Python-object equivalents and skip the
# per-cell GIL roundtrip. We probe import once at module load and gate
# the dtype_backend kwarg accordingly so the code still works on a host
# that hasn't installed pyarrow yet.
try:
    import pyarrow  # noqa: F401

    _ARROW_AVAILABLE = True
except ImportError:  # pragma: no cover — only hit on hosts without pyarrow
    _ARROW_AVAILABLE = False

_READ_KWARGS = {
    "dtype_backend": "pyarrow" if _ARROW_AVAILABLE else "numpy_nullable",
}

# Worker count for the per-.gz fan-out. Each worker peaks at roughly one
# chunk in RAM (~200-400 MB after typed reads), so cpu_count-1 is safe
# on a 16 GB box; override via env when the host is smaller.
def _default_worker_count(n_jobs):
    env_override = os.environ.get("COMTRADE_WORKERS")
    if env_override:
        try:
            n = int(env_override)
            if n > 0:
                return min(n_jobs, n)
        except ValueError:
            pass
    return min(n_jobs, max(1, (os.cpu_count() or 4) - 1))


# ---------------------------------------------------------------------------
# Per-year stats sidecar — persisted automatically so the Reports phase
# (option 5 → Aggregate report) can rebuild the xlsx anytime without redoing
# AGGREGATE. Without this the per-reporter drop counters would only live in
# memory for the duration of the run.
# ---------------------------------------------------------------------------
def _stats_sidecar_path(year_str):
    return os.path.join(AGGREGATE_DIR, f"{year_str}_stats.json")


def _save_aggregate_stats(year_str, stats):
    """Merge this run's per-reporter stats into any existing sidecar.
    AGGREGATE is append-only across runs, so the sidecar accumulates the
    same way the data CSV does."""
    if not stats:
        return
    path = _stats_sidecar_path(year_str)
    existing = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            existing = {int(k): v for k, v in raw.items()}
        except Exception as e:
            logger.warning("Could not read existing aggregate stats %s: %s", path, e)
    existing.update(stats)
    try:
        serialisable = {str(int(k)): v for k, v in existing.items()}
        progress_mod.atomic_write_json(path, serialisable)
    except Exception as e:
        logger.warning("Could not write aggregate stats %s: %s", path, e)


# All columns we need to READ (output + filter)
_LOAD_COLUMNS = list(set(KEEP_COLUMNS + FILTER_COLUMNS + ["classificationSearchCode"]))

# Matches COMTRADE-FINAL-CA{reporter}{year}H{rev}[date].gz
_FN_RE = re.compile(r"CA(\d+?)(\d{4})H\d", re.IGNORECASE)

# Columns we want as nullable Int64 straight out of read_csv. Keeps the
# per-chunk pd.to_numeric() ladder out of the filter loop.
_INT_COLUMNS = {"reporterCode", "partnerCode", "partner2Code"}

# String-key columns that need to be uppercased+stripped for filtering.
# We normalise each ONCE per chunk (not per filter rule).
_FILTER_KEY_COLUMNS = ("typeCode", "freqCode", "flowCode", "classificationSearchCode")


# ──────────────────────────────────────────────
# Path helpers — partial-file layout
# ──────────────────────────────────────────────
def _partials_dir(year_str):
    return os.path.join(AGGREGATE_DIR, "_partials", year_str)


def _partial_path(year_str, reporter_int):
    """Canonical (uncompressed) write path for a per-reporter partial."""
    return os.path.join(_partials_dir(year_str), f"{int(reporter_int)}.csv")


def _partial_read_path(year_str, reporter_int):
    """Resolve which on-disk variant of a partial exists (plain or .gz).
    Returns None if neither is present. Used by the finalizer + the
    progress validator so partials gzipped by shrink_year still count
    as 'completed' on the next run."""
    plain = _partial_path(year_str, reporter_int)
    if os.path.exists(plain):
        return plain
    gz = plain + ".gz"
    if os.path.exists(gz):
        return gz
    return None


def _legacy_partial_path(year_str):
    """Where the pre-existing aggregate/{year}.csv (if any) gets staged
    so the finalizer can include its rows in the rebuilt year CSV."""
    return os.path.join(_partials_dir(year_str), "_legacy.csv")


def _legacy_partial_read_path(year_str):
    plain = _legacy_partial_path(year_str)
    if os.path.exists(plain):
        return plain
    gz = plain + ".gz"
    if os.path.exists(gz):
        return gz
    return None


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _detect_sep(header_line):
    """Auto-detect whether a header line is tab or comma separated."""
    if "\t" in header_line:
        return "\t"
    return ","


def _resolve_columns(header):
    """
    Case-insensitive mapping: _LOAD_COLUMNS canonical name → actual header name.
    Returns only columns that exist in this file's header.
    """
    lower_map = {c.strip().lower(): c.strip() for c in header}
    resolved = {}
    for col in _LOAD_COLUMNS:
        actual = lower_map.get(col.lower())
        if actual:
            resolved[col] = actual
    return resolved


def _scan_legacy_csv_reporter_codes(year_csv_path):
    """One-time CSV scan used to bootstrap the progress JSON when an
    existing aggregate/{year}.csv predates the partials-based layout.

    Returns a set of int reporter codes, or empty if the file is missing
    or unreadable. Subsequent runs read the progress JSON instead — the
    multi-GB scan only happens once per upgrade.
    """
    if not os.path.exists(year_csv_path):
        return set()
    try:
        with open(year_csv_path, "r", encoding="utf-8") as fh:
            header = fh.readline().strip().split(",")
        lower_map = {c.lower(): c for c in header}
        col = lower_map.get("reportercode")
        if not col:
            return set()
        codes = set()
        for chunk in pd.read_csv(
            year_csv_path, usecols=[col], dtype=str, chunksize=500_000
        ):
            series = pd.to_numeric(chunk[col], errors="coerce").dropna().astype(int)
            codes.update(int(c) for c in series.unique())
        return codes
    except Exception as e:
        logger.warning("Could not scan legacy aggregate CSV: %s", e)
        return set()


def _bootstrap_progress_from_legacy(year_str):
    """If an aggregate/{year}.csv exists but no progress JSON does,
    scan the legacy file once and seed the progress sidecar so the
    next run skips the rescan. Also stash the legacy file as a
    pseudo-partial so the finalizer can include its rows after new
    reporters arrive."""
    year_csv = os.path.join(AGGREGATE_DIR, f"{year_str}.csv")
    if not os.path.exists(year_csv):
        return set()
    logger.info(
        "No progress sidecar for %s — scanning legacy aggregate CSV once "
        "to bootstrap state.",
        year_str,
    )
    codes = _scan_legacy_csv_reporter_codes(year_csv)
    if not codes:
        return set()
    os.makedirs(_partials_dir(year_str), exist_ok=True)
    legacy_partial = _legacy_partial_path(year_str)
    # Move (don't copy) so disk usage doesn't double on the upgrade run.
    # If the rename across filesystems fails, fall back to copy + remove.
    if not os.path.exists(legacy_partial):
        try:
            os.replace(year_csv, legacy_partial)
        except OSError:
            shutil.copy2(year_csv, legacy_partial)
            try:
                os.remove(year_csv)
            except OSError:
                pass
    progress_mod.init_year_progress(
        "aggregate",
        year_str,
        expected_reporters=codes,
        partials_dir=_partials_dir(year_str),
    )
    payload = progress_mod.load_progress("aggregate", year_str) or {}
    payload["completed_reporters"] = sorted(int(c) for c in codes)
    payload["legacy_partial"] = legacy_partial
    progress_mod.save_progress("aggregate", year_str, payload)
    return codes


# ──────────────────────────────────────────────
# Worker — runs in a child process; must be picklable / side-effect free
# ──────────────────────────────────────────────
_HS_BUCKETS = ("HS2", "HS4", "HS6", "Unknown")


def _empty_hs_bucket():
    return {b: 0 for b in _HS_BUCKETS}


def _empty_stats():
    return {
        "total": 0,
        "kept": 0,
        "dropped": 0,
        "rej_typeCode": 0,
        "rej_freqCode": 0,
        "rej_refYear": 0,
        "rej_classificationSearchCode": 0,
        "rej_partner_zero": 0,
        "rej_cmdCode": 0,
        "rej_reporterCode": 0,
        "rej_flowCode": 0,
        "rej_self_trade": 0,
        "rej_customsCode_not_c00": 0,
        "rej_all_estimated": 0,
        "rej_exact_duplicate": 0,
        "rej_hs_level": 0,
        "rej_commodity_filter": 0,
        # Per-reason HS-level breakdown. Keys are the same rej_* names
        # (plus "kept" for the survivors) and each value is a
        # {HS2, HS4, HS6, Unknown} dict. The aggregate report uses this
        # to show how each filter slices across HS granularities.
        "by_hs_level": {},
    }


def _hs_level_series(cmd_series):
    """Map a cmdCode Series → an object Series of HS2 / HS4 / HS6 / Unknown.

    Buckets follow ``len(str.strip(cmdCode))``: ``<= 2 → HS2``, ``<= 4 →
    HS4``, ``>= 5 → HS6``, empty / NaN → Unknown. Cheap to compute, run
    once per chunk.
    """
    L = cmd_series.astype(str).str.strip().str.len()
    out = np.where(
        L <= 0, "Unknown",
        np.where(L <= 2, "HS2", np.where(L <= 4, "HS4", "HS6")),
    )
    return pd.Series(out, index=cmd_series.index, dtype=object)


def _bump_hs(stats, reason, mask, hs_level):
    """Increment ``stats['by_hs_level'][reason]`` from ``mask`` rows.

    ``mask`` is a boolean Series the same length as ``hs_level``; only
    True positions contribute.
    """
    if not bool(mask.any()):
        return
    bucket = stats["by_hs_level"].setdefault(reason, _empty_hs_bucket())
    counts = hs_level[mask].value_counts()
    for level, n in counts.items():
        bucket[level] = bucket.get(level, 0) + int(n)


def _process_one_gz(reporter_int, gz_path, year_str):
    """Filter ONE .gz file and write its rows to the per-reporter
    partial CSV. Designed for ``ProcessPoolExecutor`` — returns either
    ``(reporter_int, stats_dict)`` or ``(reporter_int, {"_error": "..."})``.

    The function is intentionally side-effect-light: it does not touch
    progress JSON or log status lines. The main process owns those.
    """
    try:
        with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as fh:
            header_line = fh.readline().strip()
    except Exception as e:
        return reporter_int, {"_error": f"cannot read header: {e}"}

    sep = _detect_sep(header_line)
    raw_header = [c.strip() for c in header_line.split(sep)]
    col_map = _resolve_columns(raw_header)
    if not col_map:
        return reporter_int, {
            "_error": f"no recognised columns (sep={sep!r}, {len(raw_header)} cols)"
        }
    use_cols = list(col_map.values())
    rename_to = {v: k for k, v in col_map.items()}

    # Numeric dtype map applied at read time — no per-chunk pd.to_numeric.
    int_dtype_map = {
        col_map[c]: "Int64"
        for c in _INT_COLUMNS
        if c in col_map
    }

    stats = _empty_stats()

    partial_dir = _partials_dir(year_str)
    os.makedirs(partial_dir, exist_ok=True)
    final_path = _partial_path(year_str, reporter_int)
    tmp_path = final_path + ".tmp"
    if os.path.exists(tmp_path):
        try:
            os.remove(tmp_path)
        except OSError:
            pass

    # Single kept-open writer — one file open, one fsync, one rename.
    out_fh = open(tmp_path, "w", encoding="utf-8", newline="")
    out_fh.write(",".join(KEEP_COLUMNS) + "\n")

    try:
        reader = pd.read_csv(
            gz_path,
            sep=sep,
            usecols=use_cols,
            dtype=int_dtype_map,
            chunksize=500_000,
            low_memory=False,
            compression="gzip",
            encoding_errors="replace",
            engine="c",
            **_READ_KWARGS,
        )
        for chunk in reader:
            chunk.rename(columns=rename_to, inplace=True)
            n = len(chunk)
            stats["total"] += n
            if n == 0:
                continue

            # Normalise the four filter-key string columns ONCE per chunk.
            # The old code did .astype(str).str.upper().str.strip() inside
            # every filter expression — eight passes; we do four.
            for c in _FILTER_KEY_COLUMNS:
                if c in chunk.columns:
                    chunk[c] = chunk[c].astype(str).str.strip().str.upper()

            # HS-level series per chunk — used for the by_hs_level
            # breakdown on every filter. Cheap (one str.len + np.where).
            if "cmdCode" in chunk.columns:
                hs_level = _hs_level_series(chunk["cmdCode"])
            else:
                hs_level = pd.Series(
                    "Unknown", index=chunk.index, dtype=object
                )

            dropped_mask = pd.Series(False, index=chunk.index)

            def _apply(reason, bad):
                """Tally a filter's victims into both the flat counter and
                the per-HS-level breakdown, then fold into dropped_mask."""
                nonlocal dropped_mask
                new = bad & ~dropped_mask
                stats[reason] += int(new.sum())
                _bump_hs(stats, reason, new, hs_level)
                dropped_mask |= bad

            if "typeCode" in chunk.columns:
                _apply("rej_typeCode", chunk["typeCode"] != "C")

            if "freqCode" in chunk.columns:
                _apply("rej_freqCode", chunk["freqCode"] != "A")

            if "refYear" in chunk.columns:
                _apply(
                    "rej_refYear",
                    chunk["refYear"].astype(str).str.strip() != year_str,
                )

            if "classificationSearchCode" in chunk.columns:
                _apply(
                    "rej_classificationSearchCode",
                    chunk["classificationSearchCode"] != "HS",
                )

            # partnerCode == 0 AND partner2Code == 0 — coerce missing values
            # to -1 so the (0,0) check tests the genuine "World" sentinel
            # instead of catching NaN→0 conversions. partnerCode is already
            # Int64 from the typed read; .fillna(-1) is the only call left.
            if "partnerCode" in chunk.columns and "partner2Code" in chunk.columns:
                p1 = chunk["partnerCode"].fillna(-1)
                p2 = chunk["partner2Code"].fillna(-1)
                _apply("rej_partner_zero", (p1 == 0) & (p2 == 0))

            if "cmdCode" in chunk.columns:
                # cmdCode kept as string for leading-zero semantics, but
                # we still want a numeric check for "is it a real HS code".
                cmd = pd.to_numeric(chunk["cmdCode"], errors="coerce")
                _apply("rej_cmdCode", cmd.isna() | (cmd == 0))

                # HS-level filter (config.HS_LEVEL). Keep only rows whose
                # cmdCode length matches the requested HS granularity.
                # "ALL" keeps every level. cmdCode bucketing rules match
                # `_hs_level_series`: <=2 → HS2, <=4 → HS4, >=5 → HS6.
                from app.config import HS_LEVEL as _HS_LEVEL_CFG
                _hs_cfg = (_HS_LEVEL_CFG or "ALL").upper()
                if _hs_cfg in {"HS2", "HS4", "HS6"}:
                    _apply("rej_hs_level", hs_level != _hs_cfg)

                # Commodity prefix filter (config.COMMODITY_FILTER).
                # Empty tuple = no restriction. A row is kept if its
                # cmdCode (zero-padded leading-zero string) starts with
                # any of the configured prefixes.
                from app.config import COMMODITY_FILTER as _CMD_FILTER_CFG
                if _CMD_FILTER_CFG:
                    prefixes = tuple(str(p).strip() for p in _CMD_FILTER_CFG if str(p).strip())
                    if prefixes:
                        cmd_str = chunk["cmdCode"].astype(str).str.strip()
                        bad = ~cmd_str.str.startswith(prefixes)
                        _apply("rej_commodity_filter", bad)

            if "reporterCode" in chunk.columns:
                rc = chunk["reporterCode"]
                _apply("rej_reporterCode", rc.isna() | (rc == 0))

            if "flowCode" in chunk.columns:
                _apply("rej_flowCode", ~chunk["flowCode"].isin(KEEP_FLOWS))

            # Self-trade: a country cannot import from itself. We coerce
            # NaN to -1 on both sides so a missing partnerCode doesn't
            # accidentally match a missing reporterCode (both NaN→-1
            # would otherwise compare equal).
            if "reporterCode" in chunk.columns and "partnerCode" in chunk.columns:
                rc = chunk["reporterCode"].fillna(-1)
                pc = chunk["partnerCode"].fillna(-2)
                _apply("rej_self_trade", rc == pc)

            # customsCode hard gate — keep only "C00" rows (the "all
            # customs procedures" total). Component codes (C01, C02, …)
            # carry partial breakdowns that would double-count against
            # the C00 aggregate if both made it past this stage. Missing
            # / null customsCode is also dropped because it's neither
            # the documented total nor a recognised component.
            if "customsCode" in chunk.columns:
                _apply(
                    "rej_customsCode_not_c00",
                    chunk["customsCode"].fillna("").astype(str).str.strip().str.upper()
                    != "C00",
                )

            # Drop rows where ALL four quantity/weight fields are estimated
            # (no reliable measurement exists for any dimension).
            est_cols = [
                "isQtyEstimated", "isAltQtyEstimated",
                "isNetWgtEstimated", "isGrossWgtEstimated",
            ]
            present_est = [c for c in est_cols if c in chunk.columns]
            if len(present_est) == 4:
                def _is_truthy(s):
                    return s.astype(str).str.strip().str.lower().isin({"true", "1", "t", "yes"})
                all_est = _is_truthy(chunk[present_est[0]])
                for col in present_est[1:]:
                    all_est &= _is_truthy(chunk[col])
                _apply("rej_all_estimated", all_est)

            stats["dropped"] += int(dropped_mask.sum())
            chunk = chunk[~dropped_mask]
            hs_level = hs_level[~dropped_mask]

            # Drop exact duplicates within this chunk (same values on all kept columns).
            if not chunk.empty:
                before = len(chunk)
                keep_cols_present = [c for c in KEEP_COLUMNS if c in chunk.columns]
                chunk = chunk.drop_duplicates(subset=keep_cols_present, keep="first")
                n_dupe = before - len(chunk)
                stats["rej_exact_duplicate"] += n_dupe
                # The HS breakdown for duplicates is by the chunk row
                # order before the dedup; we don't track which rows were
                # collapsed, so we attribute the drop to the kept rows'
                # HS distribution.
                if n_dupe and not chunk.empty:
                    hs_level = hs_level.loc[chunk.index]
                    bucket = stats["by_hs_level"].setdefault(
                        "rej_exact_duplicate", _empty_hs_bucket()
                    )
                    proportions = hs_level.value_counts(normalize=True)
                    for level, share in proportions.items():
                        bucket[level] = bucket.get(level, 0) + int(round(n_dupe * share))

            stats["kept"] += len(chunk)
            if not chunk.empty:
                kept_bucket = stats["by_hs_level"].setdefault(
                    "kept", _empty_hs_bucket()
                )
                for level, n in hs_level.loc[chunk.index].value_counts().items():
                    kept_bucket[level] = kept_bucket.get(level, 0) + int(n)

            if chunk.empty:
                continue

            # Write only KEEP_COLUMNS in canonical order; pad missing
            # columns with empty strings so partials are concat-safe
            # across .gz files with different HS revisions / schemas.
            for c in KEEP_COLUMNS:
                if c not in chunk.columns:
                    chunk[c] = ""
            out_chunk = chunk[KEEP_COLUMNS]
            out_chunk.to_csv(out_fh, index=False, header=False, lineterminator="\n")

        out_fh.flush()
        os.fsync(out_fh.fileno())
        out_fh.close()
        os.replace(tmp_path, final_path)
        return reporter_int, stats
    except Exception as e:
        try:
            out_fh.close()
        except Exception:
            pass
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        return reporter_int, {"_error": f"{type(e).__name__}: {e}"}


# ──────────────────────────────────────────────
# Finalizer — concatenate per-reporter partials into aggregate/{year}.csv
# ──────────────────────────────────────────────
def _finalize_year_csv(year_str, completed_reporters, include_legacy=True):
    """Stitch all per-reporter partial CSVs into ``aggregate/{year}.csv``.

    Streaming byte copy with a single header — no DataFrame is ever held
    in RAM during finalize, so the year CSV scales with disk space, not
    memory. Atomic via .tmp + os.replace.
    """
    out_path = os.path.join(AGGREGATE_DIR, f"{year_str}.csv")
    tmp_path = out_path + ".tmp"
    if os.path.exists(tmp_path):
        try:
            os.remove(tmp_path)
        except OSError:
            pass

    # Order: legacy (if any) first, then partials sorted by reporter int.
    # Partials may be either plain .csv or .csv.gz (shrink_year compresses
    # them in place after the year completes). _open_partial transparently
    # decompresses on the read side so the finalizer doesn't care.
    sources = []
    legacy = _legacy_partial_read_path(year_str) if include_legacy else None
    if legacy:
        sources.append(legacy)
    sources.extend(
        p for rc in sorted(completed_reporters)
        if (p := _partial_read_path(year_str, rc)) is not None
    )

    if not sources:
        return None

    def _open_partial(path):
        if path.endswith(".gz"):
            return gzip.open(path, "rb")
        return open(path, "rb")

    written = 0
    with open(tmp_path, "wb") as fout:
        first = True
        for src in sources:
            with _open_partial(src) as fin:
                if first:
                    shutil.copyfileobj(fin, fout, 4 * 1024 * 1024)
                    first = False
                else:
                    fin.readline()  # skip per-partial header line
                    shutil.copyfileobj(fin, fout, 4 * 1024 * 1024)
            written += 1
    os.replace(tmp_path, out_path)
    return out_path


# ──────────────────────────────────────────────
# Main function
# ──────────────────────────────────────────────
def build_aggregate_csv(year, targets=None):
    """
    AGGREGATE phase: combine all .gz files for `year` into
    ./aggregate/{year}.csv after applying the eight drop filters
    documented in the module header.

    Parameters
    ----------
    year    : str or int
    targets : list of int — optional reporter code filter; None = all

    Returns
    -------
    (out_path, stats)
      out_path : path to the csv, or None if no new rows were written
      stats    : dict[reporter_int, drop_counters_dict] for reporters
                 processed in this run (already-aggregated reporters
                 are skipped silently and not present in stats).
    """
    os.makedirs(AGGREGATE_DIR, exist_ok=True)
    year_str = str(year)
    out_path = os.path.join(AGGREGATE_DIR, f"{year_str}.csv")

    # Search raw/{year}/ first (per-year subfolder layout), then raw/ root.
    from app.config import raw_year_dir
    search_dirs = [raw_year_dir(year_str), RAW_DIR]
    gz_files_set = set()
    for d in search_dirs:
        if os.path.isdir(d):
            gz_files_set.update(glob.glob(os.path.join(d, f"*{year_str}*.gz")))
    gz_files = sorted(gz_files_set)
    if not gz_files:
        logger.warning("No .gz files found for %s", year_str)
        return None, {}

    target_set = {int(t) for t in targets} if targets is not None else None

    # Discover (reporter_int, gz_path) pairs for this year.
    discovered = []
    for gz_path in gz_files:
        m = _FN_RE.search(os.path.basename(gz_path))
        if not m:
            logger.warning("Cannot parse reporter/year from: %s", os.path.basename(gz_path))
            continue
        if m.group(2) != year_str:
            continue
        reporter_int = int(m.group(1))
        if target_set is not None and reporter_int not in target_set:
            continue
        discovered.append((reporter_int, gz_path))

    if not discovered:
        logger.warning("No matching .gz files for %s after filtering targets", year_str)
        return None, {}

    # Sweep stale .tmp from a crashed previous run (year CSV tmp + partial tmps).
    progress_mod.sweep_stale_tmp(AGGREGATE_DIR)
    progress_mod.sweep_stale_tmp(_partials_dir(year_str))

    expected_reporters = sorted({rc for rc, _ in discovered})

    # Bootstrap progress JSON from a pre-existing aggregate/{year}.csv if needed.
    if progress_mod.load_progress("aggregate", year_str) is None:
        _bootstrap_progress_from_legacy(year_str)

    # Initialise / refresh progress JSON for this run.
    progress_mod.init_year_progress(
        "aggregate",
        year_str,
        expected_reporters=expected_reporters,
        partials_dir=_partials_dir(year_str),
    )

    # Validate completed reporters against the partials directory — drop
    # any whose partial file has gone missing (e.g. user deleted it).
    payload = progress_mod.validate_progress(
        "aggregate",
        year_str,
        partial_resolver=lambda rc: _partial_read_path(year_str, rc),
    ) or {}
    completed = set(int(c) for c in payload.get("completed_reporters", []))

    pending = [(rc, gz) for rc, gz in discovered if rc not in completed]
    if not pending:
        logger.info(
            "All %d reporter(s) already completed for %s — finalising year CSV.",
            len(discovered),
            year_str,
        )
        # Even with no new work, ensure the year CSV exists / is fresh.
        _finalize_year_csv(year_str, completed, include_legacy=True)
        return None, {}

    n_workers = _default_worker_count(len(pending))
    logger.info(
        "AGGREGATE %s — %d/%d reporter(s) pending; %d worker process(es).",
        year_str,
        len(pending),
        len(discovered),
        n_workers,
    )

    stats = {}
    rows_written = 0

    if n_workers <= 1:
        # Inline path — useful for debugging and on single-core hosts.
        for rc, gz in pending:
            check_for_stop()
            rc_int, result = _process_one_gz(rc, gz, year_str)
            _handle_worker_result(year_str, rc_int, result, stats)
            if isinstance(result, dict) and "_error" not in result:
                rows_written += result.get("kept", 0)
    else:
        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            futures = {
                pool.submit(_process_one_gz, rc, gz, year_str): rc
                for rc, gz in pending
            }
            try:
                for fut in as_completed(futures):
                    check_for_stop()
                    rc_int, result = fut.result()
                    _handle_worker_result(year_str, rc_int, result, stats)
                    if isinstance(result, dict) and "_error" not in result:
                        rows_written += result.get("kept", 0)
            except StopPipelineException:
                logger.warning("Stop requested — cancelling pending workers.")
                for f in futures:
                    f.cancel()
                pool.shutdown(cancel_futures=True)
                raise

    # Persist accumulated stats so the Reports phase can pick them up.
    _save_aggregate_stats(year_str, stats)

    # Refresh completed set after this run, then concat into year CSV.
    payload = progress_mod.load_progress("aggregate", year_str) or {}
    completed_after = set(int(c) for c in payload.get("completed_reporters", []))
    finalised_path = _finalize_year_csv(year_str, completed_after, include_legacy=True)

    if rows_written == 0 and finalised_path is None:
        logger.info("No new rows for %s", year_str)
        return None, stats

    if rows_written == 0:
        logger.info("%s — no new rows; year CSV refreshed from existing partials.", year_str)
    else:
        logger.info("%s.csv — %s new rows", year_str, f"{rows_written:,}")
    return finalised_path or out_path, stats


def _handle_worker_result(year_str, reporter_int, result, stats):
    """Apply a worker's result to the run-wide stats and progress JSON."""
    if isinstance(result, dict) and "_error" in result:
        logger.warning(
            "Reporter %s/%s failed: %s",
            reporter_int,
            year_str,
            result["_error"],
        )
        return
    if not isinstance(result, dict):
        logger.warning(
            "Reporter %s/%s returned unexpected payload: %r",
            reporter_int,
            year_str,
            result,
        )
        return
    stats[int(reporter_int)] = result
    progress_mod.mark_reporter_complete("aggregate", year_str, reporter_int)
    logger.info(
        "Reporter %s/%s done — %s/%s rows kept (drop %s)",
        reporter_int,
        year_str,
        f"{result.get('kept', 0):,}",
        f"{result.get('total', 0):,}",
        f"{result.get('dropped', 0):,}",
    )


