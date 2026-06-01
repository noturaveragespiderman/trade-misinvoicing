"""
MISINVOICING phase — compute the bilateral mirror residual per matched
trade group.

Reads ./match/{year}_matched.csv (rows the matcher grouped under a
shared matched_id) and produces:
  ./misinvoicing/{year}_misinvoicing.csv
  ./reports/{year}_misinvoicing_report.txt

Drop rules — a matched_id group is excluded if ANY contributing row on
EITHER side meets one of:
  - isReported  == 0 / false                (mirror-derived row, not a real report)
  - isAggregate == 1 / true                 (summed row, would double-count granular rows)
  - all four estimation flags are yes:
      isQtyEstimated AND isAltQtyEstimated AND isNetWgtEstimated AND
      isGrossWgtEstimated                   (Comtrade-imputed across the board)

Numeric metrics (qty, altQty, netWgt, grossWgt, primaryValue) are summed
within each side before differencing.

primaryValue gets a special treatment — A4 in the audit doc. Each
exporter ROW is multiplied by ``(1 + uplift_factor)`` where the
uplift factor is looked up by ``motCode`` (mode of transport) in
``app/data/reference/cif_fob_factor.csv``. The uplifted rows are
then summed per side. Sea freight is ~5 %, air freight ~20 %,
pipelines ~2 %, etc.; rows whose motCode is missing or absent from
the table fall back to ``CIF_FOB_DEFAULT`` (config.py).

The output CSV exposes the effective per-pair uplift rate
(``effective_uplift_rate``) so a reader can verify that the lookup
table behaved as intended.

Per A5 in the audit doc, ``CIFValue_mis`` and ``FOBValue_mis`` are
no longer emitted — Comtrade's ``CIFValue`` / ``FOBValue`` columns
are sparse and ``.sum()`` of NaN→0 makes the residual a function
of "is the importer's CIF column populated", not actual
misinvoicing. The per-side raw sums are kept for diagnostic
spot-checks but never as a residual.

Difference convention (suffix _mis = "misinvoicing residual"):
    <metric>_mis = exporter_side − importer_side
Positive → exporter reported MORE (over-invoiced exports / under-invoiced
imports). Negative → the reverse.
"""

import hashlib
import json
import os

import numpy as np
import pandas as pd

from app.config import (
    MATCH_DIR,
    CLEAN_DIR,
    MISINVOICING_DIR,
    CIF_FOB_DEFAULT,
    CIF_FOB_FACTOR_FILE,
    QTY_UNIT_CONVERSION_FILE,
)
from app.core.notifier import (
    check_for_stop,
    notify_user,
)
from app.core.db import load_entrepot_codes
from app.core.logger import get_logger
from app.core import progress as progress_mod

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Output schema version — bump this whenever the misinvoicing CSV
# schema changes (a column is added / removed / renamed, or its
# semantic meaning changes). The freshness-skip check below refuses
# to reuse a sidecar whose `output_schema_version` doesn't match
# this constant, so a Dockploy redeploy with new code automatically
# invalidates the on-VM cache and regenerates the CSV with the new
# schema. The user never has to `rm` anything.
#
# Convention: bump by +1, add a one-line entry below describing what
# changed. The skip check is purely numeric — newer sidecars stored
# under a different version number are also treated as stale (which
# only matters if you downgrade the code, which is fine).
#
# Version history:
#   1: original schema (ID, qty/altQty/netWgt/grossWgt/CIFValue/FOBValue
#      _exp/_imp/_mis triples + primaryValue with cif uplift).
#   2: A1+A4+A5+B1+B5+B6+C3 — dropped ID, dropped CIFValue_mis /
#      FOBValue_mis, added is_entrepot_pair, is_netwgt_imputed_*,
#      effective_uplift_rate.
#   3: slim output. Per-pair CSV is exactly 17 columns —
#      matched_id, exporterCode, importerCode, cmdCode, hs_level,
#      qtyUnitCode_X/M, qty_X/M/MISINVOICING, netWgt_X/M/MISINVOICING,
#      primaryValue_X/X_CIF/M/MISINVOICING. is_entrepot_pair,
#      is_netwgt_imputed_*, effective_uplift_rate, CIFValue_*, and
#      FOBValue_* moved out of the per-row CSV and into the sidecar
#      JSON (consumed by the unified report). qty residual is only
#      computed when qtyUnitCode_X == qtyUnitCode_M; mismatches get
#      counts.qty_unit_mismatch in the sidecar.
#   4: drops relaxed. Of the five previous group-drop rules only
#      drop_no_pair survives — pairs missing X or M are still dropped.
#      drop_isReported / drop_isAggregate / drop_fully_estimated /
#      drop_unsafe_aggregation are no longer applied per user request:
#      the misinvoicing CSV now includes pairs whose source rows carry
#      isReported=0, isAggregate=1, every-field-estimated, or mixed
#      safety columns. The four drop_* counts remain in the sidecar
#      (always 0) so reports.py doesn't need to change shape.
#   5: expanded residuals + qtyUnit conversion (current). Output CSV
#      grows from 17 to 27 columns:
#        - qty_M_in_X_unit         — qty_M converted to X's qtyUnitCode
#                                    (NaN if non-convertible)
#        - altQtyUnitCode_X/M, altQty_X/M, altQty_M_in_X_unit,
#          altQty_MISINVOICING     — altQty quartet, with same unit
#                                    conversion logic as qty
#        - grossWgt_X/M, grossWgt_MISINVOICING — gross weight residual
#                                    (always kg, no conversion needed)
#      Unit conversion uses app/data/reference/qty_unit_conversion.csv
#      (safe dimensional factors only). When units mismatch and aren't
#      convertible the residual stays NaN, same as schema v4. Sidecar
#      gains counts.qty_unit_converted / counts.altQty_unit_converted
#      for the fraction of mismatches that the conversion table
#      resolved.
#      Also: clean_matched_csv (match phase) was rewritten to keep
#      ONLY rows where isAggregate=1 AND motCode=0 — pairs missing the
#      strict row on either side are now excluded upstream.
OUTPUT_SCHEMA_VERSION = 5


# ---------------------------------------------------------------------------
# Per-year stats sidecar — persisted automatically so the Reports phase
# (option 5 → Complementary report) can rebuild the xlsx without redoing
# misinvoicing. Replaces the previous .txt report which auto-shipped.
# ---------------------------------------------------------------------------
def _stats_sidecar_path(year_str):
    return os.path.join(MISINVOICING_DIR, f"{year_str}_stats.json")


def _save_misinvoicing_stats(year_str, payload):
    os.makedirs(MISINVOICING_DIR, exist_ok=True)
    path = _stats_sidecar_path(year_str)
    try:
        progress_mod.atomic_write_json(path, payload)
    except Exception as e:
        logger.warning("Could not write misinvoicing stats %s: %s", path, e)


def load_misinvoicing_stats(year):
    """Public reader used by app.pipeline.reports."""
    path = _stats_sidecar_path(str(year))
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


# Attributes whose X-side / M-side disagreements are noted in the report.
DISCREPANCY_COLS = [
    "isOriginalClassification",
    "customsCode",
    "mosCode",
    "motCode",
    "qtyUnitCode",
    "isQtyEstimated",
    "altQtyUnitCode",
    "isAltQtyEstimated",
    "isNetWgtEstimated",
    "isGrossWgtEstimated",
    "isReported",
]

# Numeric metrics — summed per side, then differenced.
# After the motCode merge in the match phase, every side has exactly one
# row per matched_id, so the sum here is a no-op for the typical case
# but stays for defensiveness.
#
# qty is back in this list — the misinvoicing residual is computed for
# pairs whose X and M side report the same qtyUnitCode. Pairs whose
# units differ get qty_MISINVOICING = NaN (tracked under
# counts.qty_unit_mismatch in the sidecar).
#
# CIFValue / FOBValue are no longer summed — they were rarely populated
# and the legacy `_exp / _imp` diagnostic columns were dropped per the
# slim-output refactor. Per-pair uplift effectiveness is now reported
# only at the sidecar level.
NUMERIC_METRICS = ["netWgt", "primaryValue", "qty", "altQty", "grossWgt"]

# C2 — per-metric safety check, not a side-wide guard.
#
# After A2 (concordance), A3 (customs/mot total dedup), B5 (NaN
# propagation), and B6 (netWgt-imputed flag), the safety check that
# previously blocked any side with mixed flags is over-conservative:
# the metrics we actually sum (`netWgt`, `primaryValue`, `CIFValue`,
# `FOBValue`) are all in fixed units (kg, USD) and don't depend on
# `qtyUnitCode` / `altQtyUnitCode`, and the estimation flags for
# qty / altQty / grossWgt don't touch any summed column either.
# `isNetWgtEstimated` is propagated to the sidecar as a per-pair
# count rather than as a per-row column — a flag, not a drop reason.
# So the safety list is empty: no group is dropped here for "unsafe
# aggregation". The unified report consumes the imputed-pair count.
#
# Kept as a configurable list so a future fix can re-introduce a
# guard without re-shaping the drop pipeline.
SAFE_SUM_REQUIRED_SAME = []

# A row is "fully estimated" when all four of these flags are yes.
ESTIMATION_FLAGS = [
    "isQtyEstimated",
    "isAltQtyEstimated",
    "isNetWgtEstimated",
    "isGrossWgtEstimated",
]

# Yes-ish / no-ish string sets. Comtrade serialises booleans inconsistently
# across vintages (1/0, true/false, yes/no), hence the tolerant check.
# After C4: also keep an explicit no-ish set so a typoed value (`"ture"`,
# `"flase"`) is logged as suspicious rather than silently coerced to
# False — the previous "everything else → False" behaviour swallowed
# data-quality issues in the source files.
_YESISH = {"1", "true", "yes", "t", "y"}
_NOISH = {"0", "false", "no", "f", "n", ""}


def _yesish(series, *, where=""):
    """Coerce a string column to boolean True for any yes-ish value.

    Returns a bool Series. Logs once per column if any value falls
    outside `_YESISH ∪ _NOISH` so that typos / unexpected values get
    flagged at the audit level rather than treated as False (the
    permissive behaviour was C4 in the audit doc)."""
    norm = series.astype(str).str.strip().str.lower()
    is_yes = norm.isin(_YESISH)
    is_no = norm.isin(_NOISH)
    unknown = ~(is_yes | is_no)
    if unknown.any():
        # Sample a few unknown values for the warning so a noisy
        # column doesn't fill the log with the same string.
        sample = norm[unknown].drop_duplicates().head(5).tolist()
        logger.warning(
            "Boolean coercion saw %d value(s) outside yes/no in %s — "
            "treating as False. Examples: %r",
            int(unknown.sum()),
            where or "<unknown column>",
            sample,
        )
    return is_yes


# ---------------------------------------------------------------------------
# CIF/FOB factor lookup — keyed by motCode (Comtrade transport-mode code).
# Loaded once per process; the CSV lives at
# ``app/data/reference/cif_fob_factor.csv`` and is described in the
# audit (A4). Rows whose motCode is missing or absent from the table
# fall back to ``CIF_FOB_DEFAULT``.
# ---------------------------------------------------------------------------
_CIF_FOB_TABLE_CACHE = None


def _load_cif_fob_table():
    """Return ``{motCode_str: factor}`` from app/data/reference/cif_fob_factor.csv.

    Cached after first call. On any read error the function logs a
    warning and returns an empty dict, which silently degrades to
    ``CIF_FOB_DEFAULT`` for every row — preserving current behaviour
    rather than failing closed.
    """
    global _CIF_FOB_TABLE_CACHE
    if _CIF_FOB_TABLE_CACHE is not None:
        return _CIF_FOB_TABLE_CACHE
    try:
        tbl = pd.read_csv(CIF_FOB_FACTOR_FILE, dtype={"motCode": str})
        # The CSV's "motCode" is the canonical key; values like 0, 1, 2
        # are stored as strings to match the categorical motCode column
        # in matched.csv.
        if "motCode" not in tbl.columns or "factor" not in tbl.columns:
            logger.warning(
                "CIF/FOB factor table %s missing required columns "
                "(motCode, factor); falling back to default %.3f for all rows.",
                CIF_FOB_FACTOR_FILE,
                CIF_FOB_DEFAULT,
            )
            _CIF_FOB_TABLE_CACHE = {}
            return _CIF_FOB_TABLE_CACHE
        _CIF_FOB_TABLE_CACHE = {
            str(k).strip(): float(v)
            for k, v in zip(tbl["motCode"], tbl["factor"])
            if pd.notna(v)
        }
    except Exception as e:
        logger.warning(
            "Could not load CIF/FOB factor table %s: %s — "
            "falling back to default %.3f.",
            CIF_FOB_FACTOR_FILE,
            e,
            CIF_FOB_DEFAULT,
        )
        _CIF_FOB_TABLE_CACHE = {}
    return _CIF_FOB_TABLE_CACHE


# ---------------------------------------------------------------------------
# qtyUnitCode conversion table (schema v5+). Same shape as the
# CIF/FOB cache. Codes absent from the table are non-convertible —
# they only contribute to a residual when both sides agree on the
# code.
# ---------------------------------------------------------------------------
_QTY_UNIT_CONV_CACHE = None


def _load_qty_unit_conversion_table():
    """Return ``{code_str: {"dimension": str, "factor_to_base": float}}``
    from app/data/reference/qty_unit_conversion.csv.

    Conversion math (factor_to_base = how many base units one of THIS
    unit equals — e.g. 1 km = 1000 m, so km.factor_to_base = 1000):

        value_in_target = value_in_source * factor_source / factor_target

    Only valid when both source and target share the same dimension.
    Cached after first call; read errors degrade to an empty dict so
    every cross-unit residual falls through to NaN (preserves schema v4
    behaviour).
    """
    global _QTY_UNIT_CONV_CACHE
    if _QTY_UNIT_CONV_CACHE is not None:
        return _QTY_UNIT_CONV_CACHE
    try:
        tbl = pd.read_csv(QTY_UNIT_CONVERSION_FILE, dtype={"code": str})
        needed = {"code", "dimension", "factor_to_base"}
        if not needed.issubset(set(tbl.columns)):
            logger.warning(
                "qtyUnitCode conversion table %s missing required columns "
                "%s; cross-unit residuals will all be NaN.",
                QTY_UNIT_CONVERSION_FILE, sorted(needed),
            )
            _QTY_UNIT_CONV_CACHE = {}
            return _QTY_UNIT_CONV_CACHE
        out = {}
        for _, row in tbl.iterrows():
            try:
                code = str(row["code"]).strip()
                factor = float(row["factor_to_base"])
                if not (factor > 0):
                    continue
                out[code] = {
                    "dimension": str(row["dimension"]).strip(),
                    "factor_to_base": factor,
                }
            except (KeyError, ValueError, TypeError):
                continue
        _QTY_UNIT_CONV_CACHE = out
    except Exception as e:
        logger.warning(
            "Could not load qtyUnitCode conversion table %s: %s — "
            "all cross-unit residuals will be NaN.",
            QTY_UNIT_CONVERSION_FILE, e,
        )
        _QTY_UNIT_CONV_CACHE = {}
    return _QTY_UNIT_CONV_CACHE


def _convert_to_x_unit(value_m, unit_m, unit_x, conv_table):
    """Vectorised: convert ``value_m`` (Series) from per-row ``unit_m``
    codes to per-row ``unit_x`` codes. Returns a Series of the same
    index. NaN where conversion is not possible (different dimension,
    missing factor for either side, or NaN/empty unit code).

    Same-unit rows pass through unchanged (factor 1, no math).
    """
    if value_m is None or len(value_m) == 0:
        return value_m
    unit_m_s = unit_m.astype(str).str.strip()
    unit_x_s = unit_x.astype(str).str.strip()
    same = unit_m_s == unit_x_s

    if not conv_table:
        return value_m.where(same, np.nan)

    def _factor(code):
        e = conv_table.get(code)
        return e["factor_to_base"] if e else float("nan")

    def _dim(code):
        e = conv_table.get(code)
        return e["dimension"] if e else None

    f_m = unit_m_s.map(_factor)
    f_x = unit_x_s.map(_factor)
    d_m = unit_m_s.map(_dim)
    d_x = unit_x_s.map(_dim)

    convertible = (
        ~same
        & f_m.notna() & f_x.notna()
        & d_m.notna() & d_x.notna()
        & (d_m == d_x)
        & (f_x > 0)
    )
    converted = (
        value_m.astype(float, errors="ignore") * f_m / f_x
    ).where(convertible, np.nan)
    # Same-unit rows: pass through verbatim. Others: converted (or NaN
    # if non-convertible).
    return value_m.where(same, converted)


def _resolve_uplift_per_row(motcode_series):
    """Return a float-Series with the per-row CIF/FOB uplift factor
    aligned to ``motcode_series.index``. Lookup table miss → default."""
    table = _load_cif_fob_table()
    keys = motcode_series.astype(str).str.strip()
    factors = keys.map(table)
    # NaN where motCode wasn't in the table (e.g. blank, "nan", or a
    # code Comtrade emits that we don't know about). Fill with default.
    factors = factors.where(factors.notna(), CIF_FOB_DEFAULT)
    return factors.astype(float)


# `_notify` and `_h` are kept as thin aliases over the shared helpers
# in app.core.notifier — see that module for the canonical
# implementations. The aliases let existing call sites
# (`_notify(msg, notify_flag)` and `_h(path)`) keep their original
# shape while the bodies live in one place.
def _notify(msg, notify=True):
    notify_user(msg, send=notify)


def _h(path):
    return h(path)


# Mirror of match._load_matched_for_report — same matched.csv schema, same
# narrow dtypes. Loading with dtype=str triggered the same OOM pattern that
# previously bit the MATCH phase on a 16 GB VM (str-only ~3-4× larger than
# the typed frame). Keep this list in sync with match.py if columns change.
_MATCHED_DTYPES = {
    "id": "Int64",
    "matched_id": "Int64",
    "reporterCode": "Int64",
    "partnerCode": "Int64",
    "partner2Code": "Int64",
    "classificationCode": "category",
    "flowCode": "category",
    "cmdCode": "category",
    "customsCode": "category",
    "mosCode": "category",
    "motCode": "category",
    "qtyUnitCode": "category",
    "altQtyUnitCode": "category",
    "isOriginalClassification": "category",
    "isQtyEstimated": "category",
    "isAltQtyEstimated": "category",
    "isNetWgtEstimated": "category",
    "isGrossWgtEstimated": "category",
    "isReported": "category",
    "isAggregate": "category",
    "legacyEstimationFlag": "category",
    "qty": "float64",
    "altQty": "float64",
    "netWgt": "float64",
    "grossWgt": "float64",
    "CIFValue": "float64",
    "FOBValue": "float64",
    "primaryValue": "float64",
}


def _load_matched_typed(matched_csv):
    """Read matched.csv (or .csv.gz) with narrow dtypes (Int64 / category /
    float64) instead of dtype=str. Columns absent from _MATCHED_DTYPES
    default to str. pandas auto-detects gzip from the extension."""
    if matched_csv.endswith(".gz"):
        import gzip as _gz
        with _gz.open(matched_csv, "rt", encoding="utf-8") as fh:
            header = fh.readline().strip().split(",")
    else:
        with open(matched_csv, "r", encoding="utf-8") as fh:
            header = fh.readline().strip().split(",")
    dtype = {c: _MATCHED_DTYPES.get(c, str) for c in header}
    return pd.read_csv(matched_csv, dtype=dtype, low_memory=False)


# C5 — content-hash component of the freshness fingerprint.
# `os.path.getmtime` is preserved by ``cp -p`` and ``git checkout``,
# so a file moved across hosts can produce a stale skip even though
# its bytes changed. Hashing the first and last 1 MiB is essentially
# free and catches any real content edit (a malicious bit-flip in
# the middle is not a realistic threat for this pipeline).
def _quick_hash(path, window_mb=1):
    """SHA-256 over the first and last `window_mb` MB of `path`. Plus
    the total size, to disambiguate files that share both windows but
    differ in length. Returns a hex digest, or '' on read error."""
    try:
        size = os.path.getsize(path)
    except OSError:
        return ""
    window = window_mb * 1024 * 1024
    h = hashlib.sha256()
    h.update(str(size).encode("utf-8"))
    try:
        with open(path, "rb") as fh:
            head = fh.read(window)
            h.update(head)
            if size > 2 * window:
                fh.seek(-window, os.SEEK_END)
                tail = fh.read(window)
                h.update(tail)
    except OSError:
        return ""
    return h.hexdigest()


def _matched_csv_path(year_str):
    """Return the matched CSV that drives misinvoicing for this year.

    Preference order (first hit wins):
      1. ``clean/{year}.csv`` / ``.csv.gz``      — current location of
                                                   the strict-row clean
                                                   output (schema v5+).
      2. ``match/{year}_clean.csv`` / ``.csv.gz`` — legacy location for
                                                   clean files written
                                                   by older builds.
      3. ``match/{year}_matched.csv`` / ``.csv.gz`` — pre-clean fallback
                                                     when clean hasn't
                                                     been built yet.
    """
    for candidate in (
        os.path.join(CLEAN_DIR, f"{year_str}.csv"),
        os.path.join(CLEAN_DIR, f"{year_str}.csv.gz"),
        os.path.join(MATCH_DIR, f"{year_str}_clean.csv"),
        os.path.join(MATCH_DIR, f"{year_str}_clean.csv.gz"),
        os.path.join(MATCH_DIR, f"{year_str}_matched.csv"),
        os.path.join(MATCH_DIR, f"{year_str}_matched.csv.gz"),
    ):
        if os.path.exists(candidate):
            return candidate
    return None


def run_misinvoicing(year, notify=True):
    """
    Compute the per-pair misinvoicing residual for `year`.

    Returns (csv_path, report_path) — either may be None if inputs are
    missing or if no pair survived the drop rules.
    """
    year_str = str(year)
    matched_csv = _matched_csv_path(year_str)
    if matched_csv is None:
        _notify(
            f"⚠️ Cannot run misinvoicing for <b>{year_str}</b> — "
            f"match/{year_str}_matched.csv(.gz) not found. Run MATCH first.",
            notify,
        )
        return None, None

    # Idempotent skip — reuse the cached output ONLY when ALL of:
    #   (a) the previous run finished cleanly,
    #   (b) the input matched.csv hasn't changed (mtime + size + a
    #       content hash, so file-moves that preserve mtime can't
    #       fool us — see C5),
    #   (c) the cached output was written under the SAME schema
    #       version this code expects. Bumping OUTPUT_SCHEMA_VERSION
    #       (because a column changed) auto-invalidates the cache so
    #       a Dockploy redeploy regenerates the file with the new
    #       schema. The user never has to `rm` anything.
    existing = load_misinvoicing_stats(year_str)
    cur_hash = _quick_hash(matched_csv)
    cached_version = existing.get("output_schema_version") if existing else None
    schema_ok = cached_version == OUTPUT_SCHEMA_VERSION
    if (
        existing
        and existing.get("complete")
        and schema_ok
        and int(os.path.getmtime(matched_csv)) == existing.get("matched_csv_mtime")
        and os.path.getsize(matched_csv) == existing.get("matched_csv_size")
        and existing.get("matched_csv_hash") == cur_hash
        and existing.get("out_csv")
        and os.path.exists(existing["out_csv"])
    ):
        _notify(
            f"{STATUS_SKIP} <b>MISINVOICING — {year_str}</b> already complete "
            f"and matched.csv unchanged — skipping.",
            notify,
        )
        return existing["out_csv"], _stats_sidecar_path(year_str)
    if existing and existing.get("complete") and not schema_ok:
        _notify(
            f"{STATUS_SKIP} <b>MISINVOICING — {year_str}</b> sidecar is at "
            f"schema v{cached_version}, code expects v{OUTPUT_SCHEMA_VERSION} — "
            f"regenerating.",
            notify,
        )

    os.makedirs(MISINVOICING_DIR, exist_ok=True)
    # Output filename: misinvoicing/{year}.csv. The folder already names
    # the kind, so the doubled "_misinvoicing" suffix was redundant.
    out_csv = os.path.join(MISINVOICING_DIR, f"{year_str}.csv")

    _cif_table = _load_cif_fob_table()
    _cif_msg = (
        f"per-motCode lookup loaded ({len(_cif_table)} modes; "
        f"default {CIF_FOB_DEFAULT*100:.2f}%)"
        if _cif_table
        else f"flat default {CIF_FOB_DEFAULT*100:.2f}% — lookup table missing"
    )
    _notify(
        f"{PHASE_MISINVOICING} <b>MISINVOICING — {year_str}</b>\n"
        f"  Reading <code>{_h(matched_csv)}</code>\n"
        f"  CIF/FOB uplift on exporter primaryValue: {_cif_msg}",
        notify,
    )

    df = _load_matched_typed(matched_csv)
    if df.empty:
        _notify(f"{STATUS_WARN} {matched_csv} is empty — skipping.", notify)
        return None, None

    # matched_id and the numeric metrics already arrive typed; the boolean
    # and flow normalisations below remain because Comtrade encodes
    # booleans inconsistently (1/0, true/false, yes/no) and flowCode case
    # has not been canonicalised upstream.
    df = df[~df["matched_id"].isna()].copy()
    df["flowCode"] = df["flowCode"].astype(str).str.upper().str.strip()
    df["isReported"] = _yesish(df["isReported"], where="isReported")
    df["isAggregate"] = _yesish(df["isAggregate"], where="isAggregate")
    for c in ESTIMATION_FLAGS:
        if c in df.columns:
            df[c] = _yesish(df[c], where=c)
        else:
            df[c] = False

    df["_is_x"] = df["flowCode"] == "X"
    df["_is_m"] = df["flowCode"] == "M"

    # Per-group X / M presence — the only group-level flag still needed
    # for filtering. The four quality-based drops
    # (drop_isReported / drop_isAggregate / drop_fully_estimated /
    # drop_unsafe_aggregation) were removed in schema v4 per user
    # request. Their computation is also gone (saves a full pass over
    # ESTIMATION_FLAGS + a per-side nunique on SAFE_SUM_REQUIRED_SAME).
    per_group = df.groupby("matched_id", sort=False).agg(
        has_x=("_is_x", "any"),
        has_m=("_is_m", "any"),
    )

    drop_no_pair = ~(per_group["has_x"] & per_group["has_m"])
    keep = ~drop_no_pair

    # The four disabled drops stay in the sidecar dict at 0 so
    # reports.py and any downstream consumer can keep reading the
    # same keys without a schema-version branch on their side.
    counts = {
        "groups_total": int(len(per_group)),
        "drop_no_pair": int(drop_no_pair.sum()),
        "drop_isReported": 0,
        "drop_isAggregate": 0,
        "drop_fully_estimated": 0,
        "drop_unsafe_aggregation": 0,
        "kept": int(keep.sum()),
    }

    kept_ids = per_group.index[keep]
    df_kept = df[df["matched_id"].isin(kept_ids)]
    uplift_diagnostics = {}
    if df_kept.empty:
        _notify(
            f"⚠️ No pairs survived for {year_str} — writing report only.",
            notify,
        )
        out_csv = None
    else:
        x_rows = df_kept[df_kept["_is_x"]].copy()
        m_rows = df_kept[df_kept["_is_m"]]

        # ----------------------------------------------------------
        # A4 — per-row CIF/FOB uplift keyed on motCode.
        #
        # The exporter's primaryValue is FOB; the importer's is CIF.
        # We mark up each X-side ROW by the factor for its mode of
        # transport and then sum those uplifted values. Summing
        # uplifted rows is mathematically equivalent to applying a
        # weighted uplift at the side level, but it picks up the
        # right factor when a single side mixes multiple motCodes
        # (e.g. air + sea components on the same matched_id).
        # ----------------------------------------------------------
        if "motCode" in x_rows.columns:
            x_uplift = _resolve_uplift_per_row(x_rows["motCode"])
        else:
            x_uplift = pd.Series(CIF_FOB_DEFAULT, index=x_rows.index)
        x_rows["_uplift_factor"] = x_uplift.values
        x_rows["_primaryValue_uplifted"] = (
            x_rows["primaryValue"].astype(float) * (1.0 + x_rows["_uplift_factor"])
        )

        # ``primaryValue_exp_cif`` is the per-pair sum of the
        # individually-uplifted X rows, so a pair whose X side is
        # all sea-freight gets a 5 % effective uplift while a pair
        # whose X side is all air-freight gets 20 %. A pair that
        # mixes modes ends up with a value-weighted blend.
        #
        # B5 — `min_count=1` so that an all-NaN side produces NaN
        # rather than 0. Without this, a pair where the exporter's
        # netWgt was entirely missing emits `netWgt_X = 0` and
        # `netWgt_MISINVOICING = -netWgt_M`, which looks like total
        # smuggling but is actually missing data. NaN preserves
        # downstream filtering options (reports / consumers) and is
        # arithmetically distinguishable from a real zero.
        metric_cols_present = [c for c in NUMERIC_METRICS if c in x_rows.columns]
        x_sums = x_rows.groupby("matched_id", sort=False)[metric_cols_present].sum(
            min_count=1
        )
        x_uplifted_sum = x_rows.groupby("matched_id", sort=False)[
            "_primaryValue_uplifted"
        ].sum(min_count=1)
        metric_cols_m = [c for c in NUMERIC_METRICS if c in m_rows.columns]
        m_sums = m_rows.groupby("matched_id", sort=False)[metric_cols_m].sum(
            min_count=1
        )
        x_sums.columns = [f"{c}_X" for c in x_sums.columns]
        m_sums.columns = [f"{c}_M" for c in m_sums.columns]

        # qtyUnitCode / altQtyUnitCode per side — after the strict-row
        # filter there's one row per (matched_id, side), so .first()
        # picks the canonical value.
        def _first_col(rows, col, index):
            if col in rows.columns:
                return rows.groupby("matched_id", sort=False)[col].first()
            return pd.Series("", index=index)

        x_qty_unit    = _first_col(x_rows, "qtyUnitCode",    x_sums.index)
        m_qty_unit    = _first_col(m_rows, "qtyUnitCode",    m_sums.index)
        x_altqty_unit = _first_col(x_rows, "altQtyUnitCode", x_sums.index)
        m_altqty_unit = _first_col(m_rows, "altQtyUnitCode", m_sums.index)

        # B6 — propagate the netWgt-imputed flag per side. A pair
        # whose exporter side is built from rows with
        # `isNetWgtEstimated = true` should NOT contribute to weight-
        # based residuals (consumers can filter on the sidecar's
        # pairs_netwgt_imputed_either_side count), but its
        # primaryValue residual is still well-defined because the
        # value column is independent of how the weight was estimated.
        if "isNetWgtEstimated" in x_rows.columns:
            x_imputed = (
                x_rows.groupby("matched_id", sort=False)["isNetWgtEstimated"].any()
            )
        else:
            x_imputed = pd.Series(False, index=x_sums.index)
        if "isNetWgtEstimated" in m_rows.columns:
            m_imputed = (
                m_rows.groupby("matched_id", sort=False)["isNetWgtEstimated"].any()
            )
        else:
            m_imputed = pd.Series(False, index=m_sums.index)

        x_meta = x_rows.groupby("matched_id", sort=False).agg(
            exporterCode=("reporterCode", "first"),
            cmdCode=("cmdCode", "first"),
        )
        m_meta = m_rows.groupby("matched_id", sort=False).agg(
            importerCode=("reporterCode", "first"),
        )

        out = x_sums.join(m_sums, how="inner").join(x_meta).join(m_meta)
        out["primaryValue_X_CIF"] = x_uplifted_sum.reindex(out.index)
        out["qtyUnitCode_X"]    = x_qty_unit.reindex(out.index)
        out["qtyUnitCode_M"]    = m_qty_unit.reindex(out.index)
        out["altQtyUnitCode_X"] = x_altqty_unit.reindex(out.index)
        out["altQtyUnitCode_M"] = m_altqty_unit.reindex(out.index)

        # Entrepôt and netWgt-imputed flags are no longer emitted per
        # row (they bloated the CSV and are downstream concerns). They
        # are still tracked in the sidecar JSON for the unified report.
        entrepot_set = load_entrepot_codes()
        if entrepot_set:
            exp_in = out["exporterCode"].astype("int64").isin(entrepot_set)
            imp_in = out["importerCode"].astype("int64").isin(entrepot_set)
            entrepot_mask = (exp_in | imp_in)
        else:
            entrepot_mask = pd.Series(False, index=out.index)
        netwgt_imputed_mask = (
            x_imputed.reindex(out.index, fill_value=False).astype(bool)
            | m_imputed.reindex(out.index, fill_value=False).astype(bool)
        )

        # ------------------------------------------------------------
        # Residuals (schema v5).
        #
        # netWgt and grossWgt are always reported in kg per Comtrade
        # convention so they need no unit conversion. qty and altQty
        # carry their own unit codes per side — when the codes match
        # we subtract directly, and when they differ we attempt
        # dimensional conversion via _convert_to_x_unit (looks up the
        # qty_unit_conversion.csv reference). A non-convertible
        # mismatch leaves the residual as NaN.
        #
        # primaryValue is in USD per Comtrade convention; the importer
        # CIF is compared against the exporter FOB after the per-row
        # CIF/FOB uplift (motCode-keyed).
        # ------------------------------------------------------------
        conv_table = _load_qty_unit_conversion_table()

        # netWgt — always kg
        out["netWgt_MISINVOICING"] = out["netWgt_X"] - out["netWgt_M"]

        # grossWgt — always kg
        if "grossWgt_X" in out.columns and "grossWgt_M" in out.columns:
            out["grossWgt_MISINVOICING"] = out["grossWgt_X"] - out["grossWgt_M"]
        else:
            out["grossWgt_MISINVOICING"] = np.nan
            if "grossWgt_X" not in out.columns:
                out["grossWgt_X"] = np.nan
            if "grossWgt_M" not in out.columns:
                out["grossWgt_M"] = np.nan

        # primaryValue residual — importer CIF minus exporter-uplifted CIF
        out["primaryValue_MISINVOICING"] = (
            out["primaryValue_X_CIF"] - out["primaryValue_M"]
        )

        # qty residual — with unit conversion.
        if "qty_X" in out.columns and "qty_M" in out.columns:
            qty_units_x = out["qtyUnitCode_X"].astype(str).str.strip()
            qty_units_m = out["qtyUnitCode_M"].astype(str).str.strip()
            qty_units_match = qty_units_x == qty_units_m
            out["qty_M_in_X_unit"] = _convert_to_x_unit(
                out["qty_M"], qty_units_m, qty_units_x, conv_table
            )
            out["qty_MISINVOICING"] = out["qty_X"] - out["qty_M_in_X_unit"]
            qty_unit_mismatch = int((~qty_units_match).sum())
            qty_unit_converted = int(
                ((~qty_units_match) & out["qty_M_in_X_unit"].notna()).sum()
            )
        else:
            out["qty_M_in_X_unit"] = np.nan
            out["qty_MISINVOICING"] = np.nan
            qty_unit_mismatch = 0
            qty_unit_converted = 0

        # altQty residual — same conversion logic as qty.
        if "altQty_X" in out.columns and "altQty_M" in out.columns:
            altqty_units_x = out["altQtyUnitCode_X"].astype(str).str.strip()
            altqty_units_m = out["altQtyUnitCode_M"].astype(str).str.strip()
            altqty_units_match = altqty_units_x == altqty_units_m
            out["altQty_M_in_X_unit"] = _convert_to_x_unit(
                out["altQty_M"], altqty_units_m, altqty_units_x, conv_table
            )
            out["altQty_MISINVOICING"] = (
                out["altQty_X"] - out["altQty_M_in_X_unit"]
            )
            altqty_unit_mismatch = int((~altqty_units_match).sum())
            altqty_unit_converted = int(
                ((~altqty_units_match) & out["altQty_M_in_X_unit"].notna()).sum()
            )
        else:
            for c in ("altQty_X", "altQty_M", "altQty_M_in_X_unit",
                      "altQty_MISINVOICING"):
                if c not in out.columns:
                    out[c] = np.nan
            altqty_unit_mismatch = 0
            altqty_unit_converted = 0

        # Effective per-pair uplift rate — kept only as a sidecar
        # diagnostic (P5/P50/P95 below) since it was dropped from the
        # per-row output.
        with np.errstate(divide="ignore", invalid="ignore"):
            effective_uplift = np.where(
                out["primaryValue_X"] > 0,
                (out["primaryValue_X_CIF"] / out["primaryValue_X"]) - 1.0,
                np.nan,
            )

        out = out.reset_index()

        # hs_level derived from cmdCode length. HS6 = 5+ digits,
        # HS4 = 3-4, HS2 = 1-2.
        cmd_len = out["cmdCode"].astype(str).str.strip().str.len()
        out["hs_level"] = cmd_len.map(
            lambda L: "HS2" if L <= 2 else ("HS4" if L <= 4 else "HS6")
        )

        # Final column order — schema v5, 27 columns.
        # qty / altQty carry their own unit codes plus the converted
        # value (NaN when units don't match AND aren't dimensionally
        # convertible). netWgt / grossWgt are always kg so they need
        # no unit columns. primaryValue is always USD; primaryValue_X
        # is FOB, primaryValue_X_CIF is FOB after the per-row uplift,
        # primaryValue_M is CIF.
        ordered = [
            "matched_id", "exporterCode", "importerCode",
            "cmdCode", "hs_level",
            "qtyUnitCode_X", "qtyUnitCode_M",
            "qty_X", "qty_M", "qty_M_in_X_unit", "qty_MISINVOICING",
            "altQtyUnitCode_X", "altQtyUnitCode_M",
            "altQty_X", "altQty_M", "altQty_M_in_X_unit",
            "altQty_MISINVOICING",
            "netWgt_X", "netWgt_M", "netWgt_MISINVOICING",
            "grossWgt_X", "grossWgt_M", "grossWgt_MISINVOICING",
            "primaryValue_X", "primaryValue_X_CIF",
            "primaryValue_M", "primaryValue_MISINVOICING",
        ]
        ordered = [c for c in ordered if c in out.columns]
        out = out[ordered]

        out.to_csv(out_csv, index=False)

        # Per-mode diagnostics — count X rows by motCode + the share
        # that fell back to the default. Surface in the sidecar JSON
        # so the audit / reports can quote how the lookup table
        # actually behaved on this year's data.
        mot_counts = (
            x_rows["motCode"].astype(str).str.strip().value_counts(dropna=False)
            if "motCode" in x_rows.columns
            else pd.Series(dtype=int)
        )
        cif_table = _load_cif_fob_table()
        known_modes = set(cif_table.keys())
        unknown_rows = int(
            (~x_rows["motCode"].astype(str).str.strip().isin(known_modes)).sum()
            if "motCode" in x_rows.columns and known_modes
            else 0
        )
        uplift_diagnostics = {
            "x_rows_total": int(len(x_rows)),
            "x_rows_unknown_motcode": unknown_rows,
            "x_rows_by_motcode": {str(k): int(v) for k, v in mot_counts.items()},
            "cif_fob_default": float(CIF_FOB_DEFAULT),
            "cif_fob_table_loaded": bool(known_modes),
            "cif_fob_table_path": CIF_FOB_FACTOR_FILE,
        }
        eff_series = pd.Series(effective_uplift).replace(
            [np.inf, -np.inf], np.nan
        ).dropna()
        if not eff_series.empty:
            uplift_diagnostics["effective_uplift_rate_p05"] = float(
                eff_series.quantile(0.05)
            )
            uplift_diagnostics["effective_uplift_rate_p50"] = float(
                eff_series.quantile(0.50)
            )
            uplift_diagnostics["effective_uplift_rate_p95"] = float(
                eff_series.quantile(0.95)
            )

        # Entrepôt-pair and netWgt-imputation counts — kept in the
        # sidecar instead of as per-row columns.
        n_entrepot = int(entrepot_mask.sum())
        counts["pairs_entrepot"] = n_entrepot
        counts["pairs_entrepot_share"] = (
            float(n_entrepot) / float(len(out)) if len(out) else 0.0
        )

        n_imputed_either = int(netwgt_imputed_mask.sum())
        counts["pairs_netwgt_imputed_either_side"] = n_imputed_either
        counts["pairs_netwgt_imputed_share"] = (
            float(n_imputed_either) / float(len(out)) if len(out) else 0.0
        )

        # Pairs whose netWgt residual is NaN — downstream this means at
        # least one side had no usable netWgt rows.
        n_netwgt_nan = (
            int(out["netWgt_MISINVOICING"].isna().sum())
            if "netWgt_MISINVOICING" in out.columns else 0
        )
        counts["pairs_netwgt_mis_nan"] = n_netwgt_nan

        # qty / altQty unit mismatch + conversion stats (schema v5).
        # _mismatch  — pairs where exporter and importer reported different
        #              qtyUnitCode values
        # _converted — subset of _mismatch where the conversion table
        #              resolved the units and qty_MISINVOICING is NOT NaN.
        #              Pairs in (_mismatch - _converted) end up with
        #              residual = NaN exactly as in schema v4.
        counts["qty_unit_mismatch"]      = qty_unit_mismatch
        counts["qty_unit_converted"]     = qty_unit_converted
        counts["altQty_unit_mismatch"]   = altqty_unit_mismatch
        counts["altQty_unit_converted"]  = altqty_unit_converted

    # Per-attribute discrepancy counts among the kept pairs.
    # Vectorised set-equality test: two sets X and M are unequal iff
    # ``|X ∪ M| > min(|X|, |M|)`` (when both are non-empty). That single
    # inequality covers strict-subset and disjoint cases alike, with no
    # need for the per-group tuple comparison + ``apply(axis=1)`` row
    # scan the old code did.
    discrepancies = {}
    if not df_kept.empty:
        for col in DISCREPANCY_COLS:
            if col not in df_kept.columns:
                discrepancies[col] = None
                continue
            per_side = (
                df_kept.groupby(["matched_id", "_is_x"], sort=False, observed=True)[col]
                .nunique(dropna=True)
                .unstack("_is_x", fill_value=0)
            )
            x_nun = per_side.get(True, 0)
            m_nun = per_side.get(False, 0)
            total_nun = (
                df_kept.groupby("matched_id", sort=False, observed=True)[col]
                .nunique(dropna=True)
            )
            smaller_side = (
                x_nun.where(x_nun <= m_nun, m_nun) if hasattr(x_nun, "where") else x_nun
            )
            differs = (x_nun > 0) & (m_nun > 0) & (total_nun > smaller_side)
            discrepancies[col] = int(differs.sum())

    # Persist the JSON sidecar so the Reports phase can build the
    # Complementary xlsx without re-running misinvoicing. The .txt report
    # this used to write is gone — all phase auto-reports were dropped.
    # ``complete`` + ``matched_csv_mtime`` + ``matched_csv_size`` form
    # the freshness fingerprint the next run uses to skip rebuild.
    try:
        matched_mtime = int(os.path.getmtime(matched_csv))
        matched_size = os.path.getsize(matched_csv)
    except OSError:
        matched_mtime = matched_size = 0
    matched_hash = _quick_hash(matched_csv)
    payload = {
        "year": year_str,
        # Sidecar payload schema version (unrelated to the OUTPUT
        # CSV schema below — kept at 1 because the sidecar's own
        # field set hasn't changed in a breaking way).
        "schema_version": 1,
        # Output CSV schema version — what determines whether a
        # cached `misinvoicing/{year}.csv` can be reused or must be
        # regenerated. Bump OUTPUT_SCHEMA_VERSION at the top of
        # this module when you change the output column set.
        "output_schema_version": OUTPUT_SCHEMA_VERSION,
        "complete": True,
        # Legacy key kept so older readers don't crash; new readers
        # should use the per-mode lookup via cif_fob_factor.csv +
        # the diagnostics block below.
        "cif_fob_uplift": CIF_FOB_DEFAULT,
        "uplift_diagnostics": uplift_diagnostics,
        "counts": counts,
        "discrepancies": discrepancies,
        "matched_csv": matched_csv,
        "matched_csv_mtime": matched_mtime,
        "matched_csv_size": matched_size,
        # C5 — content-hash addition to the freshness fingerprint so
        # file-moves across hosts don't produce stale skips.
        "matched_csv_hash": matched_hash,
        "out_csv": out_csv,
    }
    _save_misinvoicing_stats(year_str, payload)

    _eff_p50 = uplift_diagnostics.get("effective_uplift_rate_p50")
    _eff_msg = (
        f"\n  Effective uplift P50: {_eff_p50*100:.2f}% (per-mode "
        f"weighted)"
        if _eff_p50 is not None
        else ""
    )
    _notify(
        f"{STATUS_OK} <b>MISINVOICING done — {year_str}</b>\n"
        f"  Pairs kept: {counts['kept']:,} / {counts['groups_total']:,}"
        f"{_eff_msg}\n"
        f"  CSV: <code>{_h(out_csv) if out_csv else '(no surviving pairs)'}</code>\n"
        f"  Stats: <code>{_h(_stats_sidecar_path(year_str))}</code>",
        notify,
    )

    return out_csv, _stats_sidecar_path(year_str)
