"""
Single source of truth for everything the pipeline needs to know
before it starts. Edit this file to change what runs — there is no
interactive menu and no Telegram bot. ``python -m app.main`` reads the
values defined here and executes the requested phases for the
requested years against the requested scope.

Sections
--------
1. API credentials       — Comtrade subscription key (env var).
2. Directories           — where raw/aggregate/match/clean/misinvoicing
                           outputs live on disk.
3. Run plan              — YEARS, PHASES, REPORTER_SCOPE filters.
4. Scope filters         — HS_LEVEL, COMMODITY_FILTER, FLOW filters.
5. Columns / drop rules  — which Comtrade fields are kept; which
                           filters fire in the aggregate phase.
6. Matching output       — column-drop list applied post-merge.
7. Misinvoicing knobs    — CIF/FOB uplift table, qty-unit conversion
                           table, entrepôt reporter list.

Run plan options (section 3) drive the orchestrator in app/main.py.
"""

import os


# ==========================================
# 1. API CREDENTIALS
# ==========================================
# Read from the environment so the key never ends up in git.
#
#     export COMTRADE_API_KEY=...
#
# A free key is fine for small year ranges; very large pulls benefit
# from a Comtrade premium subscription (higher rate limit).
COMTRADE_API_KEY = os.environ.get("COMTRADE_API_KEY", "")

if not COMTRADE_API_KEY:
    from app.core.logger import get_logger
    get_logger(__name__).warning(
        "COMTRADE_API_KEY not set — Comtrade downloads will fail."
    )


# ==========================================
# 2. DIRECTORIES
# ==========================================
# Static reference data lives inside the package (app/data/) so we
# resolve it relative to this file rather than the working directory.
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Runtime-generated dirs live under ``COMTRADE_DATA_ROOT``. The default
# (".") keeps everything next to where you run the command. Set
# COMTRADE_DATA_ROOT=/path/to/scratch to write the (potentially large)
# outputs elsewhere.
DATA_ROOT        = os.environ.get("COMTRADE_DATA_ROOT", ".")
RAW_DIR          = os.path.join(DATA_ROOT, "raw")           # .gz cache (raw/{year}/)
AGGREGATE_DIR    = os.path.join(DATA_ROOT, "aggregate")     # one pre-filtered csv per year
MATCH_DIR        = os.path.join(DATA_ROOT, "match")         # matched.csv + unmatched.csv per year
CLEAN_DIR        = os.path.join(DATA_ROOT, "clean")         # post-strict-filter clean.csv per year
MISINVOICING_DIR = os.path.join(DATA_ROOT, "misinvoicing")  # per-pair residuals


def raw_year_dir(year):
    """raw/{year}/ — one subfolder per year. Created on demand by retrieval."""
    return os.path.join(RAW_DIR, str(year))


REPORTER_CODES_FILE = os.path.join(DATA_DIR, "reporter_codes.csv")

# HS revision concordance — Comtrade's ``classificationCode`` carries
# one of H0..H6 (HS 1992 .. HS 2022). Codes change meaning across
# revisions, so we map every HS6 code to a single canonical revision
# before the matcher key is built. Codes missing from the table fall
# back to identity mapping.
CANONICAL_HS_REVISION = "H4"  # HS 2012 — the safest middle ground.
HS_CONCORDANCE_FILE   = os.path.join(DATA_DIR, "reference", "hs_concordance.csv")

# Entrepôt reporter list — used by misinvoicing to flag bilateral
# residuals where either side is an entrepôt (Hong Kong, Singapore,
# Netherlands, Belgium, UAE, Panama, Switzerland, etc.).
ENTREPOT_REPORTERS_FILE = os.path.join(DATA_DIR, "reference", "entrepot_reporters.csv")

NETWORK_TIMEOUT = 10
SLEEP_TIMEOUT   = 0.1


# ==========================================
# 3. RUN PLAN
# ==========================================
# Years to process. Comtrade's earliest year for most reporters is
# 1962; current year usually lags 1-2 years.
YEARS = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]

# Which phases to run, in order. Pick any contiguous slice of:
#   "retrieval"     — download .gz files into raw/{year}/
#   "aggregate"     — apply row filters, build aggregate/{year}.csv
#   "match"         — pair X / M rows, write match/{year}_*.csv and clean/{year}.csv
#   "misinvoicing"  — compute per-pair residuals into misinvoicing/{year}.csv
#
# Running just one phase is fine — each phase reads the output of the
# previous one off disk, so re-running is cheap once prior phases are done.
PHASES = ["retrieval", "aggregate", "match", "misinvoicing"]

# Reporter scope:
#   None             → all reporters in app/data/reporter_codes.csv (default).
#   [4, 156, 826]    → only these reporter codes (numeric M49 codes).
#
# REPORTER_SCOPE applies uniformly to every phase. Restrict it to a
# handful of countries while debugging — it cuts retrieval and
# aggregate time roughly in proportion to the list length.
REPORTER_SCOPE = None


# ==========================================
# 4. SCOPE FILTERS
# ==========================================
# HS-level filter applied during AGGREGATE. Rows whose cmdCode does
# not match the requested level are dropped before they ever reach the
# matcher.
#   "HS2"  — keep only 2-digit chapter codes (~96 categories)
#   "HS4"  — keep only 4-digit headings (~1,200 categories)
#   "HS6"  — keep only 6-digit sub-headings (~5,300 categories, finest grain)
#   "ALL"  — keep every level (HS2 + HS4 + HS6) — Comtrade's native mix
#
# HS6 is the standard choice for academic misinvoicing studies; HS2 /
# HS4 are faster and produce smaller files at the cost of resolution.
HS_LEVEL = "HS6"

# Commodity filter — restrict to a specific set of HS prefixes. Empty
# tuple = no restriction. Prefixes match the start of cmdCode after
# zero-padding to the HS_LEVEL granularity, e.g.:
#   ("27",)          → all energy products (HS chapter 27)
#   ("27", "26")     → energy + ores
#   ("2709",)        → only crude petroleum (HS 2709)
#   ("271012",)      → only motor spirit (HS6 sub-heading)
COMMODITY_FILTER = ()

# Trade-flow filters. Comtrade ships many flow codes; we keep only
# direct imports / exports. Re-imports (RM), re-exports (RX), and
# domestic flows (DM, DX, numeric codes, blanks) are dropped.
IMPORT_FLOWS = {"M"}
EXPORT_FLOWS = {"X"}
VALID_FLOWS  = {"M", "X"}
MATCH_FLOWS  = {"M", "X"}


# ==========================================
# 5. COLUMNS / DROP RULES
# ==========================================
# Columns kept in the per-year aggregate CSV and propagated through
# matching. Names match Comtrade's bulk-download headers (case-
# insensitive read).
KEEP_COLUMNS = [
    "reporterCode",
    "flowCode",
    "partnerCode",
    "partner2Code",
    "classificationCode",
    "isOriginalClassification",
    "cmdCode",
    "customsCode",
    "mosCode",
    "motCode",
    "qtyUnitCode",
    "qty",
    "isQtyEstimated",
    "altQtyUnitCode",
    "altQty",
    "isAltQtyEstimated",
    "netWgt",
    "isNetWgtEstimated",
    "grossWgt",
    "isGrossWgtEstimated",
    "CIFValue",
    "FOBValue",
    "primaryValue",
    "legacyEstimationFlag",
    "isReported",
    "isAggregate",
]

# Columns read for filtering but NOT written to the output CSV.
FILTER_COLUMNS = [
    "typeCode",
    "freqCode",
    "refYear",
]

# Only the full-file "TOTAL" rollup is dropped at this stage.
# Numeric chapter codes like 99 / 9999 / 999999 are valid HS levels.
AGGREGATE_CMD_CODES = {"TOTAL"}


# ==========================================
# 6. MATCHING OUTPUT
# ==========================================
# Per year, matching writes:
#   match/{year}_matched.csv     — rows with matched_id populated
#   match/{year}_unmatched.csv   — rows where matched_id is null
#   match/{year}_match_stats.json — sidecar with per-country MATCH stats
#   clean/{year}.csv             — post-strict-filter view (see README)
#
# Columns dropped after merging raw .gz files (they are bookkeeping
# fields Comtrade ships but the matcher does not need).
DROP_AFTER_MERGE = [
    "datasetCode",
    "typeCode",
    "freqCode",
    "refPeriodId",
    "refYear",
    "refMonth",
    "period",
    "classificationSearchCode",
]


# ==========================================
# 7. MISINVOICING
# ==========================================
# Per-pair CIF/FOB uplift. Comtrade exports are reported FOB (Free on
# Board — excludes insurance and freight); imports are CIF (Cost,
# Insurance, Freight). To compare like-for-like we mark up the
# exporter's FOB value by a per-motCode factor. Defaults to 0.10 (the
# legacy IMF flat uplift) when the row's motCode is missing or absent
# from the lookup table.
CIF_FOB_DEFAULT      = 0.10
CIF_FOB_FACTOR_FILE  = os.path.join(DATA_DIR, "reference", "cif_fob_factor.csv")

# Per-qtyUnitCode dimensional conversion table — used by misinvoicing
# to compute qty / altQty residuals when exporter and importer report
# in different units of the SAME physical dimension (kg vs g, l vs m³).
QTY_UNIT_CONVERSION_FILE = os.path.join(
    DATA_DIR, "reference", "qty_unit_conversion.csv"
)

