# UN Comtrade mirror-statistics pipeline

A reproducible four-phase pipeline that downloads UN Comtrade bulk
files, applies a documented chain of row filters, pairs each
exporter declaration with its mirror importer declaration, and emits
per-pair misinvoicing residuals (the gap between what country A says
it shipped and what country B says it received).

Everything is controlled from `app/config.py` and run from the
terminal with:

```bash
python -m app.main
```

No interactive prompts, no chat bots, no menus. Edit the config, run
the command, watch the logs.

---

## What the pipeline does

The pipeline runs four phases per year, in order:

1. **Retrieval** — downloads one gzip-compressed CSV per reporter
   country into `raw/{year}/`. Existing files are detected and reused
   so re-runs are cheap.
2. **Aggregate** — reads every `.gz`, applies the row-level drop
   filters described below, and concatenates the survivors into a
   single `aggregate/{year}.csv`.
3. **Match** — pairs every export declaration (`flowCode = X`) with
   its mirror import declaration (`flowCode = M`) using the
   bilateral / triangular keys documented in
   `app/pipeline/match.py`. Produces `match/{year}_matched.csv`,
   `match/{year}_unmatched.csv`, and a strict-filter view at
   `clean/{year}.csv`.
4. **Misinvoicing** — for each matched pair, sums the metrics on
   each side (qty, netWgt, primaryValue) after a per-motCode CIF/FOB
   uplift on the export side, then differences exporter − importer.
   Output: `misinvoicing/{year}.csv`.

Phases are interleaved per year — year *N* finishes all requested
phases before year *N+1* starts — so peak disk usage stays bounded
to one year of intermediates rather than the whole range.

---

## Configuration

Every per-run decision lives in `app/config.py`. The important
sections are:

| Constant | What it does |
|----------|--------------|
| `YEARS` | List of year strings to process. No hard-coded range. |
| `PHASES` | Which phases to run, in order. Use a contiguous slice (e.g. `["match", "misinvoicing"]` to skip retrieval and aggregate). |
| `REPORTER_SCOPE` | `None` = every reporter in `app/data/reporter_codes.csv`. Or a list of numeric M49 codes to keep (e.g. `[4, 156, 826]`). |
| `HS_LEVEL` | `"HS2"`, `"HS4"`, `"HS6"`, or `"ALL"`. Applied during aggregate. |
| `COMMODITY_FILTER` | Tuple of HS prefixes to keep. `()` = no restriction. e.g. `("27",)` for energy products, `("2709",)` for crude petroleum. |
| `CANONICAL_HS_REVISION` | The HS revision every cmdCode is mapped to before matching, to avoid silent cross-revision noise. Defaults to `"H4"` (HS 2012). |
| `CIF_FOB_DEFAULT` | Flat CIF/FOB uplift used when a row's motCode is missing from `app/data/reference/cif_fob_factor.csv`. |
| `COMTRADE_DATA_ROOT` (env var) | Where the runtime output directories live. Defaults to the current directory. |

The full file is heavily commented — read it before changing
anything.

### Environment

Only one secret is required:

```bash
export COMTRADE_API_KEY="your-comtrade-subscription-key"
```

A free Comtrade API key works fine for small year ranges; a premium
key raises the per-second rate limit.

`.env.example` shows the exact set of variables. Copy it to `.env`
and source it (or paste the exports into your shell rc).

---

## Install and run

Tested on Python 3.11+. From a clean clone:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export COMTRADE_API_KEY="..."
python -m app.main
```

Ctrl-C is honoured: the next safe checkpoint will unwind the run
without corrupting any partially-written CSV (every output uses a
`.tmp` + `os.replace` atomic write).

To resume after an interruption, just run the command again — every
phase is idempotent and skips work that's already on disk.

---

## Technical specs

Hardware floor (a single year of all reporters at HS6):

- **CPU**: 4 cores minimum, 8+ recommended. The aggregate phase
  fans out across `os.cpu_count() - 1` workers; override with
  `COMTRADE_WORKERS=N`.
- **RAM**: 16 GB minimum. Each aggregate worker peaks at ~200–400 MB
  with PyArrow-backed reads; the matcher loads the year aggregate
  into memory once.
- **Disk**: budget ~150 GB free for a single recent year with every
  reporter at HS6. Modern years (post-2018) are larger than older
  ones. Raw `.gz` files compress aggressively; intermediates are
  CSV and can be huge before they get gzipped by `shrink_year`.
- **Network**: stable connection; retrieval makes thousands of API
  calls per year and the per-call rate is governed by your Comtrade
  subscription tier.

Software:

- Python 3.11+
- `requirements.txt` pins exact versions (pandas 3.0, pyarrow 21,
  duckdb 1.5, comtradeapicall 1.3).
- `pyarrow` is optional but strongly recommended — it cuts memory
  pressure roughly 3–4× compared to NumPy-backed reads.
- `duckdb` is required for the clean-matched step (the SQL pick of
  the dominant `motCode='0'` row per side).

### How long does one year take?

Rough wall-clock estimates on an 8-core machine with a stable 50
Mbit/s connection, processing **all reporters** at **HS6**:

| Phase | Single year | Notes |
|-------|-------------|-------|
| Retrieval | 30–90 min | Mostly bounded by Comtrade's rate limit. A cold pull of a single year of all ~1,500 reporters fits in this window with a free key. Re-runs hitting the cache finish in seconds. |
| Aggregate | 20–60 min | CPU-bound, parallel across reporters. Modern years (more rows per reporter) take longer than older ones. |
| Match | 30–90 min | Single-process. Memory is the bottleneck — the matcher builds in-memory indices keyed by `(reporter, partner, cmdCode)`. |
| Misinvoicing | 10–30 min | Streams the matched CSV; modest CPU and RAM. |
| **End-to-end** | **~2–5 hours per year** | Plus 5–10 min of overhead. |

Narrowing the scope (smaller `REPORTER_SCOPE`, coarser `HS_LEVEL`,
or a non-empty `COMMODITY_FILTER`) cuts these numbers roughly
linearly with the data volume removed.

---

## How dropping and cleaning produces the final results

This is the question that matters most for interpreting the
misinvoicing CSV — every residual reflects the rows that survived a
documented chain of filters, and a row dropped at any stage doesn't
come back.

### Stage 1 — Row drops during aggregate

Every `.gz` is streamed in 500,000-row chunks. For each chunk we
build a per-reason boolean mask, count the rows that flip true the
first time (so a row eliminated by an earlier rule isn't double-
counted), tally by HS bucket, then keep only the rows for which all
masks are false. The drop reasons, in order:

1. **`rej_typeCode`** — `typeCode ≠ "C"`. Comtrade ships
   service-trade rows with `typeCode = "S"`; we only want
   merchandise (`C`).
2. **`rej_freqCode`** — `freqCode ≠ "A"`. Drops monthly /
   sub-annual rows that would contaminate an annual aggregate.
3. **`rej_refYear`** — `refYear ≠` the year currently being
   processed. Comtrade occasionally bundles cross-year rows in a
   year file.
4. **`rej_classificationSearchCode`** — `classificationSearchCode ≠ "HS"`.
   Drops SITC / BEC / other non-HS classifications.
5. **`rej_partner_zero`** — `partnerCode == 0 AND partner2Code == 0`.
   These are "World" aggregates, not a real bilateral row.
6. **`rej_cmdCode`** — `cmdCode` is null, non-numeric, or zero. No
   usable HS code.
7. **`rej_hs_level`** — `cmdCode` length doesn't match
   `config.HS_LEVEL` (`HS2`/`HS4`/`HS6`). Skipped when `HS_LEVEL =
   "ALL"`.
8. **`rej_commodity_filter`** — `cmdCode` doesn't start with any
   prefix in `config.COMMODITY_FILTER`. Skipped when the tuple is
   empty.
9. **`rej_reporterCode`** — null / zero reporter.
10. **`rej_flowCode`** — flow is not in `{M, X}`. Drops re-exports
    (RX), re-imports (RM), domestic (DM/DX), numeric flow codes,
    blanks.
11. **`rej_self_trade`** — `reporterCode == partnerCode`. A country
    cannot import from itself; if it appears it's a Comtrade
    bookkeeping artefact.
12. **`rej_customsCode_not_c00`** — only the "all customs procedures"
    total (`C00`) is kept. Component codes (`C01`, `C02`, …) are
    breakdowns of the same trade flow; keeping them would
    double-count against the `C00` aggregate.
13. **`rej_all_estimated`** — `isQtyEstimated AND isAltQtyEstimated
    AND isNetWgtEstimated AND isGrossWgtEstimated`. The row has no
    real measurement on any dimension — Comtrade imputed every one.
14. **`rej_exact_duplicate`** — within the chunk, identical rows on
    every kept column. Always keep the first.

Per-reporter survivors are written to
`aggregate/_partials/{year}/{reporter}.csv` and the year file is
rebuilt by concatenating partials, so a crash mid-aggregate resumes
from the next not-yet-committed reporter.

### Stage 2 — Matching

The matcher loads the year aggregate, normalises `cmdCode` to
`CANONICAL_HS_REVISION` using the concordance table, and assigns a
`matched_id` to each (X row, mirror M row) pair using two keys:

- **Direct bilateral pair** — both rows have `partner2 = 0`. Match
  on `M.reporter = X.partner ∧ M.partner = X.reporter ∧ M.cmdCode =
  X.cmdCode`.
- **Triangular pair** — both rows declare a `partner2`. Match on
  `M.reporter = X.partner2 ∧ M.partner = X.reporter ∧ M.partner2 =
  X.partner ∧ M.cmdCode = X.cmdCode`.

Anything that can't be paired stays in `match/{year}_unmatched.csv`
— useful for diagnostics; not used downstream.

### Stage 3 — `clean_matched_csv` (strict-filter view)

Before the residuals are computed, the matched CSV is filtered down
to a single canonical row per side per pair via DuckDB:

1. For each `(matched_id, flowCode)` we pick `MIN(id)` among rows
   where `motCode = '0'`. `motCode = '0'` is Comtrade's "all modes of
   transport" rollup; component codes (sea, air, road, …) sum to it,
   and using both would double-count.
2. A pair is kept only if such a canonical row exists on **both**
   the X side and the M side. Pairs missing the rollup on one side
   are dropped (recorded in the stats sidecar under `pairs_case_b`).

The survivors land in `clean/{year}.csv`. That file is what an
analyst should read when they want to verify a residual by hand —
one row per side per pair, no further dedup needed.

### Stage 4 — Misinvoicing residuals

For each surviving pair, the misinvoicing phase sums the metrics
within each side (so multi-row pairs still work) and computes
exporter − importer for the following columns:

- `qty` (only when both sides report the same `qtyUnitCode` — unit
  mismatches are counted but not differenced).
- `netWgt`.
- `primaryValue`, after each exporter row is multiplied by
  `(1 + uplift_factor)` looked up from `motCode` in
  `cif_fob_factor.csv`. Sea freight is ~5 %, air freight ~20 %,
  pipelines ~2 %; rows with a missing or unknown motCode fall back
  to `CIF_FOB_DEFAULT` (default 0.10, the legacy IMF flat value).

The output CSV exposes the effective per-pair uplift rate
(`effective_uplift_rate`) so a reader can verify the lookup table
behaved as expected.

Pairs where either side is on the entrepôt list
(`app/data/reference/entrepot_reporters.csv` — Hong Kong, Singapore,
Netherlands, Belgium, UAE, Panama, Switzerland, …) carry an
`is_entrepot_pair` column so reports can be split. Entrepôt
residuals are systematically distorted by re-export flows the
exporter side doesn't see, so they're worth treating separately.

Positive residual → exporter reported MORE (over-invoiced exports
/ under-invoiced imports). Negative → the reverse.

---

## Output layout

After a full run with `COMTRADE_DATA_ROOT="."`:

```
.
├── raw/{year}/                        # cached Comtrade .gz files
├── aggregate/
│   ├── {year}.csv                     # post-drop survivors
│   ├── {year}_stats.json              # per-reporter drop counters
│   ├── {year}_progress.json           # resume state
│   └── _partials/{year}/              # per-reporter intermediates
├── match/
│   ├── {year}_matched.csv             # rows with matched_id
│   ├── {year}_unmatched.csv           # rows the matcher rejected
│   ├── {year}_match_stats.json        # per-country MATCH stats
│   ├── {year}_match_sample.csv        # 50-pair random preview
│   └── {year}_sample_ids.json         # ids in the preview (used by misinvoicing sampler)
├── clean/
│   └── {year}.csv                     # strict-filter pair view
├── misinvoicing/
│   ├── {year}.csv                     # per-pair residuals
│   ├── {year}_stats.json              # phase sidecar
│   └── {year}_misinvoicing_sample.csv # preview aligned with match sample
└── logs/
    └── pipeline.log                   # rotating run log
```

Every sidecar JSON is the canonical source for that phase's
diagnostics — they're written atomically and survive across runs.

---

## Repository layout

```
app/
├── config.py                  # all knobs live here
├── main.py                    # terminal entry point
├── core/
│   ├── db.py                  # reporter / HS lookup, concordance
│   ├── gzcache.py             # .gz cache index
│   ├── logger.py              # rotating logger
│   ├── notifier.py            # terminal-only status helpers
│   └── progress.py            # crash-resume sidecars
├── data/                      # reference CSVs (reporter codes, HS, concordance, etc.)
├── pipeline/
│   ├── retrieval.py           # phase 1
│   ├── extract.py             # phase 2 (aggregate)
│   ├── match.py               # phase 3
│   └── misinvoicing.py        # phase 4
└── tests/
```

---

## License

See `LICENSE`.
