# Helper scripts

## `build_hs_concordance.py`

Builds `app/data/reference/hs_concordance.csv` from one or more UNSD
HS correspondence tables placed under `hs_concordance_src/`. The
generated CSV is what `app/pipeline/match.py` uses to map every HS6
code to a single canonical revision before matching, so an exporter
row reported under HS 2002 never gets paired with an importer row
reported under HS 2017.

A small concordance ships under `app/data/reference/`. Re-run the
script after dropping new UNSD tables into `hs_concordance_src/` to
regenerate it. See `hs_concordance_src/README.md` for the expected
file-name convention and download links.

```bash
python scripts/build_hs_concordance.py        # quiet
python scripts/build_hs_concordance.py -v     # verbose
```

The script writes atomically and leaves the existing concordance
untouched if every source file fails to parse.
