# UNSD HS correspondence-table inputs

Drop the United Nations Statistics Division pairwise HS correspondence
tables here. The sibling script `../build_hs_concordance.py` reads
every CSV / XLSX in this folder, auto-detects each file's
revision-pair from its filename, and builds the canonical
`app/data/reference/hs_concordance.csv` used by the matcher.

## Where to download

The UNSD's HS correspondence-tables page is the authoritative source:

  <https://unstats.un.org/unsd/trade/classifications/correspondence-tables.asp>

It links to one Excel / CSV file per adjacent revision pair (HS 1992
→ HS 1996, HS 1996 → HS 2002, etc.). Download whichever pairs cover
the revisions that appear in your Comtrade dataset. The script chains
them — if you have H2→H3, H3→H4, and H4→H5 it composes the H2→H5
transitions for you.

## Filename convention

The script parses revision-pair from the filename. Use one of:

```
HS1992_HS1996.xlsx
HS1996_HS2002.xlsx
HS2002_HS2007.xlsx
HS2007_HS2012.xlsx
HS2012_HS2017.xlsx
HS2017_HS2022.xlsx
```

Hyphen instead of underscore is also fine (`HS2007-HS2012.csv`).
Files whose name doesn't match the pattern are skipped with a warning.

## Column conventions

The script understands the common UNSD column layouts:
- `From` / `To` (+ optional `Share` / `Weight` / `Ratio`)
- `HSyyyy` / `HSzzzz` columns (the older WCO export style)
- `Old code` / `New code` (the WCO HS-tracker style)

Auto-detection is case-insensitive. When no share column is present,
the script assumes equal shares for one-to-many splits and rescales
so the per-`from_code` weights sum to 1.0.

## Running

From the repo root:

```bash
python scripts/build_hs_concordance.py
# or, with verbose logging:
python scripts/build_hs_concordance.py -v
```

The result is written atomically to
`app/data/reference/hs_concordance.csv`. If parsing fails on any file
the script logs a warning and continues with the rest; if no files
parse the existing concordance is left untouched.

## What ends up committed

This folder is intentionally **not** checked into git — the UNSD /
WCO files are redistributed under their own terms and shouldn't be
bundled with the codebase. Only the *output* (`hs_concordance.csv`)
is committed.

To make this folder reappear after `git clone`, just create it
manually before you download the UNSD CSVs.
