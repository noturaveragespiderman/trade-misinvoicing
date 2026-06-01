"""
Build app/data/reference/hs_concordance.csv from UNSD correspondence tables.

Background
----------
The codebase ships with a hand-written stub at
``app/data/reference/hs_concordance.csv`` containing ~20 well-known
HS-revision splits (electronic ICs in 2007, electric vehicles in
2017, etc.). It loads correctly but covers only a tiny fraction of
the real cross-revision drift the WCO has introduced since 1992.
This script replaces the stub with a comprehensive table built from
the United Nations Statistics Division's published correspondence
tables (https://unstats.un.org/unsd/trade/classifications/correspondence-tables.asp).

What this script does
---------------------
1. Reads UNSD-format correspondence files from an input directory
   (default: ``./hs_concordance_src/``). Files can be CSV or XLSX.
   The expected filename pattern is ``HSxxxx_HSyyyy.{csv,xlsx}`` or
   ``HSxxxx-HSyyyy.{csv,xlsx}`` where xxxx / yyyy are the four-digit
   HS vintages (e.g. ``HS2007_HS2012.csv``).

2. Auto-detects each file's column layout. UNSD has used several
   schemas over the years; the script understands the common ones:
     - "From"/"To" with "Relationship" + share columns
     - "HSxxxx"/"HSyyyy" code columns + numeric share / weight
     - the WCO HS-tracker exports ("Old code"/"New code"/"Type")

3. For each adjacent-revision pair found, builds an in-memory graph
   ``(from_revision, from_code) -> [(to_code, weight)]`` then
   transitively composes paths to the canonical revision so that
   e.g. H0 → H4 is computed as H0 → H1 → H2 → H3 → H4 with weights
   multiplied along the path. Codes that have no path to the target
   are emitted as identity rows.

4. Writes the merged result to ``app/data/reference/hs_concordance.csv``
   in the schema the codebase expects:
       from_revision, from_code, to_revision, to_code, weight, note

   ``to_revision`` is always the canonical revision (default ``H4``).
   ``weight`` is the share of trade volume that flows from
   ``from_code`` to ``to_code`` along the composed path. For
   one-to-one mappings ``weight = 1.0``; for splits, ``Σ weight = 1``
   over all output codes (within rounding).

Usage
-----
1. Download the UNSD correspondence tables — there are several
   pairwise XLSX/CSV files on the page linked above. Put them under
   ``./hs_concordance_src/`` next to this script:

       hs_concordance_src/
         HS1992_HS1996.xlsx
         HS1996_HS2002.xlsx
         HS2002_HS2007.xlsx
         HS2007_HS2012.xlsx
         HS2012_HS2017.xlsx
         HS2017_HS2022.xlsx

   Adjacent pairs are enough — the script chains them. Missing pairs
   degrade gracefully (codes in those revisions become identity rows).

2. Run from the repo root:

       python scripts/build_hs_concordance.py

   Pass ``--src DIR`` to point at a different source folder,
   ``--target H4`` to choose a different canonical revision, or
   ``--output PATH`` to write somewhere other than the default.

3. Inspect the resulting CSV (it'll have ~20k–30k rows once
   adjacent pairs are loaded), commit it, and the matcher will pick
   it up on the next run.

Behaviour when inputs are missing
---------------------------------
- No source directory / no files → the script prints a clear
  message + exits with code 2, leaving the existing
  hs_concordance.csv untouched.
- Some pairs missing → the script logs which transitions could not
  be composed and writes identity rows for codes in those
  revisions, so the matcher's identity-fallback continues to work.

This script never deletes the stub silently. It writes to a
``.tmp`` file first and atomically replaces only on success.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

LOG = logging.getLogger("build_hs_concordance")


# ---------------------------------------------------------------------------
# Revision name conventions
# ---------------------------------------------------------------------------
# UNSD names vintages by year ("HS 2007"); the codebase uses Comtrade's
# classificationCode short-form ("H3"). Map between the two.
_VINTAGE_TO_REV: Dict[str, str] = {
    "1992": "H0",
    "1996": "H1",
    "2002": "H2",
    "2007": "H3",
    "2012": "H4",
    "2017": "H5",
    "2022": "H6",
}

# Order of revisions for chain composition. Adjacent pairs are
# enough — the script walks this list to compose any A → B.
_REV_ORDER: List[str] = ["H0", "H1", "H2", "H3", "H4", "H5", "H6"]


def _vintage_of(year_str: str) -> Optional[str]:
    """Convert a 4-digit HS vintage string to a Comtrade H-code,
    or return None if it isn't a known vintage."""
    return _VINTAGE_TO_REV.get(year_str.strip())


# ---------------------------------------------------------------------------
# Filename parser
# ---------------------------------------------------------------------------
# Accepts: HS2007_HS2012.csv, HS2007-HS2012.xlsx, hs07_hs12.csv (the last
# is a WCO export convention with 2-digit years). Returns (from_rev,
# to_rev) or (None, None) if the name isn't recognised.
def _parse_pair_from_name(stem: str) -> Tuple[Optional[str], Optional[str]]:
    import re

    s = stem.lower().replace("-", "_").replace(" ", "_")
    # Four-digit pattern first: "hs2007_hs2012"
    m = re.search(r"hs(\d{4})_hs(\d{4})", s)
    if m:
        return _vintage_of(m.group(1)), _vintage_of(m.group(2))
    # Two-digit pattern: "hs07_hs12" — assume 19xx for >=92, 20xx else
    m = re.search(r"hs(\d{2})_hs(\d{2})", s)
    if m:
        def _yyyy(yy: str) -> str:
            n = int(yy)
            return f"19{yy}" if n >= 92 else f"20{yy}"
        return _vintage_of(_yyyy(m.group(1))), _vintage_of(_yyyy(m.group(2)))
    return None, None


# ---------------------------------------------------------------------------
# Column auto-detection
# ---------------------------------------------------------------------------
# Different UNSD vintages use different column names. This finder picks
# the first match from a list of plausible aliases (case-insensitive,
# substring match).
def _pick_column(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    lower_to_orig = {c.lower(): c for c in df.columns}
    # Exact match first
    for cand in candidates:
        if cand.lower() in lower_to_orig:
            return lower_to_orig[cand.lower()]
    # Substring fallback
    for cand in candidates:
        for low, orig in lower_to_orig.items():
            if cand.lower() in low:
                return orig
    return None


def _load_pair_file(path: Path, from_rev: str, to_rev: str
                    ) -> List[Tuple[str, str, float]]:
    """
    Parse a single UNSD correspondence file and return a list of
    ``(from_code, to_code, weight)`` tuples.

    Auto-detects:
      - the file format (CSV vs XLSX)
      - which column is the "from" code, which is the "to" code, and
        which (if any) carries the share / weight
    """
    suffix = path.suffix.lower()
    if suffix in (".xlsx", ".xls"):
        df = pd.read_excel(path, dtype=str)
    elif suffix in (".csv", ".tsv", ".txt"):
        # pandas autodetection requires the Python engine; default for
        # plain .csv is comma, but UNSD occasionally ships tab- or
        # semicolon-separated files under a .csv name.
        if suffix == ".tsv":
            df = pd.read_csv(path, dtype=str, sep="\t")
        else:
            df = pd.read_csv(path, dtype=str, sep=None, engine="python")
    else:
        LOG.warning("Unsupported file extension: %s — skipping", path)
        return []

    if df.empty:
        return []

    df.columns = [c.strip() for c in df.columns]

    # Column aliases UNSD has used. Order matters: most specific first.
    from_aliases = [
        f"HS{from_rev[1:]}{_REV_TO_VINTAGE[from_rev][2:]}",  # "HS07" style
        f"HS {from_rev[1:]}",
        from_rev,
        "from",
        "old code",
        "old hs code",
        "source",
    ]
    to_aliases = [
        f"HS{to_rev[1:]}{_REV_TO_VINTAGE[to_rev][2:]}",
        f"HS {to_rev[1:]}",
        to_rev,
        "to",
        "new code",
        "new hs code",
        "target",
    ]
    # Possible share / weight column names.
    share_aliases = [
        "share", "weight", "ratio", "fraction", "split",
        f"share_{to_rev.lower()}", f"weight_{to_rev.lower()}",
    ]

    from_col = _pick_column(df, from_aliases)
    to_col = _pick_column(df, to_aliases)
    if from_col is None or to_col is None:
        LOG.warning(
            "Could not identify from/to columns in %s — columns are %r. Skipping.",
            path,
            list(df.columns),
        )
        return []
    share_col = _pick_column(df, share_aliases)

    out: List[Tuple[str, str, float]] = []
    for _, row in df.iterrows():
        a = str(row[from_col]).strip()
        b = str(row[to_col]).strip()
        if not a or not b or a.lower() == "nan" or b.lower() == "nan":
            continue
        if share_col is not None:
            try:
                w = float(row[share_col])
            except (TypeError, ValueError):
                w = float("nan")
            if pd.isna(w):
                w = 1.0  # missing share → assume 1.0; will be rescaled below.
        else:
            w = 1.0
        out.append((a, b, w))

    # Normalise per from_code so the weights of all to_codes sum to 1.0.
    # UNSD files for one-to-many splits don't always include shares,
    # but downstream code needs them.
    by_from: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
    for a, b, w in out:
        by_from[a].append((b, w))
    normalised: List[Tuple[str, str, float]] = []
    for a, bws in by_from.items():
        total = sum(w for _, w in bws) or 1.0
        for b, w in bws:
            normalised.append((a, b, w / total))
    return normalised


# Reverse the vintage map so the column-alias guesser can build
# "HS07" style names.
_REV_TO_VINTAGE: Dict[str, str] = {v: k for k, v in _VINTAGE_TO_REV.items()}


# ---------------------------------------------------------------------------
# Graph composition: chain adjacent maps into A → CANONICAL paths
# ---------------------------------------------------------------------------
def _compose(
    adjacent: Dict[Tuple[str, str], List[Tuple[str, str, float]]],
    target_rev: str,
) -> List[Tuple[str, str, str, float]]:
    """
    Walk each ``(from_rev, from_code)`` to ``target_rev`` by chaining
    adjacent-pair maps. Returns ``[(from_rev, from_code, to_code, weight)]``.
    Weights along a chain are multiplied; multi-path destinations are
    summed.
    """
    # Build a lookup: (rev, code) -> [(next_rev, next_code, weight)]
    step: Dict[Tuple[str, str], List[Tuple[str, str, float]]] = defaultdict(list)
    for (from_rev, to_rev), pairs in adjacent.items():
        for a, b, w in pairs:
            step[(from_rev, a)].append((to_rev, b, w))

    out: List[Tuple[str, str, str, float]] = []
    # For each starting revision, walk forward to target_rev.
    seen_starts = {start for (start, _) in step.keys()}
    seen_starts.add(target_rev)
    target_idx = _REV_ORDER.index(target_rev) if target_rev in _REV_ORDER else None
    if target_idx is None:
        raise ValueError(f"target_rev {target_rev!r} not in known revisions")

    for start_rev in sorted(seen_starts, key=lambda r: _REV_ORDER.index(r)):
        # Codes in start_rev that have any outgoing step.
        codes = sorted({c for (r, c) in step.keys() if r == start_rev})
        # Also include codes that appear as a "to" of an earlier-rev step:
        # we want to be able to start the walk from each code in start_rev
        # regardless of how we learned it exists. Build a fuller list by
        # scanning all known codes per revision.
        all_codes_per_rev: Dict[str, set] = defaultdict(set)
        for (r, c), targets in step.items():
            all_codes_per_rev[r].add(c)
            for (r2, c2, _w) in targets:
                all_codes_per_rev[r2].add(c2)
        codes = sorted(all_codes_per_rev.get(start_rev, set()))

        for code in codes:
            # If the start is already the target, emit identity.
            if start_rev == target_rev:
                out.append((start_rev, code, code, 1.0))
                continue

            # BFS-like walk with weight multiplication. We need direction:
            # forward if start_rev is older than target_rev, backward if
            # newer. The adjacent maps in `step` only contain forward
            # edges (older → newer), so for backward composition we
            # invert the path (the loader can be given backward files
            # too if they exist).
            start_idx = _REV_ORDER.index(start_rev)
            if start_idx < target_idx:
                # Forward walk.
                frontier = {(start_rev, code): 1.0}
                for next_rev in _REV_ORDER[start_idx + 1 : target_idx + 1]:
                    new_frontier: Dict[Tuple[str, str], float] = defaultdict(float)
                    for (cur_rev, cur_code), cur_w in frontier.items():
                        edges = step.get((cur_rev, cur_code), [])
                        if not edges:
                            # No outgoing edge — drop the path (we'll
                            # emit an identity row for the original
                            # code below if no compositions worked).
                            continue
                        for (er, ec, ew) in edges:
                            if er == next_rev:
                                new_frontier[(er, ec)] += cur_w * ew
                    if not new_frontier:
                        break
                    frontier = dict(new_frontier)
                # frontier now contains (target_rev, code) -> total weight
                if frontier and all(r == target_rev for (r, _) in frontier.keys()):
                    for (_r, to_code), w in frontier.items():
                        out.append((start_rev, code, to_code, w))
                    continue
            # Backward walk or unresolved: emit identity row so the
            # matcher's identity-fallback covers it.
            out.append((start_rev, code, code, 1.0))

    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _default_src_dir() -> Path:
    return Path(__file__).resolve().parent / "hs_concordance_src"


def _default_output_path() -> Path:
    here = Path(__file__).resolve()
    return here.parent.parent / "app" / "data" / "reference" / "hs_concordance.csv"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build app/data/reference/hs_concordance.csv from UNSD pairwise "
            "correspondence tables. See the module docstring for the "
            "expected input layout."
        )
    )
    parser.add_argument(
        "--src",
        type=Path,
        default=_default_src_dir(),
        help="Directory containing UNSD pairwise correspondence files. "
             "Default: ./scripts/hs_concordance_src/",
    )
    parser.add_argument(
        "--target",
        default="H4",
        choices=_REV_ORDER,
        help="Canonical HS revision to map every code to. Default: H4 (HS 2012).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_default_output_path(),
        help="Output CSV path. Default: app/data/reference/hs_concordance.csv",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Verbose logging."
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    src_dir: Path = args.src
    if not src_dir.is_dir():
        LOG.error(
            "Source directory %s does not exist. Download UNSD correspondence "
            "tables (https://unstats.un.org/unsd/trade/classifications/"
            "correspondence-tables.asp) and place them there, then re-run.",
            src_dir,
        )
        return 2

    # Load every pairwise file present.
    adjacent: Dict[Tuple[str, str], List[Tuple[str, str, float]]] = {}
    pair_files: List[Path] = sorted(
        p for p in src_dir.iterdir()
        if p.is_file() and p.suffix.lower() in (".csv", ".tsv", ".txt", ".xlsx", ".xls")
    )
    if not pair_files:
        LOG.error("No CSV/XLSX files found in %s.", src_dir)
        return 2

    LOG.info("Found %d candidate file(s) in %s", len(pair_files), src_dir)
    for path in pair_files:
        from_rev, to_rev = _parse_pair_from_name(path.stem)
        if from_rev is None or to_rev is None:
            LOG.warning(
                "Could not infer (from_revision, to_revision) from filename %r — "
                "skipping. Expected pattern: HSxxxx_HSyyyy.{csv,xlsx}.",
                path.name,
            )
            continue
        pairs = _load_pair_file(path, from_rev, to_rev)
        if not pairs:
            LOG.warning("File %s yielded zero pairs — skipping.", path.name)
            continue
        LOG.info(
            "  %s → %s : %s rows from %s",
            from_rev, to_rev, f"{len(pairs):,}", path.name,
        )
        # Merge multiple files for the same pair by accumulating their rows.
        adjacent.setdefault((from_rev, to_rev), []).extend(pairs)

    if not adjacent:
        LOG.error(
            "No usable adjacent maps were loaded. The existing concordance CSV "
            "has been left untouched. See the module docstring for input "
            "format requirements."
        )
        return 2

    # Compose the chain.
    composed = _compose(adjacent, args.target)
    if not composed:
        LOG.error("Composition produced no rows — leaving output untouched.")
        return 2
    LOG.info(
        "Composed %s rows mapping into canonical revision %s",
        f"{len(composed):,}", args.target,
    )

    # Atomic write — only replace the output if everything above succeeded.
    out_path: Path = args.output
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")

    with tmp_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["from_revision", "from_code", "to_revision", "to_code", "weight", "note"])
        for from_rev, from_code, to_code, weight in composed:
            note = "identity" if from_rev == args.target and from_code == to_code else ""
            w.writerow([from_rev, from_code, args.target, to_code, f"{weight:.4f}", note])
    os.replace(tmp_path, out_path)

    LOG.info("Wrote %s", out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
