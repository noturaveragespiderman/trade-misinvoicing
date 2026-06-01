"""
Shared .gz cache-check helper.

A reporter+year is considered cached only when a file in RAW_DIR:
  1. Matches the pattern  *CA{padded}{year}H*.gz  (the H classification
     marker anchors the year boundary, preventing substring collisions).
  2. Is non-empty and starts with the gzip magic bytes (0x1f 0x8b).

The integrity byte-check treats partial / truncated downloads as absent so
the download is retried instead of silently failing later in the pipeline.

Two access patterns are supported:

  • ``has_valid_gz(code, year)`` — single-shot check. Globs the year
    directory each call. Use this for one-off lookups (after a
    download, in audits, etc.).

  • ``index_year_cache(year)`` — bulk check. Scans the year directory
    ONCE and returns a ``{int_code: valid_path}`` dict for O(1)
    membership tests. Use this when iterating over many reporters
    (the retrieval / audit loops). Replaces ~2 globs per reporter
    with one ``os.scandir()`` and N magic-byte reads — typically a
    100-1000× speedup on a Dockploy bind-mounted volume with ~1500
    reporters per year.
"""

import glob
import os
import re

import pandas as pd

from app.config import RAW_DIR, AGGREGATE_DIR, raw_year_dir
from app.core.logger import get_logger

logger = get_logger(__name__)


# Gzip magic number — first two bytes of every valid .gz file.
_GZIP_MAGIC = b"\x1f\x8b"


def _candidate_paths(code, year):
    """Return filesystem paths that match (reporter, year).

    Searches raw/{year}/ first (new per-year layout), falling back to
    raw/ root for any files left in the flat layout pre-migration.
    """
    padded = str(code).zfill(3)
    pattern = f"*CA{padded}{year}H*.gz"
    paths = glob.glob(os.path.join(raw_year_dir(year), pattern))
    if not paths:
        paths = glob.glob(os.path.join(RAW_DIR, pattern))
    return paths


def _is_valid_gz(path):
    """True when the file exists, is non-empty, and has gzip magic bytes."""
    try:
        if os.path.getsize(path) < 3:
            return False
        with open(path, "rb") as fh:
            return fh.read(2) == _GZIP_MAGIC
    except OSError:
        return False


def has_valid_gz(code, year):
    """
    Return True if a valid .gz is already cached for (code, year).
    Silently deletes any matching file that is empty or corrupt so the
    next download attempt replaces it.

    Two-pass design:
      • first scan to see if at least one valid file exists (early-return
        on the first hit — no need to inspect the rest of the candidates)
      • only sweep corrupt files when no valid file was found, so we don't
        delete duplicates of a still-good cache entry
    """
    candidates = _candidate_paths(code, year)
    for path in candidates:
        if _is_valid_gz(path):
            return True
    # No valid file → sweep corrupt artefacts so the retry has a clean slate
    for path in candidates:
        try:
            os.remove(path)
            logger.info("Removed corrupt cache file: %s", os.path.basename(path))
        except OSError as e:
            logger.warning("Could not remove corrupt file %s: %s", path, e)
    return False


# Compiled once per process per year. The reporter code is the
# zero-padded value Comtrade emits between "CA" and the four-digit
# year. UN M49 codes max out at three digits today, but
# ``str.zfill(3)`` only pads up to a minimum, so the legacy ``glob``
# pattern ``*CA{padded}{year}H*.gz`` accepted any width. The lazy
# quantifier ``\d+?`` matches the same set: it consumes as few
# digits as possible while still leaving the literal year string
# matchable downstream.
_FILENAME_CODE_RE_TEMPLATE = r"CA(\d+?){year}H[^/]*\.gz$"
_PATTERN_CACHE = {}


def _filename_pattern(year):
    """Cache the compiled per-year regex so repeated calls don't recompile."""
    key = str(year)
    pat = _PATTERN_CACHE.get(key)
    if pat is None:
        pat = re.compile(_FILENAME_CODE_RE_TEMPLATE.format(year=re.escape(key)))
        _PATTERN_CACHE[key] = pat
    return pat


def index_year_cache(year, validate=True):
    """
    Scan ``raw/{year}/`` (and the legacy flat ``raw/`` layout, in
    that precedence order) ONCE and return a
    ``{int(reporter_code): valid_path}`` dict.

    Designed for the hot path in retrieval / audit loops: a single
    ``os.scandir()`` plus ``len(dir)`` magic-byte reads instead of
    one glob-per-reporter call. With ~1500 reporters per year the
    saving is roughly two orders of magnitude — and it scales the
    same way as the directory size, not the product of reporters
    and files.

    Parameters
    ----------
    year : str | int
        Year to index. Coerced to ``str``.
    validate : bool, default True
        When True, opens each candidate file briefly to confirm it
        starts with the gzip magic bytes (catches partial /
        truncated downloads). Set False on a slow network volume to
        skip the per-file open and trust ``size >= 3``; the trade-
        off is that a corrupt file would be reported as cached and
        only fail in the next phase.

    Notes
    -----
    Same precedence as ``has_valid_gz``: ``raw/{year}/`` matches win
    over the legacy flat ``raw/`` layout. Within a single directory,
    the first matching file per reporter wins (matches the
    glob-iteration order of the legacy code).
    """
    pat = _filename_pattern(year)
    idx = {}
    for d in (raw_year_dir(year), RAW_DIR):
        if not os.path.isdir(d):
            continue
        try:
            with os.scandir(d) as it:
                for entry in it:
                    if not entry.is_file():
                        continue
                    m = pat.search(entry.name)
                    if not m:
                        continue
                    code = int(m.group(1))
                    if code in idx:
                        # First hit wins, matching the precedence of
                        # _candidate_paths in has_valid_gz.
                        continue
                    path = entry.path
                    if validate:
                        if _is_valid_gz(path):
                            idx[code] = path
                    else:
                        try:
                            if entry.stat().st_size >= 3:
                                idx[code] = path
                        except OSError:
                            continue
        except OSError as e:
            logger.warning("index_year_cache scan of %s failed: %s", d, e)
            continue
    return idx


def aggregate_csv_complete(year, expected_codes):
    """
    True iff ``aggregate/{year}.csv`` exists, has a ``reporterCode`` column,
    and contains rows for *every* int reporter code in ``expected_codes``.

    Used by ``match.run_match_year`` to decide whether the pre-filtered
    AGGREGATE CSV (built by ``extract.build_aggregate_csv``) can be
    reused in lieu of re-streaming every ``.gz`` file. When this returns
    False the caller falls back to the slower ``.gz`` merge path so a
    partial aggregate CSV never silently feeds incomplete data to the
    matcher.

    Short-circuits as soon as every expected code is found — does not need
    to scan the whole file in the common case.
    """
    csv_path = os.path.join(AGGREGATE_DIR, f"{year}.csv")
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) < 16:
        return False
    expected = {int(c) for c in expected_codes}
    if not expected:
        # Nothing to validate against — refuse to claim completeness so the
        # caller takes the safe (.gz) path.
        return False
    try:
        with open(csv_path, "r", encoding="utf-8") as fh:
            header = fh.readline().strip().split(",")
        lower_map = {c.lower(): c for c in header}
        col = lower_map.get("reportercode")
        if not col:
            return False
        present = set()
        for chunk in pd.read_csv(csv_path, usecols=[col], dtype=str, chunksize=500_000):
            ints = pd.to_numeric(chunk[col], errors="coerce").dropna().astype(int)
            present.update(ints.unique())
            if expected.issubset(present):
                return True
        return expected.issubset(present)
    except Exception as e:
        logger.warning("aggregate_csv_complete check failed for %s: %s", year, e)
        return False


def sweep_corrupt(code, year):
    """
    Explicit post-download sweep: delete any zero-byte or non-gzip file
    matching (code, year). Returns the number of files removed.
    """
    removed = 0
    for path in _candidate_paths(code, year):
        if not _is_valid_gz(path):
            try:
                os.remove(path)
                removed += 1
            except OSError as e:
                logger.warning("Could not sweep corrupt file %s: %s", path, e)
    return removed
