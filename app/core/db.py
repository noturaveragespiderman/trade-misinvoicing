"""
Local file-backed reference data. Every lookup the pipeline needs at
runtime is loaded from a CSV under ``app/data/``.
"""

import csv
import os
from app.config import (
    REPORTER_CODES_FILE,
    HS_CONCORDANCE_FILE,
    CANONICAL_HS_REVISION,
    ENTREPOT_REPORTERS_FILE,
)


# ──────────────────────────────────────────────
# Reporter codes
# ──────────────────────────────────────────────
def fetch_all_reporters():
    """
    Reads app/data/reporter_codes.csv and returns dicts with keys
    {code, name, fullname, continent, status}.
    """
    if not os.path.exists(REPORTER_CODES_FILE):
        raise FileNotFoundError(f"Reference file not found: {REPORTER_CODES_FILE}")

    out = []
    with open(REPORTER_CODES_FILE, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                code = int(float((row.get("iso_code") or "").strip()))
            except (TypeError, ValueError):
                continue
            out.append(
                {
                    "code": code,
                    "name": (row.get("name") or "").strip() or "Unknown",
                    "fullname": (row.get("full_name") or row.get("name") or "").strip()
                    or "Unknown",
                    "continent": (row.get("continent") or "").strip() or "Unknown",
                    "status": (row.get("status") or "").strip() or "Unknown",
                }
            )
    return out


# ──────────────────────────────────────────────
# HS code normalisation
# ──────────────────────────────────────────────
def normalize_cmdcode(raw):
    """
    Zero-pad a raw cmdCode string to its canonical HS level.

    Raw codes are not always zero-padded in .gz files:
      '8'     → '08'     (HS2)
      '101'   → '0101'   (HS4)
      '10110' → '010110' (HS6)

    Returns (padded_code, hs_level_str) or (None, None) for invalid codes.
    """
    s = str(raw).strip()
    if not s or not s.isdigit():
        return None, None
    n = len(s)
    if n <= 2:
        return s.zfill(2), "HS2"
    if n <= 4:
        return s.zfill(4), "HS4"
    if n <= 6:
        return s.zfill(6), "HS6"
    return None, None


def hs_level(cmd_code):
    """Return 'HS2', 'HS4', or 'HS6' for a normalised cmdCode, or None."""
    _, level = normalize_cmdcode(cmd_code)
    return level


# ──────────────────────────────────────────────
# HS revision concordance
# ──────────────────────────────────────────────
# In-process cache so the matcher only parses the CSV once per run.
_HS_CONCORDANCE_CACHE = None


def load_hs_concordance(target_revision=None):
    """
    Load ``app/data/reference/hs_concordance.csv`` and return a
    ``(from_revision, from_code) -> to_code`` dict whose ``to_code``
    is in ``target_revision``.

    `target_revision` defaults to ``config.CANONICAL_HS_REVISION``
    (currently `H4` / HS 2012). One-to-many mappings collapse to the
    highest-weight row; ties resolve to whichever the CSV listed
    first (deterministic).

    Codes that don't appear in the table are interpreted as
    revision-stable — the caller's identity-fallback handles them.
    Missing or unreadable file → empty dict (everything falls back
    to identity).

    Result format:
        {("H3", "854240"): "854231",  # share 0.4 wins
         ("H4", "270900"): "270900",  # explicit identity row
         ...}

    The function caches its result in-process; the cache key is the
    target_revision. Reload the process to pick up CSV edits.
    """
    global _HS_CONCORDANCE_CACHE
    target = target_revision or CANONICAL_HS_REVISION
    if _HS_CONCORDANCE_CACHE is not None and _HS_CONCORDANCE_CACHE[0] == target:
        return _HS_CONCORDANCE_CACHE[1]

    table = {}
    if os.path.exists(HS_CONCORDANCE_FILE):
        try:
            with open(HS_CONCORDANCE_FILE, "r", encoding="utf-8", newline="") as f:
                # ``best_per_key[(from_rev, from_code)] = (weight, to_code)``
                # so we can keep only the highest-weight to_code per
                # source key without holding the whole table in memory.
                best_per_key = {}
                for row in csv.DictReader(f):
                    if (row.get("to_revision") or "").strip() != target:
                        # Only keep rows that map TO our canonical
                        # revision; other rows are noise for this
                        # lookup direction.
                        continue
                    key = (
                        (row.get("from_revision") or "").strip(),
                        (row.get("from_code") or "").strip(),
                    )
                    if not key[0] or not key[1]:
                        continue
                    try:
                        w = float(row.get("weight") or 0.0)
                    except (TypeError, ValueError):
                        w = 0.0
                    to_code = (row.get("to_code") or "").strip()
                    if not to_code:
                        continue
                    prev = best_per_key.get(key)
                    if prev is None or w > prev[0]:
                        best_per_key[key] = (w, to_code)
                table = {k: v[1] for k, v in best_per_key.items()}
        except Exception:
            # Defensive — a malformed concordance shouldn't crash the
            # matcher. Empty dict makes every code identity-mapped.
            table = {}

    _HS_CONCORDANCE_CACHE = (target, table)
    return table


# ──────────────────────────────────────────────
# Entrepôt reporter set
# ──────────────────────────────────────────────
_ENTREPOT_CACHE = None


def load_entrepot_codes():
    """
    Return ``set[int]`` of reporter codes flagged as major entrepôts in
    ``app/data/reference/entrepot_reporters.csv`` — UN/UNCTAD-style
    re-export hubs whose bilateral residuals are systematically
    distorted by the Rotterdam-effect mechanism (the partner's M
    includes re-exported goods that the hub's X never reported as
    such, since the pipeline drops RX/RM).

    Cached after the first call. Empty set on read error / missing
    file (interpreted as "no entrepôts configured" — every pair
    flagged ``False`` downstream).
    """
    global _ENTREPOT_CACHE
    if _ENTREPOT_CACHE is not None:
        return _ENTREPOT_CACHE
    out = set()
    if os.path.exists(ENTREPOT_REPORTERS_FILE):
        try:
            with open(ENTREPOT_REPORTERS_FILE, "r", encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    s = (row.get("code") or "").strip()
                    if not s:
                        continue
                    try:
                        out.add(int(float(s)))
                    except (TypeError, ValueError):
                        continue
        except Exception:
            out = set()
    _ENTREPOT_CACHE = out
    return out


def concord_cmdcode(classification_code, cmd_code, table=None):
    """
    Map ``(classification_code, cmd_code)`` to its canonical HS6
    code per the concordance table.

    Returns the canonical code, or ``cmd_code`` unchanged if:
      • the input is not HS6 (HS2/HS4 are revision-stable),
      • the (revision, code) pair isn't in the concordance,
      • or the input classification IS already the target revision.

    `table` is optional; the function loads it lazily if not supplied.
    Pass it explicitly in hot loops to avoid the dict lookup overhead.
    """
    code = str(cmd_code).strip()
    rev = str(classification_code).strip()
    if not code or not rev:
        return cmd_code
    # HS2 / HS4 are revision-stable across HS 1992–2022 — they
    # describe chapters and headings, not 6-digit sub-headings.
    if len(code) <= 4:
        return cmd_code
    if rev == CANONICAL_HS_REVISION:
        return cmd_code
    if table is None:
        table = load_hs_concordance()
    return table.get((rev, code), cmd_code)
