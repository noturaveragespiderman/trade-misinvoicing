"""
Unified progress / checkpoint helpers used by every pipeline phase.

Every phase persists a small JSON sidecar describing what has already
been completed, so a crash (SIGKILL, OOM, network blip) leaves enough
state on disk for the next run to resume exactly where it stopped.

The phases each store their progress JSON next to their data outputs:

    aggregate/{year}_progress.json       — per-reporter checkpoints
    match/{year}_progress.json           — stage marker (merged → complete)
    misinvoicing/{year}_progress.json    — year-level idempotent skip flag
    raw/_retrieval_progress.json         — cross-year in-flight breadcrumbs

Atomic writes: every save goes through a ``.tmp`` path → ``flush`` →
``os.fsync`` → ``os.replace`` so a crash mid-write can never leave a
half-written JSON.
"""

import json
import os
import time
from datetime import datetime, timezone

from app.config import (
    AGGREGATE_DIR,
    MATCH_DIR,
    MISINVOICING_DIR,
    RAW_DIR,
)
from app.core.logger import get_logger

logger = get_logger(__name__)

SCHEMA_VERSION = 1

# Directory each phase writes its progress JSON into.
_PHASE_DIRS = {
    "aggregate": AGGREGATE_DIR,
    "match": MATCH_DIR,
    "misinvoicing": MISINVOICING_DIR,
}


def _utcnow_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Atomic write — building block reused by every sidecar across the pipeline.
# ---------------------------------------------------------------------------
def atomic_write_json(path, payload):
    """Write ``payload`` to ``path`` atomically.

    Sequence: write to ``path + ".tmp"`` → flush → fsync → ``os.replace``.
    A SIGKILL between any two steps either leaves the original file
    untouched (if the rename hasn't happened) or commits the new file
    fully (if it has). There is no in-between state.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------
def progress_path(phase, year):
    """Resolve the progress JSON path for a per-year phase."""
    if phase not in _PHASE_DIRS:
        raise ValueError(f"Unknown per-year phase: {phase!r}")
    return os.path.join(_PHASE_DIRS[phase], f"{year}_progress.json")


def retrieval_progress_path():
    """Cross-year retrieval breadcrumb sidecar."""
    return os.path.join(RAW_DIR, "_retrieval_progress.json")


# ---------------------------------------------------------------------------
# Load / save
# ---------------------------------------------------------------------------
def _read_json(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception as e:
        logger.warning("Could not read progress JSON %s: %s", path, e)
        return None
    if not isinstance(data, dict):
        logger.warning("Progress JSON %s is not a dict — ignoring.", path)
        return None
    schema = data.get("schema_version")
    if schema is not None and schema != SCHEMA_VERSION:
        logger.warning(
            "Progress JSON %s has schema_version=%s (expected %d) — treating as fresh.",
            path,
            schema,
            SCHEMA_VERSION,
        )
        return None
    return data


def load_progress(phase, year):
    """Return the progress payload for (phase, year), or None if missing."""
    return _read_json(progress_path(phase, year))


def save_progress(phase, year, payload):
    """Atomically write the progress payload for (phase, year)."""
    payload = dict(payload)
    payload.setdefault("phase", phase)
    payload.setdefault("year", str(year))
    payload["schema_version"] = SCHEMA_VERSION
    payload["updated_at"] = _utcnow_iso()
    payload.setdefault("started_at", payload["updated_at"])
    atomic_write_json(progress_path(phase, year), payload)


# ---------------------------------------------------------------------------
# Per-reporter helpers (Aggregate phase is the main caller)
# ---------------------------------------------------------------------------
def init_year_progress(phase, year, expected_reporters, partials_dir=None):
    """Create or refresh the progress JSON for (phase, year).

    Existing ``completed_reporters`` are preserved; ``expected_reporters``
    is overwritten to reflect the current scope.
    """
    existing = load_progress(phase, year) or {}
    completed = set(int(c) for c in existing.get("completed_reporters", []))
    payload = {
        "phase": phase,
        "year": str(year),
        "expected_reporters": sorted(int(c) for c in expected_reporters),
        "completed_reporters": sorted(completed),
        "partials_dir": partials_dir,
        "started_at": existing.get("started_at", _utcnow_iso()),
    }
    save_progress(phase, year, payload)
    return payload


def mark_reporter_complete(phase, year, reporter_int):
    """Append a reporter to ``completed_reporters`` and persist atomically."""
    payload = load_progress(phase, year) or {
        "phase": phase,
        "year": str(year),
        "expected_reporters": [],
        "completed_reporters": [],
        "partials_dir": None,
    }
    completed = set(int(c) for c in payload.get("completed_reporters", []))
    completed.add(int(reporter_int))
    payload["completed_reporters"] = sorted(completed)
    save_progress(phase, year, payload)


def validate_progress(phase, year, partial_resolver=None):
    """Drop completed reporters whose partial files no longer exist.

    ``partial_resolver(reporter_int) -> path`` is called once per
    completed reporter; if it returns a path that does not exist or is
    too small to contain a header (<16 B), that reporter is removed
    from ``completed_reporters`` and re-added to the work queue.
    """
    payload = load_progress(phase, year)
    if not payload:
        return None
    if partial_resolver is None:
        return payload
    completed = list(payload.get("completed_reporters", []))
    cleaned = []
    dropped = []
    for rc in completed:
        path = partial_resolver(int(rc))
        if path and os.path.exists(path) and os.path.getsize(path) >= 16:
            cleaned.append(int(rc))
        else:
            dropped.append(int(rc))
    if dropped:
        logger.warning(
            "Progress %s/%s: dropping %d reporter(s) with missing partials: %s",
            phase,
            year,
            len(dropped),
            dropped,
        )
        payload["completed_reporters"] = cleaned
        save_progress(phase, year, payload)
    return payload


# ---------------------------------------------------------------------------
# Stale .tmp cleanup — every phase calls this on entry.
# ---------------------------------------------------------------------------
def sweep_stale_tmp(directory):
    """Remove ``*.tmp`` files left behind by a crashed previous run."""
    if not os.path.isdir(directory):
        return 0
    removed = 0
    for name in os.listdir(directory):
        if not name.endswith(".tmp"):
            continue
        path = os.path.join(directory, name)
        try:
            os.remove(path)
            removed += 1
            logger.info("Swept stale tmp file: %s", path)
        except OSError as e:
            logger.warning("Could not remove stale tmp %s: %s", path, e)
    return removed


# ---------------------------------------------------------------------------
# Retrieval-phase breadcrumb (cross-year, only tracks in-flight downloads).
# ---------------------------------------------------------------------------
def retrieval_load():
    return _read_json(retrieval_progress_path()) or {
        "schema_version": SCHEMA_VERSION,
        "years": {},
    }


def retrieval_mark_in_flight(year, reporter_int):
    payload = retrieval_load()
    years = payload.setdefault("years", {})
    entry = years.setdefault(
        str(year),
        {"started_at": _utcnow_iso(), "in_flight": [], "expected": 0},
    )
    in_flight = set(int(c) for c in entry.get("in_flight", []))
    in_flight.add(int(reporter_int))
    entry["in_flight"] = sorted(in_flight)
    entry["updated_at"] = _utcnow_iso()
    payload["schema_version"] = SCHEMA_VERSION
    atomic_write_json(retrieval_progress_path(), payload)


def retrieval_clear_in_flight(year, reporter_int):
    payload = retrieval_load()
    years = payload.get("years", {})
    entry = years.get(str(year))
    if not entry:
        return
    in_flight = set(int(c) for c in entry.get("in_flight", []))
    in_flight.discard(int(reporter_int))
    entry["in_flight"] = sorted(in_flight)
    entry["updated_at"] = _utcnow_iso()
    payload["schema_version"] = SCHEMA_VERSION
    atomic_write_json(retrieval_progress_path(), payload)


def retrieval_set_year_expected(year, expected_count):
    payload = retrieval_load()
    years = payload.setdefault("years", {})
    entry = years.setdefault(
        str(year),
        {"started_at": _utcnow_iso(), "in_flight": [], "expected": 0},
    )
    entry["expected"] = int(expected_count)
    entry["updated_at"] = _utcnow_iso()
    payload["schema_version"] = SCHEMA_VERSION
    atomic_write_json(retrieval_progress_path(), payload)
