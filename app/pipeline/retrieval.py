"""
Phase 1: .gz retrieval.

Years and reporter scope are read from ``app/config.py``. The caller
(``app.main``) passes them in explicitly. Reporters that fail to
download are recorded in the per-year stats sidecar at
``raw/_retrieval_stats.json``; there is no automatic retry — re-run
the phase to pick up missing ones from cache.

Cache check: before any API call, ``has_valid_gz`` (from gzcache)
checks ``raw/{year}/`` for an existing .gz matching the reporter and
year, so nothing is re-downloaded.
"""

import json
import os
import time

import comtradeapicall
import requests

from app.config import (
    COMTRADE_API_KEY,
    RAW_DIR,
    SLEEP_TIMEOUT,
    YEARS,
    raw_year_dir,
)
from app.core.notifier import check_for_stop
from app.core.db import fetch_all_reporters
from app.core.gzcache import has_valid_gz, sweep_corrupt, index_year_cache
from app.core.logger import get_logger
from app.core import progress as progress_mod

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Per-year retrieval stats sidecar — persisted automatically so the Reports
# phase (option 5 → Country report) can rebuild the xlsx without redoing
# RETRIEVAL. Lives under raw/ since that's the phase's canonical
# output folder.
# ---------------------------------------------------------------------------
_RETRIEVAL_STATS_PATH = os.path.join(RAW_DIR, "_retrieval_stats.json")


def _save_retrieval_stats(year, payload):
    os.makedirs(RAW_DIR, exist_ok=True)
    existing = {}
    if os.path.exists(_RETRIEVAL_STATS_PATH):
        try:
            with open(_RETRIEVAL_STATS_PATH, "r", encoding="utf-8") as fh:
                existing = json.load(fh)
        except Exception as e:
            logger.warning("Could not read retrieval stats: %s", e)
    existing[str(year)] = payload
    try:
        progress_mod.atomic_write_json(_RETRIEVAL_STATS_PATH, existing)
    except Exception as e:
        logger.warning("Could not write retrieval stats: %s", e)


# Transient errors get an exponential-backoff retry inside _download_one;
# anything else we surface once to the log and move on.
_TRANSIENT_EXC = (
    requests.RequestException,
    ConnectionError,
    TimeoutError,
)


# ----------------------------------------------------------------------
# Download primitives
# ----------------------------------------------------------------------
def _download_one(code, year, max_attempts=3):
    # Drop a breadcrumb so a SIGKILL during the API call leaves enough
    # state on disk for the next run to know which reporter was mid-flight.
    # The retrieval-stats sidecar is written per-year; this in-flight
    # sidecar is written per-reporter and cleared on completion.
    try:
        progress_mod.retrieval_mark_in_flight(year, code)
    except Exception as e:
        logger.warning("retrieval_mark_in_flight failed for %s/%s: %s", code, year, e)

    # Write into raw/{year}/ — keeps each year's downloads grouped so
    # the listing stays manageable (one folder per year instead of one
    # flat directory holding 1500+ .gz files).
    out_dir = raw_year_dir(year)
    os.makedirs(out_dir, exist_ok=True)

    last_err = None
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                comtradeapicall.bulkDownloadFinalFile(
                    subscription_key=COMTRADE_API_KEY,
                    directory=out_dir,
                    typeCode="C",
                    freqCode="A",
                    clCode="HS",
                    period=str(year),
                    reporterCode=str(code),
                    decompress=False,
                )
                break  # API call returned (success OR "no data for this year")
            except _TRANSIENT_EXC as e:
                last_err = e
                wait = 2 ** (attempt - 1)
                logger.warning(
                    "transient error %s/%s (attempt %d/%d): %s; retrying in %ds",
                    code,
                    year,
                    attempt,
                    max_attempts,
                    e,
                    wait,
                )
                time.sleep(wait)
            except Exception as e:
                # Permanent / unknown error — don't retry.
                logger.warning("API error %s/%s: %s", code, year, e)
                last_err = e
                break

        # Inter-call rate limit (not a retry sleep): Comtrade allows ~3 req/s
        # for non-premium keys; SLEEP_TIMEOUT (~0.3s) keeps us well under.
        # Always pause regardless of success/failure so a fast 'no data' reply
        # doesn't immediately trigger another call.
        time.sleep(SLEEP_TIMEOUT)
        # Delete any zero-byte / non-gzip artefacts the API may have left behind.
        sweep_corrupt(code, year)
        return has_valid_gz(code, year)
    finally:
        try:
            progress_mod.retrieval_clear_in_flight(year, code)
        except Exception as e:
            logger.warning("retrieval_clear_in_flight failed for %s/%s: %s", code, year, e)


# ----------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------
def retrieve(targets=None, years=None, ask_continue=True):
    os.makedirs(RAW_DIR, exist_ok=True)

    if targets is None or years is None:
        logger.info("Loading reporters from local db…")
        reporters = fetch_all_reporters()
        if not reporters:
            logger.error("Could not load reporters from app/data/reporter_codes.csv.")
            return {}
        years = years or [str(y) for y in YEARS]
        # Scope defaults to "all reporters". Callers that want a
        # narrower subset must pass `targets=`.
        targets = targets or reporters
        logger.info(
            "Retrieval scope: all reporters — %d targets · years %s–%s",
            len(targets), years[0], years[-1],
        )

    stats = {}

    for i, year in enumerate(years):
        check_for_stop()
        logger.info("Retrieving year %s", year)
        try:
            progress_mod.retrieval_set_year_expected(year, len(targets))
        except Exception as e:
            logger.warning("retrieval_set_year_expected failed for %s: %s", year, e)
        downloaded = cached = 0
        failed_reporters = []

        # Cache index — one os.scandir() of raw/{year}/ instead of two
        # glob.glob() calls per reporter inside has_valid_gz. On a
        # Dockploy bind-mounted volume with ~1500 reporters this turns
        # several minutes of cache-checking into a fraction of a
        # second. Reporters newly downloaded in this loop don't need
        # to be added back to the index — each reporter is checked
        # exactly once.
        t_index = time.time()
        cache_index = index_year_cache(year)
        index_s = time.time() - t_index
        logger.info(
            "Year %s — cache index built in %.2fs (%d valid .gz files)",
            year, index_s, len(cache_index),
        )

        for r in targets:
            check_for_stop()
            if int(r["code"]) in cache_index:
                cached += 1
                continue
            if _download_one(r["code"], year):
                downloaded += 1
            else:
                failed_reporters.append(r)

        present = len(targets) - len(failed_reporters)

        if present == 0:
            logger.error(
                "Year %s FAILED — no .gz retrieved for any of the %d reporters.",
                year, len(targets),
            )
        else:
            logger.info(
                "Year %s complete — downloaded=%d cached=%d missing=%d / %d",
                year, downloaded, cached, len(failed_reporters), len(targets),
            )

        # Per-year payload — snapshots of present + missing reporters with
        # full meta so the Reports phase doesn't have to re-scan the .gz
        # filesystem at report time. The Country xlsx reads this sidecar.
        stats[year] = {
            "downloaded": downloaded,
            "cached": cached,
            "missing": len(failed_reporters),
            "present": present,
            "targets": len(targets),
            "missing_reporters": [
                {
                    "code": int(r["code"]),
                    "fullname": str(r.get("fullname", "")),
                    "continent": str(r.get("continent", "")),
                    "status": str(r.get("status", "")),
                }
                for r in failed_reporters
            ],
            "present_reporters": [
                {
                    "code": int(r["code"]),
                    "fullname": str(r.get("fullname", "")),
                    "continent": str(r.get("continent", "")),
                    "status": str(r.get("status", "")),
                }
                for r in targets
                if r not in failed_reporters
            ],
        }
        _save_retrieval_stats(year, stats[year])

        # Auto-continue to next year — no per-year confirmation prompt.
        _ = ask_continue  # kept for API compatibility; ignored.

    return stats


if __name__ == "__main__":
    retrieve()
