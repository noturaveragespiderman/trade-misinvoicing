"""
Terminal entry point for the UN Comtrade mirror-statistics pipeline.

The orchestrator reads every per-run knob from ``app/config.py``: which
years to process, which phases to run, which reporters to keep,
which HS level / commodity prefixes to retain. There is no
interactive prompt — edit the config, then run::

    python -m app.main

Four data phases, executed in the order listed in ``config.PHASES``:

  1. retrieval     download raw .gz files into ./raw/{year}/
  2. aggregate     build ./aggregate/{year}.csv from .gz files
  3. match         pair X / M rows; produce
                     match/{year}_{matched,unmatched}.csv and
                     clean/{year}.csv
  4. misinvoicing  per-pair residuals → ./misinvoicing/{year}.csv

Each phase reads only what the previous one produced on disk, so a
run can be resumed by setting ``PHASES`` to the slice you want.
Phases are interleaved per year (year N runs all requested phases
before year N+1 starts) so peak disk usage stays bounded to one year
of intermediates at a time.

Ctrl-C is honoured: the signal handler in ``app.core.notifier`` sets
a flag that the next ``check_for_stop()`` checkpoint raises on, so
the pipeline exits without corrupting any partially-written CSVs.
"""

import os
import sys

from app.core.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

from app.config import (
    RAW_DIR,
    AGGREGATE_DIR,
    MATCH_DIR,
    MISINVOICING_DIR,
    YEARS,
    PHASES,
    REPORTER_SCOPE,
    HS_LEVEL,
    COMMODITY_FILTER,
)
from app.core.notifier import (
    check_for_stop,
    StopPipelineException,
    install_stop_handler,
)
from app.core.db import fetch_all_reporters
from app.pipeline.retrieval import retrieve
from app.pipeline.extract import build_aggregate_csv
from app.pipeline.misinvoicing import run_misinvoicing


# ──────────────────────────────────────────────
# Per-phase runners — each takes (year, targets).
# ──────────────────────────────────────────────
def _phase_retrieval(year, targets):
    os.makedirs(RAW_DIR, exist_ok=True)
    retrieve(targets=targets, years=[str(year)], ask_continue=False)


def _phase_aggregate(year, targets):
    os.makedirs(AGGREGATE_DIR, exist_ok=True)
    target_codes = [r["code"] for r in targets]
    build_aggregate_csv(year, targets=target_codes)


def _phase_match(year, targets):
    os.makedirs(MATCH_DIR, exist_ok=True)
    from app.pipeline.match import run_match_year

    run_match_year(year, targets, scope_label="All reporters")


def _phase_misinvoicing(year, _targets):
    os.makedirs(MISINVOICING_DIR, exist_ok=True)
    run_misinvoicing(year)


_PHASE_RUNNERS = {
    "retrieval":    _phase_retrieval,
    "aggregate":    _phase_aggregate,
    "match":        _phase_match,
    "misinvoicing": _phase_misinvoicing,
}


# ──────────────────────────────────────────────
# Helpers — validate config, resolve scope.
# ──────────────────────────────────────────────
def _validate_phases(requested):
    unknown = [p for p in requested if p not in _PHASE_RUNNERS]
    if unknown:
        raise SystemExit(
            f"Unknown phases in config.PHASES: {unknown}. "
            f"Valid options: {list(_PHASE_RUNNERS)}"
        )
    # Phases must run in canonical order — the pipeline is sequential
    # on disk. We accept any contiguous slice of the canonical order.
    canonical = list(_PHASE_RUNNERS)
    ordered = [p for p in canonical if p in requested]
    if list(requested) != ordered:
        logger.warning(
            "config.PHASES was reordered (%s); running in canonical order: %s",
            list(requested), ordered,
        )
    return ordered


def _validate_years(requested):
    if not requested:
        raise SystemExit("config.YEARS is empty — set at least one year.")
    out = []
    for y in requested:
        try:
            out.append(str(int(str(y).strip())))
        except (TypeError, ValueError):
            raise SystemExit(f"Invalid year in config.YEARS: {y!r}")
    return out


def _resolve_targets():
    """Return the list of reporter dicts to process.

    ``REPORTER_SCOPE`` is either ``None`` (use every reporter in
    ``reporter_codes.csv``) or a list of numeric M49 codes to keep.
    Unknown codes are logged and skipped.
    """
    reporters = fetch_all_reporters()
    if not reporters:
        raise SystemExit(
            "Could not load reporters from app/data/reporter_codes.csv."
        )
    if REPORTER_SCOPE is None:
        return reporters
    wanted = {int(c) for c in REPORTER_SCOPE}
    keep = [r for r in reporters if int(r["code"]) in wanted]
    found = {int(r["code"]) for r in keep}
    missing = sorted(wanted - found)
    if missing:
        logger.warning(
            "REPORTER_SCOPE codes not found in reporter_codes.csv: %s",
            missing,
        )
    if not keep:
        raise SystemExit(
            "REPORTER_SCOPE matched no reporters. Check the codes in config.py."
        )
    return keep


# ──────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────
def run_pipeline():
    install_stop_handler()

    phases  = _validate_phases(PHASES)
    years   = _validate_years(YEARS)
    targets = _resolve_targets()

    logger.info("=" * 70)
    logger.info("UN Comtrade pipeline — config-driven run")
    logger.info("  phases:    %s", " → ".join(phases))
    logger.info("  years:     %s – %s (%d total)", years[0], years[-1], len(years))
    logger.info("  reporters: %d", len(targets))
    logger.info("  HS level:  %s", HS_LEVEL)
    if COMMODITY_FILTER:
        logger.info("  commodity prefixes: %s", ", ".join(COMMODITY_FILTER))
    else:
        logger.info("  commodity prefixes: (none — all HS codes kept)")
    logger.info("=" * 70)

    try:
        for y in years:
            check_for_stop()
            logger.info("[year %s] starting", y)
            for phase in phases:
                check_for_stop()
                logger.info("[year %s] phase: %s", y, phase)
                _PHASE_RUNNERS[phase](y, targets)
            # After every requested phase finishes for this year, run
            # ``shrink_year`` to gzip in-place partials so the next
            # year has the disk space it needs. Only worth doing when
            # match (or later) ran — otherwise there's nothing to shrink.
            if any(p in phases for p in ("match", "misinvoicing")):
                check_for_stop()
                from app.pipeline.match import shrink_year
                shrink_year(str(y))
            logger.info("[year %s] done", y)
    except StopPipelineException:
        logger.warning("Pipeline interrupted by user — exiting cleanly.")
        return 130
    except Exception as e:
        logger.exception("Pipeline crashed: %s: %s", type(e).__name__, e)
        return 1

    logger.info("All requested phases finished for years %s – %s.",
                years[0], years[-1])
    return 0


if __name__ == "__main__":
    sys.exit(run_pipeline())
