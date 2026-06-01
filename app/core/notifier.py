"""
Terminal status helpers for the pipeline.

The pipeline used to ship status updates through an interactive chat
bot; everything has been collapsed onto the standard logger so the
script runs unattended in any terminal. This module exposes:

* ``StopPipelineException`` / ``install_stop_handler`` /
  ``check_for_stop`` — graceful Ctrl-C handling. The signal handler
  flips a flag; ``check_for_stop`` re-raises at safe checkpoints so
  atomic ``.tmp`` writes are never left half-finished.
* ``notify_user(msg, ...)`` — log a status line, stripping any
  residual inline markup. Pipeline phases call this when they want
  a one-line status update to appear in the log.
* ``ProgressReporter`` — log a "Label — N%" line every ``step_pct``
  percent of progress. Replaces an old in-place progress bar.
* ``h(s)`` / ``strip_tags(s)`` — small string helpers. ``h`` is a
  shell-escape used by callers that interpolate paths; ``strip_tags``
  removes the legacy ``<b>`` / ``<code>`` markup older log strings
  still contain.
* Phase / status constants — fixed prefixes the log lines use so
  output stays visually consistent across phases.
"""

import html as _html
import re
import signal

from app.core.logger import get_logger

logger = get_logger(__name__)


# ----------------------------------------------------------------------
# Log prefixes — keep status lines consistent across phases.
# ----------------------------------------------------------------------
PHASE_RETRIEVAL    = "[retrieval]"
PHASE_AGGREGATE    = "[aggregate]"
PHASE_MATCH        = "[match]"
PHASE_MISINVOICING = "[misinvoicing]"
PHASE_PIPELINE     = "[pipeline]"

STATUS_OK     = "OK"
STATUS_DONE   = "DONE"
STATUS_WARN   = "WARN"
STATUS_ERR    = "ERR"
STATUS_SKIP   = "SKIP"
STATUS_STOP   = "STOP"
STATUS_INFO   = "INFO"
STATUS_RUN    = "RUN"
STATUS_SAMPLE = "SAMPLE"
STATUS_DISK   = "DISK"
STATUS_ZIP    = "ZIP"


# ----------------------------------------------------------------------
# Legacy string helpers — kept for the callers that still wrap dynamic
# values for display.
# ----------------------------------------------------------------------
def h(s):
    """Shell-escape a dynamic value for display. Kept as a thin
    wrapper around ``html.escape`` so existing call sites keep working."""
    return _html.escape(str(s), quote=False)


_TAG_RE = re.compile(r"</?(b|i|u|s|code|pre)[^>]*>", re.IGNORECASE)


def strip_tags(s):
    """Strip the inline ``<b>``/``<code>``/etc. markup older log strings
    still contain so the terminal sees clean text."""
    return _TAG_RE.sub("", str(s))


# ----------------------------------------------------------------------
# Stop handling — install a SIGINT handler that sets a flag; phases
# call ``check_for_stop`` at safe checkpoints between long-running
# steps and unwind cleanly when the flag is set.
# ----------------------------------------------------------------------
class StopPipelineException(Exception):
    """Raised when the user presses Ctrl-C; phases catch and exit cleanly."""


_STOP_REQUESTED = False


def _sigint_handler(_signum, _frame):
    global _STOP_REQUESTED
    _STOP_REQUESTED = True
    logger.warning("Ctrl-C received — pipeline will stop at the next checkpoint.")


def install_stop_handler():
    """Install the SIGINT handler. Idempotent — safe to call multiple
    times. Returns the previous handler so callers can restore it."""
    return signal.signal(signal.SIGINT, _sigint_handler)


def check_for_stop():
    """Raise ``StopPipelineException`` if Ctrl-C was received."""
    if _STOP_REQUESTED:
        raise StopPipelineException("Pipeline stopped by user (Ctrl-C)")


# ----------------------------------------------------------------------
# Log helpers — phase modules call ``notify_user`` for one-line status
# updates. ``send`` is kept as a legacy switch so existing call sites
# can mute a particular line without restructuring.
# ----------------------------------------------------------------------
def _log_at_level(level, msg):
    log_fn = getattr(logger, level, None)
    if not callable(log_fn):
        log_fn = logger.info
    log_fn(strip_tags(msg))


def notify_user(msg, *, send=True, level="info"):
    """Log ``msg`` at the given level after stripping inline markup."""
    if not send or msg is None:
        return None
    _log_at_level(level, msg)
    return None


# ----------------------------------------------------------------------
# ProgressReporter — log-only step reporter.
# ----------------------------------------------------------------------
class ProgressReporter:
    """Emit one INFO log line every ``step_pct`` percent.

    Phases that loop over many items (file merges, reporter scans)
    pass through here so the log shows ``"Label — 25%"`` once per
    threshold crossing instead of every iteration.
    """

    def __init__(self, label, step_pct=10, enabled=True):
        self.label = label
        self.step_pct = max(1, int(step_pct))
        self.enabled = enabled
        self._last_pct = -1

    def start(self, suffix=""):
        if not self.enabled:
            return
        logger.info("%s — 0%%%s", self.label, f" ({suffix})" if suffix else "")
        self._last_pct = 0

    def update(self, done, total, suffix=""):
        if total <= 0 or not self.enabled:
            return
        pct = int(done / total * 100)
        if pct == self._last_pct:
            return
        if pct < 100 and pct - self._last_pct < self.step_pct:
            return
        self._last_pct = pct
        logger.info(
            "%s — %d%%%s", self.label, pct, f" ({suffix})" if suffix else ""
        )

    def done(self, final_text=None, suffix=""):
        if not self.enabled:
            return
        logger.info(
            "%s — 100%%%s",
            final_text or self.label,
            f" ({suffix})" if suffix else "",
        )
        self._last_pct = 100
