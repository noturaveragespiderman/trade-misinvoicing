"""
Centralised logging setup for the pipeline.

Call ``setup_logging()`` once at process start (done in ``app/main.py``).
Anywhere else, do:

    from app.core.logger import get_logger
    logger = get_logger(__name__)

Console handler writes to stderr at LOG_LEVEL (default INFO). A rotating
file handler also writes to ``./logs/pipeline.log`` at DEBUG so a full
trace is always available on disk even when the console is INFO.

Override via env vars:
  LOG_LEVEL=DEBUG   bump console verbosity
  LOG_DIR=/path     redirect the file handler (default ./logs)
  LOG_FILE_LEVEL=…  bump/lower the file handler level
"""

import logging
import logging.handlers
import os
import sys

_CONFIGURED = False


def setup_logging():
    """Configure root logger. Idempotent — safe to call more than once."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    console_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    file_level = os.environ.get("LOG_FILE_LEVEL", "DEBUG").upper()
    log_dir = os.environ.get("LOG_DIR", "./logs")

    root = logging.getLogger()
    # Set the root threshold to whichever handler is most verbose, so neither
    # handler is throttled by the root level.
    root.setLevel(min(_to_level(console_level), _to_level(file_level)))

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stderr)
    console.setLevel(_to_level(console_level))
    console.setFormatter(fmt)
    root.addHandler(console)

    try:
        os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, "pipeline.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(_to_level(file_level))
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
    except OSError as e:
        root.warning("Could not attach file log handler at %s: %s", log_dir, e)

    # Quiet noisy third-party libs unless the user explicitly asks for DEBUG.
    if _to_level(console_level) > logging.DEBUG:
        for noisy in ("urllib3", "requests"):
            logging.getLogger(noisy).setLevel(logging.WARNING)

    _CONFIGURED = True


def get_logger(name):
    """Return a module-scoped logger. Triggers setup on first use."""
    if not _CONFIGURED:
        setup_logging()
    return logging.getLogger(name)


def _to_level(value):
    if isinstance(value, int):
        return value
    return getattr(logging, str(value).upper(), logging.INFO)
