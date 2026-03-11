"""
Structured logging helper.

Creates a logger that writes JSON lines so logs are easily parseable
in CloudWatch / Splunk / ELK.
"""

from __future__ import annotations

import logging
import json
import sys
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Return a named logger that writes JSON to *stderr*.

    Parameters
    ----------
    name  : logical name (usually ``__name__`` of the calling module).
    level : one of DEBUG / INFO / WARNING / ERROR / CRITICAL.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger
