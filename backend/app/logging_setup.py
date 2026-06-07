"""One-line-per-event logging, shared by the API and the worker.

A delivery service that logs nothing is impossible to debug after the fact, and
the previous version logged nothing at all. JSON is the default because both
processes write to stdout and something downstream will want to parse it.
"""

from __future__ import annotations

import json
import logging
import sys

#: Attributes ``logging`` puts on every record. Anything else came from the
#: caller's ``extra=`` and belongs in the payload.
_STANDARD_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "message",
    "asctime",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS:
                payload[key] = _stringify(value)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def _stringify(value: object) -> object:
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


def configure_logging(level: str = "INFO", *, as_json: bool = True) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        JsonFormatter()
        if as_json
        else logging.Formatter("%(asctime)s %(levelname)-7s %(name)s %(message)s")
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # SQLAlchemy's own logger is noisy at INFO when echo is on.
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
