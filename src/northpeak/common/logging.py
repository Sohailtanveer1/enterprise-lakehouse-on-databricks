"""Structured logging with run_id propagation.

NFR-OBS-04: one `run_id` per job run, propagated to every task, table and log
line. With it, "which run produced this bad row?" is one query. Without it, the
answer is archaeology.

JSON output, never bare print(). Databricks captures stdout per task, and JSON
lines are greppable and parseable where prose is not.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

_RUN_ID: str | None = None
_CONTEXT: dict[str, Any] = {}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": get_run_id(),
            **_CONTEXT,
        }
        extra = getattr(record, "extra_fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_run_id() -> str:
    """Current run id, created on first use.

    Prefers the Databricks job run id when present, so a log line can be traced
    back to a specific run in the Jobs UI without a lookup table.
    """
    global _RUN_ID
    if _RUN_ID is None:
        _RUN_ID = os.environ.get("DATABRICKS_JOB_RUN_ID") or f"local-{uuid.uuid4().hex[:12]}"
    return _RUN_ID


def set_run_id(run_id: str) -> None:
    """Override the run id — used when a parent job passes it to a child task."""
    global _RUN_ID
    _RUN_ID = run_id


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("NORTHPEAK_LOG_LEVEL", "INFO"))
        # Without this, Spark's root handler prints every line a second time
        # in plain text and the JSON becomes unreadable.
        logger.propagate = False
    return logger


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Attach fields to every log line inside the block.

        with log_context(entity="orders", layer="silver"):
            log.info("merged")   # carries entity and layer automatically

    Beats threading the same kwargs through twelve call sites.
    """
    global _CONTEXT
    previous = dict(_CONTEXT)
    _CONTEXT.update({k: v for k, v in fields.items() if v is not None})
    try:
        yield
    finally:
        _CONTEXT = previous


def log_event(logger: logging.Logger, level: str, message: str, **fields: Any) -> None:
    """Log with structured fields attached to this line only."""
    logger.log(
        getattr(logging, level.upper()),
        message,
        extra={"extra_fields": fields},
    )
