"""Audit tables — the observability layer (NFR-OBS-01..03).

Three tables, one per question an operator actually asks:

  pipeline_run_audit      did it run, how long, how many rows, did it fail
  data_quality_results    what did the rules find
  reconciliation_results  do the numbers agree end to end

Every row carries `run_id`, so "which run produced this bad row?" is one query
rather than archaeology.

These are built rather than read from Databricks system tables because Free
Edition's system table access is limited (PHASE0-FEASIBILITY.md §3). In a paid
workspace you would join these against `system.billing` and
`system.access.audit` instead of duplicating them.
"""
from __future__ import annotations

import time
import traceback
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql import Row

from .config import EnvConfig
from .logging import get_logger, get_run_id
from .spark import get_spark

log = get_logger(__name__)

RUN_AUDIT = "pipeline_run_audit"
DQ_RESULTS = "data_quality_results"
RECON_RESULTS = "reconciliation_results"

DDL = {
    RUN_AUDIT: """(
        run_id STRING, job_id STRING, task_name STRING, entity STRING, layer STRING,
        status STRING, rows_in BIGINT, rows_out BIGINT, rows_rejected BIGINT,
        start_ts TIMESTAMP, end_ts TIMESTAMP, duration_s DOUBLE,
        error_message STRING, batch_id STRING, env STRING
    )""",
    DQ_RESULTS: """(
        run_id STRING, entity STRING, layer STRING, rule_name STRING, rule_type STRING,
        severity STRING, rows_checked BIGINT, rows_failed BIGINT, pass_rate DOUBLE,
        passed BOOLEAN, detail STRING, evaluated_at TIMESTAMP, env STRING
    )""",
    RECON_RESULTS: """(
        run_id STRING, check_name STRING, entity STRING,
        source_layer STRING, target_layer STRING,
        source_value STRING, target_value STRING, difference STRING,
        passed BOOLEAN, tolerance STRING, evaluated_at TIMESTAMP, env STRING
    )""",
}


@dataclass
class RunRecord:
    run_id: str
    job_id: str | None
    task_name: str
    entity: str
    layer: str
    status: str = "RUNNING"
    rows_in: int = 0
    rows_out: int = 0
    rows_rejected: int = 0
    start_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_ts: datetime | None = None
    duration_s: float = 0.0
    error_message: str | None = None
    batch_id: str | None = None
    env: str = ""


def ensure_audit_tables(env: EnvConfig) -> None:
    spark = get_spark()
    for name, cols in DDL.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {env.table('audit', name)} {cols} USING DELTA")


def _append(env: EnvConfig, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    spark = get_spark()
    target = env.table("audit", table)
    df = spark.createDataFrame([Row(**r) for r in rows])
    # mergeSchema so adding an audit column later does not require a migration
    # or, worse, a silent write failure at 04:00.
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)


def write_run(env: EnvConfig, record: RunRecord) -> None:
    _append(env, RUN_AUDIT, [asdict(record)])


def write_dq_results(env: EnvConfig, results: list[dict]) -> None:
    _append(env, DQ_RESULTS, results)


def write_recon_results(env: EnvConfig, results: list[dict]) -> None:
    _append(env, RECON_RESULTS, results)


@contextmanager
def audited_task(
    env: EnvConfig, task_name: str, entity: str, layer: str, batch_id: str | None = None
) -> Iterator[RunRecord]:
    """Wrap a task so it is recorded whether it succeeds or fails.

        with audited_task(env, "bronze_ingest", "orders", "bronze") as rec:
            rec.rows_out = ingest(...)

    A failure writes the audit row and re-raises. Recording only successes is
    how a pipeline ends up with no trace of the night it broke.
    """
    record = RunRecord(
        run_id=get_run_id(),
        job_id=__import__("os").environ.get("DATABRICKS_JOB_ID"),
        task_name=task_name,
        entity=entity,
        layer=layer,
        batch_id=batch_id,
        env=env.name,
    )
    started = time.time()
    try:
        yield record
        record.status = "SUCCESS"
    except Exception as exc:
        record.status = "FAILED"
        # Full traceback, truncated. The one-line str(exc) of a Spark error is
        # almost never the part that tells you what went wrong.
        record.error_message = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"[:4000]
        raise
    finally:
        record.end_ts = datetime.now(timezone.utc)
        record.duration_s = round(time.time() - started, 3)
        try:
            write_run(env, record)
        except Exception as audit_exc:  # noqa: BLE001
            # Never let an audit-write failure mask the real error.
            log.error(f"audit write failed for {task_name}/{entity}: {audit_exc}")
        log.info(
            f"[{entity}] {task_name} {record.status} "
            f"in {record.duration_s}s rows_out={record.rows_out} "
            f"rejected={record.rows_rejected}"
        )


def dq_row(
    entity: str, layer: str, rule_name: str, rule_type: str, severity: str,
    rows_checked: int, rows_failed: int, passed: bool, detail: str, env_name: str,
) -> dict[str, Any]:
    return {
        "run_id": get_run_id(), "entity": entity, "layer": layer,
        "rule_name": rule_name, "rule_type": rule_type, "severity": severity,
        "rows_checked": rows_checked, "rows_failed": rows_failed,
        "pass_rate": round(1 - rows_failed / rows_checked, 6) if rows_checked else 1.0,
        "passed": passed, "detail": detail[:1000],
        "evaluated_at": datetime.now(timezone.utc), "env": env_name,
    }
