"""CDC apply — Delta MERGE with insert/update/delete semantics.

Turns a deduplicated batch of change events into the current state of a Silver
table. The batch must already be deduplicated to one row per key
(deduplicate.py); MERGE raises if the source matches a target row more than
once, and that error is the correct behaviour — silently picking one would be
non-deterministic.

Deletes are **soft**. A hard delete would erase the row Gold's facts point at,
so the fact would either break or silently drop. `is_deleted` lets Gold decide,
and preserves the audit trail that a hard delete destroys.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..common.config import EntityConfig, EnvConfig
from ..common.logging import get_logger
from ..common.spark import get_spark, table_exists

log = get_logger(__name__)

SOFT_DELETE_COLUMNS = {"is_deleted": False, "deleted_at": None}


def ensure_target(df: DataFrame, table: str, entity: EntityConfig) -> None:
    """Create the Silver table from the batch's schema on first run."""
    if table_exists(table):
        return
    writer = df.limit(0).write.format("delta")
    if entity.partition_by:
        writer = writer.partitionBy(*entity.partition_by)
    writer.saveAsTable(table)

    if entity.cluster_by:
        # Liquid clustering adapts as query patterns change, without the
        # rewrite that changing a partition column requires.
        get_spark().sql(f"ALTER TABLE {table} CLUSTER BY ({', '.join(entity.cluster_by)})")
    # Change Data Feed lets Gold read only what changed instead of rebuilding
    # from the whole table. Must be enabled at creation to be useful.
    get_spark().sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    log.info(f"created silver table {table}")


def prepare(df: DataFrame, entity: EntityConfig) -> DataFrame:
    """Add soft-delete columns and normalise CDC metadata."""
    if "_is_delete" not in df.columns:
        # Non-CDC sources: every row is an upsert.
        df = df.withColumn("_is_delete", F.lit(False))
    return (
        df.withColumn("is_deleted", F.col("_is_delete"))
        .withColumn(
            "deleted_at",
            F.when(F.col("_is_delete"), F.current_timestamp()).otherwise(
                F.lit(None).cast("timestamp")
            ),
        )
        .withColumn("_silver_updated_at", F.current_timestamp())
    )


def apply_merge(
    df: DataFrame, env: EnvConfig, entity: EntityConfig, target_table: str | None = None
) -> dict:
    """MERGE the batch into Silver. Returns operation metrics."""
    target = target_table or env.table("silver", entity.name)
    source = prepare(df, entity)
    ensure_target(source, target, entity)

    condition = " AND ".join(f"t.{k} <=> s.{k}" for k in entity.primary_key)
    # <=> is null-safe. Plain `=` returns NULL for a NULL key, the row never
    # matches, and every change becomes an insert — duplicating the key on
    # every run. Composite keys with a nullable member hit this constantly.

    delta_table = DeltaTable.forName(get_spark(), target)
    update_columns = {c: f"s.{c}" for c in source.columns if c not in entity.primary_key}

    (
        delta_table.alias("t")
        .merge(source.alias("s"), condition)
        # Only overwrite when the incoming change is newer. Without this guard,
        # a replayed or out-of-order file overwrites current state with stale
        # values — and the job still reports success.
        .whenMatchedUpdate(condition=_newer_than_target(source, entity), set=update_columns)
        .whenNotMatchedInsertAll()
        .execute()
    )

    metrics = _last_operation_metrics(delta_table)
    log.info(
        f"[{entity.name}] merge -> {target}: "
        f"inserted={metrics.get('numTargetRowsInserted')} "
        f"updated={metrics.get('numTargetRowsUpdated')} "
        f"deleted(soft)={source.where(F.col('_is_delete')).count()}"
    )
    return metrics


def _newer_than_target(source: DataFrame, entity: EntityConfig) -> str:
    """Guard so only newer changes win.

    LSN when available — the database's own total order. Otherwise the
    watermark. Falls back to unconditional update when neither exists, which is
    correct for full-snapshot sources where the whole batch is by definition
    current.
    """
    if "_source_lsn" in source.columns:
        return "s._source_lsn >= t._source_lsn"
    if entity.watermark_column and entity.watermark_column in source.columns:
        col = entity.watermark_column
        return f"s.{col} >= t.{col} OR t.{col} IS NULL"
    return "true"


def _last_operation_metrics(delta_table: DeltaTable) -> dict:
    try:
        row = delta_table.history(1).select("operationMetrics").collect()[0]
        return {k: int(v) for k, v in (row["operationMetrics"] or {}).items() if v.isdigit()}
    except Exception as exc:
        log.warning(f"could not read operation metrics: {exc}")
        return {}


def read_changes(env: EnvConfig, entity_name: str, since_version: int) -> DataFrame:
    """Read the Change Data Feed since a version.

    This is what makes incremental Gold builds possible: instead of rebuilding
    a fact from the whole Silver table, read only the rows that changed.
    """
    table = env.table("silver", entity_name)
    return (
        get_spark()
        .read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", since_version)
        .table(table)
        # CDF emits update_preimage and update_postimage as separate rows.
        # Keeping both double-counts every update.
        .where(F.col("_change_type") != "update_preimage")
    )


def current_version(env: EnvConfig, entity_name: str) -> int:
    table = env.table("silver", entity_name)
    if not table_exists(table):
        return 0
    return int(DeltaTable.forName(get_spark(), table).history(1).collect()[0]["version"])
