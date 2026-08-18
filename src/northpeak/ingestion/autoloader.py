"""Incremental file ingestion — Auto Loader, with a local equivalent.

**Auto Loader (`cloudFiles`) is a Databricks-proprietary source. It does not
exist in open-source Spark.** Calling it in the spark-dev container fails with
an unhelpful "data source not found". So this module has two implementations
behind one signature — the concrete instance of the adapter pattern that
NFR-PORT-04 asks for.

              Databricks                    local (spark-dev)
  source      cloudFiles                    Spark file streaming source
  file state  RocksDB, scales to millions   checkpoint offset log
  schema      inferred + evolving           explicit or sampled
  rescue      _rescued_data column          not available

The semantics that matter for *logic* testing are shared: both are streaming
sources over a directory, both checkpoint their progress, both process each
file exactly once, and both resume where they stopped. What local cannot test
is Auto Loader's schema evolution and rescued-data behaviour, which is called
out in ARCHITECTURE.md §9 and validated on Databricks.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from ..common.config import EntityConfig, EnvConfig
from ..common.logging import get_logger
from ..common.spark import get_spark, on_databricks

log = get_logger(__name__)

# addNewColumns: a new column appears -> the stream fails once, records the new
# schema, and succeeds on restart. That restart is why Bronze ingestion is
# retried by the job (NFR-REL-02) rather than treated as a hard failure.
# Pairs with NFR-DQ-08: an added column is WARN, not FATAL.
SCHEMA_EVOLUTION_MODE = "addNewColumns"


def landing_path(env: EnvConfig, entity: EntityConfig) -> str:
    return f"{env.landing_root.rstrip('/')}/{entity.source.path_glob}"


def source_dir(env: EnvConfig, entity: EntityConfig) -> str:
    """Directory to watch: every path segment before the first wildcard one.

    Auto Loader takes a base directory, not a fully globbed path, and it
    discovers files recursively beneath it.

        erp/orders/dt=*/*.parquet  ->  <root>/erp/orders
        customers/dt=*/*.json      ->  <root>/customers
        stores/*.csv               ->  <root>/stores

    Splitting on the first "*" character instead of the first wildcard
    *segment* leaves a dangling "dt=" on the path, which resolves to nothing
    and produces an empty ingest that looks like a successful run.

    Watching the parent also means Spark reads dt= as a Hive-style partition
    column, so the landing date arrives for free.
    """
    segments = entity.source.path_glob.strip("/").split("/")
    kept: list[str] = []
    for segment in segments:
        if "*" in segment or "?" in segment:
            break
        kept.append(segment)
    return "/".join([env.landing_root.rstrip("/"), *kept])


def checkpoint_path(env: EnvConfig, entity: EntityConfig) -> str:
    return f"{env.checkpoint_root.rstrip('/')}/bronze/{entity.name}"


def schema_path(env: EnvConfig, entity: EntityConfig) -> str:
    return f"{env.schema_root.rstrip('/')}/bronze/{entity.name}"


def read_incremental(env: EnvConfig, entity: EntityConfig) -> DataFrame:
    """Streaming DataFrame of files not yet processed for this entity."""
    spark = get_spark()
    fmt = entity.source.format
    directory = source_dir(env, entity)

    if on_databricks():
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.schemaLocation", schema_path(env, entity))
            .option("cloudFiles.schemaEvolutionMode", SCHEMA_EVOLUTION_MODE)
            # Malformed records land here instead of failing the batch or being
            # silently dropped. Bronze keeps everything; Silver decides.
            .option("rescuedDataColumn", "_rescued_data")
            # Bound the batch so one enormous backfill cannot exhaust the
            # Free Edition quota in a single trigger.
            .option("cloudFiles.maxFilesPerTrigger", "200")
        )
        if entity.source.format == "csv":
            # Without this, Auto Loader types every CSV column as string. With
            # it, a column that is int today and alphanumeric tomorrow becomes
            # a schema-evolution event rather than silent corruption.
            reader = reader.option("cloudFiles.inferColumnTypes", "true")
        if entity.schema_hints:
            reader = reader.option("cloudFiles.schemaHints", entity.schema_hints)
    else:
        # Local: Spark's own file streaming source. It requires an explicit
        # schema (no inference on streaming reads), so infer once from the
        # files already present and reuse it.
        static = spark.read.format(fmt).options(**entity.source.options)
        if fmt in ("json", "csv"):
            static = static.option("inferSchema", "true")
        inferred = static.load(directory).schema
        reader = spark.readStream.format(fmt).schema(inferred).option("maxFilesPerTrigger", "200")

    for key, value in entity.source.options.items():
        reader = reader.option(key, value)

    log.info(f"[{entity.name}] reading {fmt} from {directory}")
    return reader.load(directory)


def write_bronze(
    df: DataFrame,
    env: EnvConfig,
    entity: EntityConfig,
    target_table: str,
) -> int:
    """Append to the Bronze table and block until the batch completes.

    `availableNow` drains everything currently available and stops. This is
    incremental batch on the streaming engine — checkpointing, exactly-once
    file handling and progress tracking, without an always-on stream that
    would exhaust the Free Edition daily quota (ARCHITECTURE.md §1).
    """
    writer = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path(env, entity))
        # Bronze is append-only and schema-permissive by design. mergeSchema
        # lets a new source column land without a manual migration; it can
        # never remove or retype an existing one.
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
    )
    if entity.partition_by:
        writer = writer.partitionBy(*entity.partition_by)

    query = writer.toTable(target_table)
    query.awaitTermination()

    rows = sum(p["numOutputRows"] for p in query.recentProgress) if query.recentProgress else 0
    log.info(f"[{entity.name}] bronze append complete: {rows} rows -> {target_table}")
    return int(rows)
