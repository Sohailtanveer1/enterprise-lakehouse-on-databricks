"""Spark session factory and the Databricks adapter (NFR-PORT-04).

The same `src/northpeak` package runs in two very different places:

  local        spark-dev container. Plain Apache Spark + delta-spark, a
               filesystem warehouse, no Unity Catalog, no Auto Loader.
  databricks   serverless. Session already exists, Unity Catalog governs
               everything, Auto Loader available.

Everything Databricks-specific is funnelled through this module so the rest of
the codebase never branches on environment. That is what lets the whole
pipeline be developed and tested locally without spending Databricks quota.
"""

from __future__ import annotations

import os
from functools import lru_cache

from pyspark.sql import SparkSession

from .config import EnvConfig
from .logging import get_logger

log = get_logger(__name__)


def on_databricks() -> bool:
    """True when running inside a Databricks runtime.

    Checked via the runtime version env var rather than by importing
    `dbutils`, because the import succeeds in some local shims and would give
    a false positive.
    """
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


@lru_cache(maxsize=1)
def get_spark(app_name: str = "northpeak") -> SparkSession:
    """Return the active session, creating a local one if needed."""
    if on_databricks():
        # Serverless owns the session. Creating one is at best ignored and at
        # worst an error; never call configure_spark_with_delta_pip here.
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Small local data: 200 shuffle partitions produces 200 tiny files and
        # dominates runtime with scheduling overhead.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        # Delta writes are otherwise legal but not readable by older readers;
        # pinning avoids surprise protocol upgrades between local and DBX.
        .config("spark.databricks.delta.autoCompact.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def ensure_namespaces(env: EnvConfig) -> None:
    """Create catalog/schemas if absent. Idempotent.

    On Databricks this is `CREATE CATALOG` + `CREATE SCHEMA`; locally there is
    no catalog concept, so only databases are created.
    """
    spark = get_spark()
    schemas = [env.bronze_schema, env.silver_schema, env.gold_schema, env.audit_schema]

    if env.use_unity_catalog:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {env.catalog}")
        for schema in schemas:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env.catalog}.{schema}")
    else:
        spark.conf.set("spark.sql.warehouse.dir", env.local_warehouse or "/tmp/warehouse")
        for schema in schemas:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    log.info(f"namespaces ready: {env.catalog} / {', '.join(schemas)}")


def table_exists(name: str) -> bool:
    return get_spark().catalog.tableExists(name)


def sql(query: str):
    """Run SQL through the active session. Kept here so tests can patch one
    place rather than every call site."""
    return get_spark().sql(query)
