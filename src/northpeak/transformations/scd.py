"""Slowly changing dimensions — Type 1 and Type 2.

**Type 1** overwrites. Used where history has no analytical value: a customer's
phone number changes, and nobody asks what it used to be.

**Type 2** versions. Used where history changes the answer. The canonical case
from ARCHITECTURE.md §7: a customer moves from Delhi to Mumbai. An order placed
in January must stay attributed to Delhi forever. Overwriting silently restates
every historical report, and it is invisible — the report still runs.

The Type 2 write is two operations that must both land or neither:

    close   prior current version -> effective_end = new_start - 1ms,
            is_current = false
    open    new version -> effective_start = change time, is_current = true,
            new surrogate key

Delta gives ACID over one table, so this is expressed as a single MERGE with a
union trick rather than two statements. Two statements would leave a window in
which a key has zero current rows, and any Gold build in that window loses
those rows entirely.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..common.config import EntityConfig, EnvConfig
from ..common.keys import SCD2_MAX_TS, record_hash, surrogate_key, versioned_surrogate_key
from ..common.logging import get_logger
from ..common.spark import get_spark, table_exists

log = get_logger(__name__)

EFFECTIVE_START = "effective_start_ts"
EFFECTIVE_END = "effective_end_ts"
IS_CURRENT = "is_current"
IS_INFERRED = "is_inferred"
HASH_COL = "_scd_hash"


# --------------------------------------------------------------------- Type 1


def apply_scd1(
    df: DataFrame, env: EnvConfig, entity: EntityConfig, target_table: str | None = None
) -> dict:
    """Overwrite in place. One row per business key, always the latest."""
    target = target_table or env.table("gold", f"dim_{entity.name}")
    key = entity.primary_key

    source = df.withColumn(f"{entity.name}_sk", surrogate_key(*key)).withColumn(
        "_dim_updated_at", F.current_timestamp()
    )

    if not table_exists(target):
        source.write.format("delta").saveAsTable(target)
        return {"created": True, "rows": source.count()}

    condition = " AND ".join(f"t.{k} <=> s.{k}" for k in key)
    (
        DeltaTable.forName(get_spark(), target)
        .alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log.info(f"[{entity.name}] SCD1 merge -> {target}")
    return {"created": False, "rows": source.count()}


# --------------------------------------------------------------------- Type 2


def apply_scd2(
    df: DataFrame,
    env: EnvConfig,
    entity: EntityConfig,
    target_table: str | None = None,
    effective_from: str = "_silver_updated_at",
) -> dict:
    """Version the dimension, preserving history."""
    target = target_table or env.table("gold", f"dim_{entity.name}")
    key = entity.primary_key
    tracked = entity.scd2_tracked_columns
    sk_column = f"{entity.name}_sk"

    if not tracked:
        raise ValueError(f"[{entity.name}] scd2 requires scd2_tracked_columns")

    start_col = effective_from if effective_from in df.columns else None

    # Hash only the TRACKED columns. Hashing every column opens a new version
    # whenever any field changes — including _ingest_ts, which changes on every
    # single load. The dimension would then grow by its full size every run.
    source = (
        df.withColumn(HASH_COL, record_hash(tracked))
        .withColumn(
            EFFECTIVE_START,
            F.col(start_col).cast("timestamp") if start_col else F.current_timestamp(),
        )
        .withColumn(EFFECTIVE_END, F.lit(SCD2_MAX_TS).cast("timestamp"))
        .withColumn(IS_CURRENT, F.lit(True))
        .withColumn(IS_INFERRED, F.lit(False))
    )
    source = source.withColumn(sk_column, versioned_surrogate_key(key, EFFECTIVE_START))

    if not table_exists(target):
        source.write.format("delta").saveAsTable(target)
        get_spark().sql(
            f"ALTER TABLE {target} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
        rows = source.count()
        log.info(f"[{entity.name}] SCD2 initial load: {rows} rows -> {target}")
        return {"created": True, "inserted": rows, "closed": 0}

    delta_table = DeltaTable.forName(get_spark(), target)
    current = delta_table.toDF().where(F.col(IS_CURRENT))

    join_condition = [source[k] == current[k] for k in key]
    changed = (
        source.alias("s")
        .join(current.alias("c"), join_condition, "inner")
        .where(F.col(f"c.{HASH_COL}") != F.col(f"s.{HASH_COL}"))
        .select("s.*")
    )
    new_keys = source.alias("s").join(current.alias("c"), join_condition, "left_anti")

    changed_count = changed.count()
    new_count = new_keys.count()
    if changed_count == 0 and new_count == 0:
        log.info(f"[{entity.name}] SCD2: no changes")
        return {"created": False, "inserted": 0, "closed": 0}

    # The union trick. Rows to insert are marked _scd_action='insert' with a
    # NULL merge key so they can never match an existing row; rows to close
    # carry the real key so they do match. One MERGE, one transaction, no
    # window in which a key has zero current versions.
    to_close = changed.withColumn("_merge_key", F.concat_ws("||", *[F.col(k) for k in key]))
    to_insert = changed.unionByName(new_keys, allowMissingColumns=True).withColumn(
        "_merge_key", F.lit(None).cast("string")
    )
    staged = to_close.unionByName(to_insert, allowMissingColumns=True)

    target_key = F.concat_ws("||", *[F.col(f"t.{k}") for k in key])

    (
        delta_table.alias("t")
        .merge(
            staged.alias("s"),
            F.expr("s._merge_key IS NOT NULL").__and__(target_key == F.col("s._merge_key"))
            & F.col(f"t.{IS_CURRENT}"),
        )
        .whenMatchedUpdate(
            set={
                # 1ms before the new version starts, so the ranges are a clean
                # half-open partition with no gap and no overlap. An as-of join
                # must never match two versions.
                EFFECTIVE_END: F.expr(f"s.{EFFECTIVE_START} - INTERVAL 1 MILLISECOND"),
                IS_CURRENT: F.lit(False),
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    log.info(
        f"[{entity.name}] SCD2 -> {target}: closed={changed_count} "
        f"inserted={changed_count + new_count}"
    )
    return {"created": False, "inserted": changed_count + new_count, "closed": changed_count}


def add_inferred_members(
    facts: DataFrame, dimension_table: str, business_key: str, entity_name: str
) -> int:
    """Insert placeholder dimension rows for keys a fact references but the
    dimension has not received yet (DATA_MODEL.md §4).

    An inferred member keeps the linkage: when the real record arrives it
    updates this row in place, and the fact's surrogate key never needs
    restating. Routing orphans to a shared "-1 Unknown" member loses the
    linkage permanently and is only correct when the natural key itself is
    missing.
    """
    spark = get_spark()
    if not table_exists(dimension_table):
        return 0

    dimension = spark.table(dimension_table)
    orphans = (
        facts.select(F.col(business_key).alias("_bk"))
        .where(F.col("_bk").isNotNull())
        .distinct()
        .join(
            dimension.select(F.col(business_key).alias("_dk")),
            F.col("_bk") == F.col("_dk"),
            "left_anti",
        )
    )
    count = orphans.count()
    if count == 0:
        return 0

    sk_column = f"{entity_name}_sk"
    inferred = (
        orphans.withColumnRenamed("_bk", business_key)
        .withColumn(EFFECTIVE_START, F.lit("1900-01-01 00:00:00").cast("timestamp"))
        .withColumn(EFFECTIVE_END, F.lit(SCD2_MAX_TS).cast("timestamp"))
        .withColumn(IS_CURRENT, F.lit(True))
        .withColumn(IS_INFERRED, F.lit(True))
    )
    inferred = inferred.withColumn(
        sk_column, versioned_surrogate_key([business_key], EFFECTIVE_START)
    )
    inferred.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        dimension_table
    )

    log.warning(f"[{entity_name}] inserted {count} inferred members into {dimension_table}")
    return count


def as_of_join(
    facts: DataFrame,
    dimension: DataFrame,
    business_key: str,
    event_time_column: str,
    surrogate_key_column: str,
) -> DataFrame:
    """Resolve the dimension version effective at the fact's event time.

    NOT `is_current = true`. Joining on is_current attributes every historical
    order to the customer's present region, silently restating history every
    time somebody moves. This half-open range is the whole point of SCD2.
    """
    dim = dimension.select(
        F.col(business_key).alias("_dim_bk"),
        F.col(surrogate_key_column).alias(surrogate_key_column),
        F.col(EFFECTIVE_START).alias("_dim_start"),
        F.col(EFFECTIVE_END).alias("_dim_end"),
    )
    return facts.join(
        dim,
        (F.col(business_key) == F.col("_dim_bk"))
        & (F.col(event_time_column) >= F.col("_dim_start"))
        & (F.col(event_time_column) <= F.col("_dim_end")),
        "left",
    ).drop("_dim_bk", "_dim_start", "_dim_end")
