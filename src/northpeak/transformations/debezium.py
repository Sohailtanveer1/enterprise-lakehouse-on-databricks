"""Debezium envelope unwrapping.

Bronze stores the envelope exactly as delivered. Silver flattens it. Doing this
in the sink instead would destroy the `before` image, and with it any chance of
SCD Type 2 — which is why the cdc-sink container deliberately does not.

Envelope shape (schemas disabled):

    {op, ts_ms, before: {...}, after: {...},
     source: {db, schema, table, lsn, txId, ts_ms, snapshot}}

    op:  c = create   r = snapshot read   u = update   d = delete

Three details that are easy to get wrong and each break CDC silently:

1. `after` is NULL for deletes, so a naive `after.*` expansion drops every
   delete and the target simply never loses rows.
2. A **tombstone** (null value, non-null key) follows every delete. It carries
   no payload and must not become a row.
3. `source.lsn` is the ordering key, not `ts_ms` and not the application's
   `updated_at`. It is the database's own total order over commits.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..common.logging import get_logger

log = get_logger(__name__)

OP_CREATE, OP_SNAPSHOT, OP_UPDATE, OP_DELETE = "c", "r", "u", "d"
DELETE_OPS = (OP_DELETE,)
UPSERT_OPS = (OP_CREATE, OP_SNAPSHOT, OP_UPDATE)

# Columns Silver adds while unwrapping. Downstream code keys off these rather
# than re-reading the envelope.
CDC_COLUMNS = ["_op", "_source_lsn", "_source_ts_ms", "_is_delete", "_is_snapshot"]


def is_envelope(df: DataFrame) -> bool:
    """Detect the envelope structurally rather than trusting config alone."""
    cols = set(df.columns)
    return {"op", "source"}.issubset(cols) and ("after" in cols or "before" in cols)


def unwrap(df: DataFrame, primary_key: list[str]) -> DataFrame:
    """Flatten the envelope into business columns plus CDC metadata.

    For deletes, `before` supplies the payload — that is the only place the
    deleted row's values still exist, and the primary key is needed to target
    the MERGE.
    """
    if not is_envelope(df):
        log.warning("unwrap() called on a non-envelope DataFrame; passing through")
        return df

    # Tombstones carry neither before nor after. Debezium emits one after every
    # delete; keeping them would insert an all-null row per delete.
    df = df.where(F.col("after").isNotNull() | F.col("before").isNotNull())

    after_fields = set(df.schema["after"].dataType.fieldNames()) if "after" in df.columns else set()
    before_fields = (
        set(df.schema["before"].dataType.fieldNames()) if "before" in df.columns else set()
    )
    business_columns = sorted(after_fields | before_fields)

    # coalesce(after.x, before.x): after for c/r/u, before for d.
    projections = [
        F.coalesce(
            F.col(f"after.{c}") if c in after_fields else F.lit(None),
            F.col(f"before.{c}") if c in before_fields else F.lit(None),
        ).alias(c)
        for c in business_columns
    ]

    metadata = [
        F.col("op").alias("_op"),
        F.col("source.lsn").cast("long").alias("_source_lsn"),
        F.col("source.ts_ms").cast("long").alias("_source_ts_ms"),
        (F.col("op") == F.lit(OP_DELETE)).alias("_is_delete"),
        (F.col("op") == F.lit(OP_SNAPSHOT)).alias("_is_snapshot"),
    ]
    # Carry Bronze provenance through so lineage survives the unwrap.
    passthrough = [F.col(c) for c in df.columns if c.startswith("_") and c != "_rescued_data"]

    out = df.select(*projections, *metadata, *passthrough)

    missing = [k for k in primary_key if k not in out.columns]
    if missing:
        # Fail loudly. A missing key here means the MERGE would match nothing
        # and every change would land as an insert.
        raise ValueError(
            f"primary key {missing} absent after unwrap; envelope fields were "
            f"{sorted(business_columns)}"
        )
    return out


def before_after_diff(df: DataFrame, columns: list[str]) -> DataFrame:
    """Attach `_changed_columns` for update events.

    Useful for auditing what a source actually changed, and for proving in an
    interview that an update event whose before and after are identical is a
    no-op the pipeline correctly ignores.
    """
    comparisons = [
        F.when(
            ~F.col(f"before.{c}").eqNullSafe(F.col(f"after.{c}")), F.lit(c)
        ).otherwise(F.lit(None))
        for c in columns
    ]
    return df.withColumn(
        "_changed_columns",
        F.when(
            F.col("op") == OP_UPDATE, F.array_compact(F.array(*comparisons))
        ).otherwise(F.array()),
    )


def split_deletes(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Partition into (upserts, deletes).

    Kept separate because they take different MERGE clauses, and because a
    delete count is worth reporting on its own — a source that suddenly deletes
    40% of its rows is an incident, not a load.
    """
    return df.where(~F.col("_is_delete")), df.where(F.col("_is_delete"))
