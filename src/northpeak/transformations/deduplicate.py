"""Deduplication with a guaranteed total order.

The whole of NFR-IDEM-01 rests on this module. `ROW_NUMBER() OVER (PARTITION BY
pk ORDER BY updated_at DESC)` is **not deterministic** when two records share
`updated_at` — which happens constantly in CDC extracts, because a bulk UPDATE
stamps every affected row with the same transaction timestamp.

When the order is ambiguous, Spark picks a winner by whatever the shuffle
happened to produce. Re-run the job and a different row can win. The output
changes with no code change and no error, which is the worst failure mode a
pipeline has.

So every ordering here ends with a tiebreaker that is unique per row. The
entity config supplies it: `order_by: [source_lsn, _ingest_ts, _record_hash]`.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ..common.config import EntityConfig
from ..common.logging import get_logger

log = get_logger(__name__)

# Appended when the configured ordering cannot be proven unique. Content hash
# is per-row and deterministic, so it makes any ordering a total one.
FALLBACK_TIEBREAKER = "_record_hash"


def _order_expressions(entity: EntityConfig, available: list[str]) -> list:
    """Build the DESC ordering, guaranteeing a total order.

    Columns absent from the DataFrame are dropped rather than raising: the CDC
    unwrap renames `source.lsn` to `_source_lsn`, and configs name it both ways
    across entities.
    """
    resolved: list[str] = []
    for column in entity.order_by:
        for candidate in (column, f"_{column}", column.lstrip("_")):
            if candidate in available and candidate not in resolved:
                resolved.append(candidate)
                break
        else:
            log.warning(f"[{entity.name}] order_by column '{column}' not present; skipped")

    if FALLBACK_TIEBREAKER in available and FALLBACK_TIEBREAKER not in resolved:
        resolved.append(FALLBACK_TIEBREAKER)

    if not resolved:
        raise ValueError(
            f"[{entity.name}] no usable ordering columns. Deduplicating without a "
            "deterministic order produces a different answer on every run."
        )
    # nulls_last: a NULL watermark must never beat a real one.
    return [F.col(c).desc_nulls_last() for c in resolved]


def deduplicate(df: DataFrame, entity: EntityConfig, keep_counts: bool = False) -> DataFrame:
    """Keep exactly one row per primary key: the latest by the total order."""
    order = _order_expressions(entity, df.columns)
    window = Window.partitionBy(*[F.col(c) for c in entity.primary_key]).orderBy(*order)

    ranked = df.withColumn("_rn", F.row_number().over(window))
    if keep_counts:
        ranked = ranked.withColumn(
            "_duplicate_count", F.count("*").over(Window.partitionBy(*entity.primary_key))
        )
    return ranked.where(F.col("_rn") == 1).drop("_rn")


def exact_duplicates(df: DataFrame) -> DataFrame:
    """Rows that are byte-identical to another row, by content hash.

    Distinct from primary-key duplication: an exact duplicate is a delivery
    artefact (a file sent twice), whereas two different rows sharing a key is a
    genuine update. They are reported separately because they mean different
    things.
    """
    window = Window.partitionBy(FALLBACK_TIEBREAKER)
    return (
        df.withColumn("_copies", F.count("*").over(window))
        .where(F.col("_copies") > 1)
        .drop("_copies")
    )


def dedup_report(df: DataFrame, entity: EntityConfig) -> dict:
    """Counts for the audit table, computed in one pass.

    Separate .count() calls would scan the input three times; on the events
    table that is the difference between seconds and minutes.
    """
    stats = df.agg(
        F.count("*").alias("total"),
        F.countDistinct(*[F.col(c) for c in entity.primary_key]).alias("distinct_keys"),
        (
            F.countDistinct(F.col(FALLBACK_TIEBREAKER)).alias("distinct_hashes")
            if FALLBACK_TIEBREAKER in df.columns
            else F.lit(None).alias("distinct_hashes")
        ),
    ).collect()[0]

    total, keys = stats["total"], stats["distinct_keys"]
    hashes = stats["distinct_hashes"]
    return {
        "entity": entity.name,
        "rows_in": total,
        "distinct_keys": keys,
        "key_duplicates": total - keys,
        "exact_duplicates": (total - hashes) if hashes is not None else None,
        "duplicate_rate": round((total - keys) / total, 6) if total else 0.0,
    }
