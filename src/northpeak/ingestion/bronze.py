"""Bronze layer: ingest metadata and the append contract.

Bronze answers one question — "what did the source actually say?" — so the
only permitted transformation is *adding* provenance columns. No casting, no
cleaning, no filtering, no deduplication. Every one of those is a decision, and
decisions belong in Silver where they can be reviewed and reversed.

The metadata columns exist for concrete reasons, not for tidiness:

  _ingest_ts      when we saw it; freshness rules compare against this
  _ingest_date    partition column; makes replay of one load window cheap
  _source_system  which system, for reconciliation and lineage
  _batch_id       groups one logical load; the unit of replay
  _run_id         ties every row to a job run (NFR-OBS-04)
  _file_name      which file a row came from; the first question asked when
                  a bad row is found
  _record_hash    content fingerprint for exact-duplicate detection in Silver
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..common.config import EntityConfig, EnvConfig
from ..common.keys import record_hash
from ..common.logging import get_logger, get_run_id

log = get_logger(__name__)

METADATA_COLUMNS = [
    "_ingest_ts",
    "_ingest_date",
    "_source_system",
    "_batch_id",
    "_run_id",
    "_file_name",
    "_record_hash",
]


def add_ingest_metadata(df: DataFrame, entity: EntityConfig, batch_id: str) -> DataFrame:
    """Attach provenance. The only thing Bronze is allowed to do."""
    # Hash the source columns only — never the metadata. Including _ingest_ts
    # would make every row unique and the duplicate check useless, which is a
    # mistake that looks like it works right up until you count duplicates.
    source_columns = [c for c in df.columns if c not in METADATA_COLUMNS]

    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_ingest_date", F.current_date())
        .withColumn("_source_system", F.lit(entity.source.system))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_run_id", F.lit(get_run_id()))
        # input_file_name() is unavailable on some serverless configurations;
        # _metadata.file_path is the supported replacement and works on both.
        .withColumn("_file_name", F.col("_metadata.file_path"))
        .withColumn("_record_hash", record_hash(source_columns))
    )


def bronze_table_name(env: EnvConfig, entity: EntityConfig) -> str:
    return env.table("bronze", entity.name)


def validate_bronze(df: DataFrame, entity: EntityConfig) -> dict:
    """Post-append checks. Cheap, and they catch the failure that looks like
    success: a job that ran, succeeded, and ingested nothing.
    """
    total = df.count()
    result = {
        "entity": entity.name,
        "row_count": total,
        "distinct_files": df.select("_file_name").distinct().count() if total else 0,
        "distinct_hashes": df.select("_record_hash").distinct().count() if total else 0,
        "issues": [],
    }
    if total == 0:
        result["issues"].append("no rows ingested")
    if total and result["distinct_hashes"] < total:
        # Not an error at Bronze — duplicates are part of what the source said,
        # and Silver removes them. Recorded so the count is explainable.
        result["duplicate_rows"] = total - result["distinct_hashes"]

    missing = [c for c in entity.primary_key if c not in df.columns]
    # Debezium sources nest the business columns under `after`, so the primary
    # key legitimately is not a top-level column until Silver unwraps it.
    if missing and not entity.source.debezium_envelope:
        result["issues"].append(f"primary key columns absent: {missing}")
    return result
