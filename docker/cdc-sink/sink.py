"""Kafka -> Parquet -> GCS sink for Debezium change events.

PHASE 4 STUB. Phase 2 only proves the container builds and can reach Kafka.
The batching, Parquet write and GCS upload land in Phase 4 alongside the
Bronze ingestion framework, because the two share a file-layout contract.

Design decisions already fixed, so Phase 4 implements rather than decides:

  * Land one Parquet file per (topic, batch), partitioned by ingest date:
        {prefix}/erp/{table}/dt=YYYY-MM-DD/part-{batch_id}.parquet
    Date-partitioned paths keep Auto Loader's directory listing cheap.

  * Preserve the Debezium envelope verbatim. Unwrapping happens in Silver,
    not here. A sink that "helpfully" flattens destroys the before-image and
    with it any chance of SCD Type 2.

  * Commit Kafka offsets only after the GCS upload succeeds. Offsets committed
    first means a crash silently loses a batch — the exact silent data loss
    the reconciliation checks exist to catch.

  * Tombstones (null value, non-null key) are retained. They mark deletes and
    dropping them makes deletes unrecoverable downstream.
"""
from __future__ import annotations

import os
import sys

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "landing")

TOPICS = [
    "northpeak.erp.orders",
    "northpeak.erp.order_items",
    "northpeak.erp.payments",
]


def main() -> int:
    print(f"cdc-sink stub. bootstrap={KAFKA_BOOTSTRAP} bucket={GCS_BUCKET or '<unset>'}")
    print(f"topics: {', '.join(TOPICS)}")
    print("Implementation lands in Phase 4. Container idling.")
    # Idle rather than exit, so `docker compose ps` shows the estate as intended
    # and a crash-loop is not mistaken for a real failure.
    import time

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    sys.exit(main())
