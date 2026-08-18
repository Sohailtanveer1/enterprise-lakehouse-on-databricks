"""Silver pipeline runner — Bronze to trusted, one config-driven engine.

    python -m northpeak.transformations.silver --env local
    python -m northpeak.transformations.silver --env dev --entity orders

Stage order is not arbitrary; each step depends on the last:

    unwrap        Debezium envelope -> business columns + CDC metadata
    standardise   trim, casefold codes, cast types, normalise nulls
    dedup         one row per key, by a guaranteed total order
    quality       WARN / ERROR / FATAL routing, quarantine
    apply         MERGE into Silver (soft deletes) or SCD1 / SCD2

Dedup runs **before** quality on purpose. Quality first would count the same
bad row three times if it arrived three times, making the pass rate a function
of duplication rather than of data quality.

Quality runs **before** apply on purpose. Merging first and validating after
means bad rows are already in the target when you find out.
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..common import audit
from ..common.config import (
    EntityConfig,
    EnvConfig,
    SCDType,
    list_entities,
    load_entity,
    load_env,
    load_quality,
)
from ..common.logging import get_logger, log_context
from ..common.spark import ensure_namespaces, get_spark, table_exists
from ..quality import engine as dq
from . import cdc, debezium, deduplicate, scd

log = get_logger(__name__)

# Codes are compared against fixed domains, so casing must be canonical before
# the domain rule runs — otherwise "shipped" fails a rule that lists "SHIPPED".
UPPERCASE_SUFFIXES = ("_status", "_id", "_type", "_reason", "region", "state", "channel")


def standardise(df: DataFrame, entity: EntityConfig) -> DataFrame:
    """Types, whitespace and canonical codes. No business rules."""
    out = df
    for column, dtype in df.dtypes:
        if column.startswith("_") or dtype != "string":
            continue
        value = F.trim(F.col(column))
        # Empty string and whitespace-only are the same absence as NULL. Left
        # alone they defeat every not_null rule while carrying no information.
        value = F.when(value == "", None).otherwise(value)
        if column.endswith(UPPERCASE_SUFFIXES) or column in UPPERCASE_SUFFIXES:
            value = F.upper(value)
        out = out.withColumn(column, value)

    if entity.exclude_columns:
        # P0 PII with no analytical use never reaches Silver at all. Dropping
        # here rather than masking later means it cannot leak through a view
        # somebody forgets to mask.
        present = [c for c in entity.exclude_columns if c in out.columns]
        if present:
            out = out.drop(*present)
            log.info(f"[{entity.name}] dropped excluded columns: {present}")
    return out


def process_entity(env: EnvConfig, entity: EntityConfig) -> dict:
    spark = get_spark()
    bronze_table = env.table("bronze", entity.name)

    if not table_exists(bronze_table):
        log.warning(f"[{entity.name}] bronze table missing; skipping")
        return {"entity": entity.name, "status": "SKIPPED", "reason": "no bronze table"}

    with log_context(entity=entity.name, layer="silver"):
        with audit.audited_task(env, "silver_transform", entity.name, "silver") as rec:
            df = spark.table(bronze_table)
            rec.rows_in = df.count()

            if entity.source.debezium_envelope:
                df = debezium.unwrap(df, entity.primary_key)

            df = standardise(df, entity)

            report = deduplicate.dedup_report(df, entity)
            df = deduplicate.deduplicate(df, entity)
            log.info(f"dedup: {report['key_duplicates']} key duplicates removed")

            outcome = dq.evaluate(
                df,
                load_quality(entity.name),
                env,
                layer="silver",
                trailing_mean=dq.trailing_mean_rows(env, entity.name),
            )
            dq.write_quarantine(outcome, env, entity.name)
            audit.write_dq_results(env, outcome.results)
            rec.rows_rejected = outcome.rows_quarantined

            valid = outcome.valid
            cdc.apply_merge(valid, env, entity)

            if entity.scd_type is SCDType.TYPE_2:
                scd.apply_scd2(valid, env, entity)
            elif entity.scd_type is SCDType.TYPE_1:
                scd.apply_scd1(valid, env, entity)

            rec.rows_out = outcome.rows_valid
            return {
                "entity": entity.name, "status": "SUCCESS",
                "rows_in": report["rows_in"], "rows_out": outcome.rows_valid,
                "quarantined": outcome.rows_quarantined,
                "duplicates_removed": report["key_duplicates"],
                "pass_rate": outcome.pass_rate,
            }


def run(env_name: str, entities: list[str] | None = None) -> list[dict]:
    env = load_env(env_name)
    ensure_namespaces(env)
    audit.ensure_audit_tables(env)

    selected = [load_entity(n) for n in entities] if entities else list_entities()

    # Sequential, and deliberately dependency-ordered. Referential-integrity
    # rules read the referenced Silver table, so customers must land before
    # orders or the check silently reports SKIPPED on every first run.
    priority = {"categories": 0, "stores": 0, "promotions": 0, "customers": 1,
                "products": 1, "orders": 2, "order_items": 3, "payments": 3,
                "shipments": 3, "returns": 3, "inventory": 3, "events": 3}
    selected.sort(key=lambda e: priority.get(e.name, 9))

    results = []
    for entity in selected:
        try:
            results.append(process_entity(env, entity))
        except dq.FatalDataQualityError as exc:
            log.error(f"[{entity.name}] FATAL: {exc}")
            results.append({"entity": entity.name, "status": "FATAL_DQ", "error": str(exc)})
        except Exception as exc:  # noqa: BLE001
            log.error(f"[{entity.name}] failed: {exc}", exc_info=True)
            results.append({"entity": entity.name, "status": "FAILED", "error": str(exc)[:400]})
    return results


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Silver transformation pipeline")
    ap.add_argument("--env", default="local")
    ap.add_argument("--entity", action="append")
    a = ap.parse_args(argv)

    results = run(a.env, a.entity)
    print(f"\n{'entity':<16}{'status':<12}{'in':>9}{'out':>9}{'quar':>7}{'dupes':>8}")
    for r in results:
        print(f"{r['entity']:<16}{r['status']:<12}{r.get('rows_in',0):>9,}"
              f"{r.get('rows_out',0):>9,}{r.get('quarantined',0):>7,}"
              f"{r.get('duplicates_removed',0):>8,}")
    return 1 if any(r["status"] in ("FAILED", "FATAL_DQ") for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
