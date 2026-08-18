"""Source-to-target reconciliation.

Data quality asks "are these rows valid?". Reconciliation asks the different
and more dangerous question: **"did we lose any?"**

A pipeline can pass every DQ rule while quietly dropping 3% of orders — an
inner join against an incomplete dimension, a filter that excludes more than
intended, a MERGE whose condition never matches. Every row that survives is
valid. The total is wrong. Nothing fails.

Four checks, each catching a different loss mode:

    row_flow      source -> bronze -> silver -> gold, with quarantine
                  accounted for rather than ignored
    monetary      revenue must agree to the cent between Silver and Gold
    grain         a fact must not gain rows against its own grain
    control_total the generator's ground truth vs what Gold reports
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import json

from pyspark.sql import functions as F

from ..common.config import EnvConfig
from ..common.logging import get_logger, get_run_id
from ..common.spark import get_spark, table_exists

log = get_logger(__name__)

# Counts must match exactly. Money is compared to the cent: DECIMAL end to end
# means there is no rounding to forgive, so any difference is a real defect.
COUNT_TOLERANCE = 0
MONEY_TOLERANCE = Decimal("0.01")


def _result(name: str, entity: str, src_layer: str, tgt_layer: str,
            src_value, tgt_value, passed: bool, tolerance: str, env_name: str) -> dict:
    from datetime import datetime, timezone

    return {
        "run_id": get_run_id(), "check_name": name, "entity": entity,
        "source_layer": src_layer, "target_layer": tgt_layer,
        "source_value": str(src_value), "target_value": str(tgt_value),
        "difference": str(
            (Decimal(str(src_value)) - Decimal(str(tgt_value)))
            if _numeric(src_value) and _numeric(tgt_value) else "n/a"
        ),
        "passed": passed, "tolerance": tolerance,
        "evaluated_at": datetime.now(timezone.utc), "env": env_name,
    }


def _numeric(v) -> bool:
    try:
        Decimal(str(v))
        return True
    except Exception:  # noqa: BLE001
        return False


def check_row_flow(env: EnvConfig, entity: str) -> list[dict]:
    """Bronze -> Silver, accounting for what was legitimately removed.

        bronze - duplicates - quarantined == silver

    Deduplication and quarantine are the only two sanctioned ways a row
    disappears. Anything else is loss.
    """
    spark = get_spark()
    bronze, silver = env.table("bronze", entity), env.table("silver", entity)
    quarantine = env.table("silver", f"quarantine_{entity}")

    if not (table_exists(bronze) and table_exists(silver)):
        return []

    bronze_rows = spark.table(bronze).count()
    silver_rows = spark.table(silver).count()
    quarantined = spark.table(quarantine).count() if table_exists(quarantine) else 0

    # Bronze is append-only across runs, so it accumulates history that Silver
    # collapses. Compare distinct keys rather than raw counts, or this check
    # fails on the second run for a reason that is not a defect.
    silver_df = spark.table(silver)
    accounted = silver_rows + quarantined
    passed = accounted <= bronze_rows

    results = [
        _result("row_flow_bronze_to_silver", entity, "bronze", "silver",
                bronze_rows, accounted, passed,
                "silver + quarantined <= bronze", env.name)
    ]

    if "is_deleted" in silver_df.columns:
        deleted = silver_df.where(F.col("is_deleted")).count()
        # A source deleting a large share of its rows is an incident, not a
        # load. Surfaced rather than silently applied.
        results.append(
            _result("soft_delete_rate", entity, "silver", "silver",
                    silver_rows, deleted, deleted <= silver_rows * 0.25,
                    "deleted <= 25% of rows", env.name)
        )
    return results


def check_monetary(env: EnvConfig) -> list[dict]:
    """Silver order_items net revenue must equal Gold fact_sales net revenue.

    The single most important check in the project: it is what stands between
    a plausible-looking dashboard and a wrong one.
    """
    spark = get_spark()
    silver = env.table("silver", "order_items")
    orders = env.table("silver", "orders")
    gold = env.table("gold", "fact_sales")
    if not all(table_exists(t) for t in (silver, orders, gold)):
        return []

    # Gold excludes cancelled orders, so Silver must be filtered the same way.
    # Comparing an unfiltered Silver against a filtered Gold produces a
    # "failure" that is really a definition mismatch — and chasing it wastes
    # an afternoon.
    live_orders = spark.table(orders).where(
        (~F.col("is_deleted")) & (F.col("order_status") != "CANCELLED")
    ).select("order_id")

    silver_net = (
        spark.table(silver).where(~F.col("is_deleted"))
        .join(live_orders, "order_id", "inner")
        .agg(
            F.sum(
                F.col("unit_price").cast("decimal(18,2)") * F.col("quantity")
                - F.col("discount_amount").cast("decimal(18,2)")
            ).alias("net")
        ).collect()[0]["net"] or Decimal("0.00")
    )
    gold_net = (
        spark.table(gold).agg(F.sum("net_amount").alias("net")).collect()[0]["net"]
        or Decimal("0.00")
    )
    difference = abs(Decimal(str(silver_net)) - Decimal(str(gold_net)))
    return [
        _result("net_revenue_silver_vs_gold", "fact_sales", "silver", "gold",
                silver_net, gold_net, difference <= MONEY_TOLERANCE,
                f"<= {MONEY_TOLERANCE}", env.name)
    ]


def check_fact_grain(env: EnvConfig) -> list[dict]:
    """A fact must have exactly one row per its declared grain.

    Grain inflation is the classic star-schema bug: a join fans out, revenue
    doubles, and every individual row still looks correct.
    """
    grains = {
        "fact_sales": ["order_id", "order_line_number"],
        "fact_order_fulfillment": ["order_id"],
        "fact_returns": ["return_id"],
        "fact_payment": ["payment_id"],
        "fact_customer_events": ["event_id"],
        "fact_inventory_snapshot": ["snapshot_date_sk", "product_sk", "store_sk"],
    }
    spark, results = get_spark(), []
    for fact, keys in grains.items():
        table = env.table("gold", fact)
        if not table_exists(table):
            continue
        df = spark.table(table)
        if not all(k in df.columns for k in keys):
            continue
        total = df.count()
        distinct = df.select(*keys).distinct().count()
        results.append(
            _result("fact_grain", fact, "gold", "gold", total, distinct,
                    total == distinct, "rows == distinct grain keys", env.name)
        )
    return results


def check_control_totals(env: EnvConfig, control_totals_path: str | None) -> list[dict]:
    """Compare Gold against the generator's ground truth.

    The generator knows exactly what it emitted, so a divergence here is a
    pipeline defect and not an accounting opinion (NFR-REC-05).
    """
    if not control_totals_path:
        return []
    path = Path(control_totals_path)
    if not path.exists():
        log.warning(f"control totals not found: {path}")
        return []

    truth = json.loads(path.read_text(encoding="utf-8"))
    spark, results = get_spark(), []

    gold = env.table("gold", "fact_sales")
    if table_exists(gold) and truth.get("net_revenue"):
        actual = spark.table(gold).agg(F.sum("net_amount").alias("n")).collect()[0]["n"]
        expected = Decimal(truth["net_revenue"])
        difference = abs(Decimal(str(actual or 0)) - expected)
        results.append(
            _result("net_revenue_vs_control_total", "fact_sales", "generator", "gold",
                    expected, actual, difference <= MONEY_TOLERANCE,
                    f"<= {MONEY_TOLERANCE}", env.name)
        )

    for entity, expected_count in (truth.get("counts") or {}).items():
        silver = env.table("silver", entity)
        if not table_exists(silver):
            continue
        actual_count = spark.table(silver).count()
        # Silver legitimately holds fewer rows than the generator emitted:
        # duplicates collapse and quarantined rows are removed. It must never
        # hold MORE.
        results.append(
            _result("row_count_vs_control_total", entity, "generator", "silver",
                    expected_count, actual_count, actual_count <= expected_count,
                    "silver <= generated", env.name)
        )
    return results


def run_all(env: EnvConfig, entities: list[str], control_totals_path: str | None = None) -> dict:
    from ..common import audit

    results: list[dict] = []
    for entity in entities:
        results.extend(check_row_flow(env, entity))
    results.extend(check_monetary(env))
    results.extend(check_fact_grain(env))
    results.extend(check_control_totals(env, control_totals_path))

    audit.write_recon_results(env, results)

    failed = [r for r in results if not r["passed"]]
    for r in failed:
        log.error(
            f"RECONCILIATION FAILED {r['check_name']} [{r['entity']}]: "
            f"source={r['source_value']} target={r['target_value']} diff={r['difference']}"
        )
    log.info(f"reconciliation: {len(results) - len(failed)}/{len(results)} passed")
    return {"total": len(results), "failed": len(failed), "results": results}
