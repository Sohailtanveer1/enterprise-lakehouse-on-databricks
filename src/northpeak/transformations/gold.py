"""Gold pipeline runner — build the star schema and the aggregate marts.

    python -m northpeak.transformations.gold --env local

Dimensions land in Gold during Phase 5 (scd.py writes gold.dim_*), so this
stage owns dim_date, the six facts, and the four aggregates.

Order matters: dim_date and the conformed dimensions must exist before any
fact resolves a surrogate key against them.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

from ..common import audit
from ..common.config import EnvConfig, load_env
from ..common.logging import get_logger, log_context
from ..common.spark import ensure_namespaces, get_spark, table_exists
from . import dimensional as D

log = get_logger(__name__)

FACT_BUILDERS = {
    "fact_sales": D.build_fact_sales,
    "fact_order_fulfillment": D.build_fact_order_fulfillment,
    "fact_inventory_snapshot": D.build_fact_inventory_snapshot,
    "fact_returns": D.build_fact_returns,
    "fact_customer_events": D.build_fact_customer_events,
    "fact_payment": D.build_fact_payment,
}


# --------------------------------------------------------------- aggregates


def build_agg_daily_sales(env: EnvConfig) -> int:
    """date x region x category. Serves questions 1, 2, 3 and 13."""
    spark = get_spark()
    sales = spark.table(env.table("gold", "fact_sales"))
    dates = spark.table(env.table("gold", "dim_date"))
    customers = _dim(env, "dim_customers", ["customers_sk", "region"])
    categories = _dim(env, "dim_categories", ["categories_sk", "category_name"])

    df = (
        sales.join(dates, sales.order_date_sk == dates.date_sk, "left")
        .join(customers, sales.customer_sk == customers.customers_sk, "left")
        .join(categories, sales.category_sk == categories.categories_sk, "left")
        .groupBy("full_date", "year", "month_number", "region", "category_name")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
            F.sum("gross_amount").alias("gross_revenue"),
            F.sum("discount_amount").alias("discount_total"),
            F.sum("net_amount").alias("net_revenue"),
            # AOV is computed here, once, so every dashboard reads the same
            # number rather than each recomputing its own definition.
            (F.sum("net_amount") / F.countDistinct("order_id"))
            .cast("decimal(18,2)")
            .alias("avg_order_value"),
        )
    )
    return _write(env, df, "agg_daily_sales")


def build_agg_customer_lifetime(env: EnvConfig) -> int:
    """One row per customer. Serves question 5 (CLV)."""
    spark = get_spark()
    sales = spark.table(env.table("gold", "fact_sales"))
    dates = spark.table(env.table("gold", "dim_date"))
    customers = _dim(
        env,
        "dim_customers",
        ["customers_sk", "customer_id", "customer_segment", "region", "signup_date"],
        current_only=True,
    )

    per_customer = (
        sales.join(dates, sales.order_date_sk == dates.date_sk, "left")
        .groupBy("customer_sk")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("net_amount").alias("lifetime_value"),
            (F.sum("net_amount") / F.countDistinct("order_id"))
            .cast("decimal(18,2)")
            .alias("avg_order_value"),
            F.min("full_date").alias("first_order_date"),
            F.max("full_date").alias("last_order_date"),
        )
        .withColumn("days_since_last_order", F.datediff(F.current_date(), F.col("last_order_date")))
        .withColumn("is_repeat_customer", F.col("total_orders") > 1)
    )
    df = per_customer.join(
        customers, per_customer.customer_sk == customers.customers_sk, "left"
    ).drop("customers_sk")
    return _write(env, df, "agg_customer_lifetime")


def build_agg_customer_cohort(env: EnvConfig) -> int:
    """cohort_month x months_since_signup. Serves question 9 (retention)."""
    spark = get_spark()
    sales = spark.table(env.table("gold", "fact_sales"))
    dates = spark.table(env.table("gold", "dim_date"))
    customers = _dim(env, "dim_customers", ["customers_sk", "signup_date"], current_only=True)

    df = (
        sales.join(dates, sales.order_date_sk == dates.date_sk, "left")
        .join(customers, sales.customer_sk == customers.customers_sk, "left")
        .withColumn("cohort_month", F.date_format("signup_date", "yyyy-MM"))
        .withColumn("order_month", F.date_format("full_date", "yyyy-MM"))
        .withColumn(
            "months_since_signup",
            F.months_between(F.trunc("full_date", "month"), F.trunc("signup_date", "month")).cast(
                "int"
            ),
        )
        # Negative values mean an order predates the signup date - a data
        # problem, not a cohort. Excluded rather than silently skewing retention.
        .where(F.col("months_since_signup") >= 0)
        .groupBy("cohort_month", "months_since_signup")
        .agg(
            F.countDistinct("customer_sk").alias("active_customers"),
            F.countDistinct("order_id").alias("orders"),
            F.sum("net_amount").alias("net_revenue"),
        )
    )
    return _write(env, df, "agg_customer_cohort")


def build_agg_product_performance(env: EnvConfig) -> int:
    """date x product, with returns attached. Serves questions 3, 6 and 12."""
    spark = get_spark()
    sales = spark.table(env.table("gold", "fact_sales"))
    dates = spark.table(env.table("gold", "dim_date"))
    products = _dim(
        env,
        "dim_products",
        ["products_sk", "product_id", "product_name", "brand"],
        current_only=True,
    )

    base = (
        sales.join(dates, sales.order_date_sk == dates.date_sk, "left")
        .groupBy("product_sk", "full_date")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("net_amount").alias("net_revenue"),
            F.countDistinct("order_id").alias("orders"),
            F.sum(F.when(F.col("promotion_sk") != -1, F.col("net_amount")).otherwise(0)).alias(
                "promo_revenue"
            ),
        )
    )

    returns_table = env.table("gold", "fact_returns")
    if table_exists(returns_table):
        returns = (
            spark.table(returns_table)
            .join(dates, F.col("return_date_sk") == dates.date_sk, "left")
            .groupBy("product_sk", "full_date")
            .agg(F.sum("quantity_returned").alias("units_returned"))
        )
        base = base.join(returns, ["product_sk", "full_date"], "left")
    else:
        base = base.withColumn("units_returned", F.lit(0))

    df = (
        base.fillna({"units_returned": 0})
        .withColumn(
            "return_rate",
            F.when(
                F.col("units_sold") > 0, F.col("units_returned") / F.col("units_sold")
            ).otherwise(F.lit(0.0)),
        )
        .join(products, base.product_sk == products.products_sk, "left")
        .drop("products_sk")
    )
    return _write(env, df, "agg_product_performance")


AGGREGATE_BUILDERS = {
    "agg_daily_sales": build_agg_daily_sales,
    "agg_customer_lifetime": build_agg_customer_lifetime,
    "agg_customer_cohort": build_agg_customer_cohort,
    "agg_product_performance": build_agg_product_performance,
}


def _dim(env: EnvConfig, name: str, columns: list[str], current_only: bool = False):
    """Load a dimension, tolerating absence and missing columns.

    Aggregates should degrade rather than crash when a dimension has not been
    built yet — a partially-built Gold is more useful than none.
    """
    table = env.table("gold", name)
    if not table_exists(table):
        raise RuntimeError(f"required dimension missing: {table}")
    df = get_spark().table(table)
    if current_only and "is_current" in df.columns:
        df = df.where(F.col("is_current"))
    present = [c for c in columns if c in df.columns]
    return df.select(*present)


def _write(env: EnvConfig, df, name: str) -> int:
    target = env.table("gold", name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    count = get_spark().table(target).count()
    log.info(f"{name}: {count:,} rows -> {target}")
    return count


# ------------------------------------------------------------------- runner


def run(env_name: str, only: list[str] | None = None) -> list[dict]:
    env = load_env(env_name)
    ensure_namespaces(env)
    audit.ensure_audit_tables(env)
    results: list[dict] = []

    with log_context(layer="gold"):
        with audit.audited_task(env, "gold_dim_date", "dim_date", "gold") as rec:
            rec.rows_out = D.build_dim_date(env)
            results.append({"object": "dim_date", "rows": rec.rows_out, "status": "SUCCESS"})

        D.conform_dim_product(env)

        # Facts first, then aggregates: every aggregate reads a fact.
        for stage, builders in (("fact", FACT_BUILDERS), ("agg", AGGREGATE_BUILDERS)):
            for name, builder in builders.items():
                if only and name not in only:
                    continue
                try:
                    with audit.audited_task(env, f"gold_{stage}", name, "gold") as rec:
                        rec.rows_out = builder(env)
                        results.append({"object": name, "rows": rec.rows_out, "status": "SUCCESS"})
                except Exception as exc:
                    log.error(f"{name} failed: {exc}", exc_info=True)
                    results.append(
                        {"object": name, "rows": 0, "status": "FAILED", "error": str(exc)[:300]}
                    )
    return results


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Gold star-schema build")
    ap.add_argument("--env", default="local")
    ap.add_argument("--only", action="append", help="build one object; repeatable")
    a = ap.parse_args(argv)

    results = run(a.env, a.only)
    print(f"\n{'object':<30}{'status':<10}{'rows':>12}")
    for r in results:
        print(f"{r['object']:<30}{r['status']:<10}{r['rows']:>12,}")
    return 1 if any(r["status"] == "FAILED" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
