"""Gold dimensional model — dimensions, facts and aggregates.

Dimensions are already versioned by Phase 5 (scd.py writes `gold.dim_*`). This
module owns the date dimension, the fact builders, and the aggregate marts.

The one thing that makes or breaks this layer is how facts resolve dimension
keys. Every fact joins **as of its own event time**, never on `is_current`.
Joining on is_current attributes a January order to the customer's present
region, silently restating history every time somebody moves — and the report
still runs, so nobody notices.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..common.config import EnvConfig
from ..common.keys import date_key, surrogate_key
from ..common.logging import get_logger
from ..common.spark import get_spark, table_exists
from .scd import as_of_join

log = get_logger(__name__)

# Money is DECIMAL end to end. DOUBLE accumulates cent-level drift over
# millions of rows, and "reconciles to within a few cents" is not
# reconciliation (NFR-REC-03).
MONEY = "decimal(18,2)"


def _dec(column: str):
    return F.col(column).cast(MONEY)


# ------------------------------------------------------------------ dim_date


def build_dim_date(env: EnvConfig, start: str = "2023-01-01", end: str = "2028-12-31") -> int:
    """Generate the date dimension.

    Pre-generated rather than derived from the facts: a date with no orders
    still needs a row, or a time-series report silently skips it and a zero
    day looks like a missing day.
    """
    spark = get_spark()
    target = env.table("gold", "dim_date")

    df = (
        spark.sql(
            f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS full_date"
        )
        .withColumn("date_sk", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("month_number", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
        # Simplified: US federal holidays would need a calendar table. Flagged
        # here rather than silently wrong.
        .withColumn("is_holiday", F.lit(False))
        .withColumn(
            "fiscal_period",
            F.concat_ws("-", F.year("full_date"), F.lpad(F.month("full_date"), 2, "0")),
        )
        .select(
            "date_sk",
            "full_date",
            "day_of_week",
            "day_name",
            "day_of_month",
            "week_of_year",
            "month_number",
            "month_name",
            "quarter",
            "year",
            "is_weekend",
            "is_holiday",
            "fiscal_period",
        )
    )
    df.write.format("delta").mode("overwrite").saveAsTable(target)
    count = df.count()
    log.info(f"dim_date: {count} rows -> {target}")
    return count


def conform_dim_product(env: EnvConfig) -> None:
    """Attach category_sk to dim_product so facts need one join fewer."""
    spark = get_spark()
    product, category = env.table("gold", "dim_products"), env.table("gold", "dim_categories")
    if not (table_exists(product) and table_exists(category)):
        return
    enriched = (
        spark.table(product)
        .alias("p")
        .join(
            spark.table(category).select(
                F.col("category_id").alias("_ck"), F.col("categories_sk").alias("category_sk")
            ),
            F.col("p.category_id") == F.col("_ck"),
            "left",
        )
        .drop("_ck")
    )
    enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        product
    )


# -------------------------------------------------------------------- facts


def build_fact_sales(env: EnvConfig) -> int:
    """Transaction fact. Grain: one line of one order.

    Merges what the brief called fact_sales and fact_order_items. A separate
    header fact would be nothing but a SUM of its lines, and two tables holding
    the same number eventually disagree (DATA_MODEL.md §3).
    """
    spark = get_spark()
    items = spark.table(env.table("silver", "order_items")).where(~F.col("is_deleted"))
    orders = spark.table(env.table("silver", "orders")).where(~F.col("is_deleted"))

    df = items.alias("i").join(
        orders.select(
            "order_id",
            "customer_id",
            "order_date",
            "order_status",
            "payment_status",
            "store_id",
            "promotion_id",
        ).alias("o"),
        "order_id",
        "inner",
    )

    df = (
        df.withColumn("gross_amount", _dec("unit_price") * F.col("quantity"))
        .withColumn("discount_amount", _dec("discount_amount"))
        .withColumn("tax_amount", _dec("tax_amount"))
        # Net excludes tax by definition. Tax is a liability, not revenue —
        # the mistake the DQ rules and this cast exist to prevent.
        .withColumn("net_amount", F.col("gross_amount") - F.col("discount_amount"))
        .withColumn("order_date_sk", date_key("order_date"))
    )

    df = _resolve_dimension_keys(env, df, "order_date")

    fact = df.withColumn("sales_sk", surrogate_key("order_id", "order_line_number")).select(
        "sales_sk",
        "order_id",
        "order_line_number",
        "order_date_sk",
        "customer_sk",
        "product_sk",
        "category_sk",
        "store_sk",
        "promotion_sk",
        "quantity",
        F.col("unit_price").cast(MONEY).alias("unit_price"),
        "gross_amount",
        "discount_amount",
        "tax_amount",
        "net_amount",
        "order_status",
        "payment_status",
    )
    return _write_fact(env, fact, "fact_sales", cluster_by=["order_date_sk", "product_sk"])


def build_fact_order_fulfillment(env: EnvConfig) -> int:
    """Accumulating snapshot. Grain: one order, updated as milestones occur.

    Carries the timestamps that genuinely cannot be derived from the lines,
    which is what answers business question 11. hours_to_ship is
    semi-additive: average it, never sum it.
    """
    spark = get_spark()
    orders = spark.table(env.table("silver", "orders")).where(~F.col("is_deleted"))
    items = spark.table(env.table("silver", "order_items")).where(~F.col("is_deleted"))
    shipments = (
        spark.table(env.table("silver", "shipments")).where(~F.col("is_deleted"))
        if table_exists(env.table("silver", "shipments"))
        else None
    )

    rollup = items.groupBy("order_id").agg(
        F.count("*").alias("order_line_count"),
        F.sum(_dec("unit_price") * F.col("quantity") - _dec("discount_amount"))
        .cast(MONEY)
        .alias("order_net_amount"),
    )

    df = orders.alias("o").join(rollup, "order_id", "left")
    if shipments is not None:
        ship = shipments.groupBy("order_id").agg(
            F.min("shipped_at").alias("shipped_at"),
            F.max("delivered_at").alias("delivered_at"),
        )
        df = df.join(ship, "order_id", "left")
    else:
        df = df.withColumn("shipped_at", F.lit(None).cast("timestamp")).withColumn(
            "delivered_at", F.lit(None).cast("timestamp")
        )

    df = (
        df.withColumn("placed_at", F.col("order_date").cast("timestamp"))
        .withColumn(
            "cancelled_at",
            F.when(F.col("order_status") == "CANCELLED", F.col("_silver_updated_at")).otherwise(
                F.lit(None).cast("timestamp")
            ),
        )
        .withColumn(
            "hours_to_ship", (F.unix_timestamp("shipped_at") - F.unix_timestamp("placed_at")) / 3600
        )
        .withColumn(
            "hours_to_deliver",
            (F.unix_timestamp("delivered_at") - F.unix_timestamp("placed_at")) / 3600,
        )
        .withColumn("order_date_sk", date_key("order_date"))
        .withColumn("final_status", F.col("order_status"))
    )
    df = _resolve_dimension_keys(env, df, "order_date", need=("customer", "store"))

    fact = df.withColumn("order_fulfillment_sk", surrogate_key("order_id")).select(
        "order_fulfillment_sk",
        "order_id",
        "order_date_sk",
        "customer_sk",
        "store_sk",
        "placed_at",
        "shipped_at",
        "delivered_at",
        "cancelled_at",
        F.col("hours_to_ship").cast("int").alias("hours_to_ship"),
        F.col("hours_to_deliver").cast("int").alias("hours_to_deliver"),
        "order_line_count",
        "order_net_amount",
        "final_status",
    )
    return _write_fact(env, fact, "fact_order_fulfillment", cluster_by=["order_date_sk"])


def build_fact_inventory_snapshot(env: EnvConfig) -> int:
    """Periodic snapshot. Grain: product x location x day.

    quantity_on_hand is NON-ADDITIVE over time. Summing 30 daily snapshots of
    "100 in stock" yields 3,000 units that never existed. It is additive across
    products on one day, and averaged across days.
    """
    spark = get_spark()
    source = env.table("silver", "inventory")
    if not table_exists(source):
        return 0
    df = spark.table(source).where(~F.col("is_deleted"))
    df = (
        df.withColumn("snapshot_date_sk", date_key("snapshot_date"))
        .withColumn("is_stockout", F.col("quantity_available") <= 0)
        .withColumn("is_below_reorder", F.col("quantity_available") < F.col("reorder_point"))
        .withColumnRenamed("location_id", "store_id")
    )
    df = _resolve_dimension_keys(env, df, "snapshot_date", need=("product", "store"))

    fact = df.withColumn(
        "inventory_sk", surrogate_key("snapshot_date", "product_id", "store_id")
    ).select(
        "inventory_sk",
        "snapshot_date_sk",
        "product_sk",
        "store_sk",
        "quantity_on_hand",
        "quantity_reserved",
        "quantity_available",
        "reorder_point",
        "is_stockout",
        "is_below_reorder",
    )
    return _write_fact(env, fact, "fact_inventory_snapshot", partition_by=["snapshot_date_sk"])


def build_fact_returns(env: EnvConfig) -> int:
    """Transaction fact. Grain: one returned product line."""
    spark = get_spark()
    source = env.table("silver", "returns")
    if not table_exists(source):
        return 0
    returns = spark.table(source).where(~F.col("is_deleted"))
    orders = spark.table(env.table("silver", "orders")).select(
        "order_id", "customer_id", F.col("order_date").alias("original_order_date")
    )
    df = returns.join(orders, "order_id", "left")
    df = (
        df.withColumn("return_date_sk", date_key("return_date"))
        .withColumn("original_order_date_sk", date_key("original_order_date"))
        .withColumn("days_to_return", F.datediff("return_date", "original_order_date"))
        .withColumnRenamed("quantity", "quantity_returned")
    )
    df = _resolve_dimension_keys(env, df, "return_date", need=("customer", "product"))

    fact = df.withColumn("return_sk", surrogate_key("return_id")).select(
        "return_sk",
        "return_id",
        "return_date_sk",
        "original_order_date_sk",
        "product_sk",
        "customer_sk",
        "quantity_returned",
        F.col("refund_amount").cast(MONEY).alias("refund_amount"),
        "return_reason",
        "days_to_return",
    )
    return _write_fact(env, fact, "fact_returns", cluster_by=["return_date_sk"])


def build_fact_customer_events(env: EnvConfig) -> int:
    """Transaction fact. Grain: one event. Highest-volume fact."""
    spark = get_spark()
    source = env.table("silver", "events")
    if not table_exists(source):
        return 0
    df = spark.table(source).where(~F.col("is_deleted"))
    df = df.withColumn("event_date_sk", date_key("event_time"))
    df = _resolve_dimension_keys(env, df, "event_time", need=("customer", "product"))

    fact = df.withColumn("event_sk", surrogate_key("event_id")).select(
        "event_sk",
        "event_id",
        "event_date_sk",
        "customer_sk",
        "product_sk",
        "session_id",
        "event_time",
        "event_type",
        "channel",
        "device_type",
    )
    return _write_fact(env, fact, "fact_customer_events", partition_by=["event_date_sk"])


def build_fact_payment(env: EnvConfig) -> int:
    """Transaction fact. Grain: one payment attempt.

    Attempts are preserved rather than collapsed to the successful one —
    payment success rate is meaningless without the failures.
    """
    spark = get_spark()
    source = env.table("silver", "payments")
    if not table_exists(source):
        return 0
    payments = spark.table(source).where(~F.col("is_deleted"))
    orders = spark.table(env.table("silver", "orders")).select(
        "order_id", "customer_id", "order_date"
    )
    df = payments.join(orders, "order_id", "left")
    df = df.withColumn("payment_date_sk", date_key("paid_at")).withColumn(
        "is_successful", F.col("payment_status") == "CAPTURED"
    )
    df = _resolve_dimension_keys(env, df, "order_date", need=("customer",))

    fact = df.withColumn("payment_sk", surrogate_key("payment_id")).select(
        "payment_sk",
        "payment_id",
        "payment_date_sk",
        "customer_sk",
        "order_id",
        "payment_method",
        F.col("payment_amount").cast(MONEY).alias("payment_amount"),
        "payment_status",
        "attempt_number",
        "is_successful",
    )
    return _write_fact(env, fact, "fact_payment", cluster_by=["payment_date_sk"])


# ------------------------------------------------------------------ helpers


_DIM_SPEC = {
    "customer": ("dim_customers", "customer_id", "customers_sk", "customer_sk"),
    "product": ("dim_products", "product_id", "products_sk", "product_sk"),
    "store": ("dim_stores", "store_id", "stores_sk", "store_sk"),
    "category": ("dim_categories", "category_id", "categories_sk", "category_sk"),
    "promotion": ("dim_promotions", "promotion_id", "promotions_sk", "promotion_sk"),
}
_SCD2_DIMS = {"customer", "product", "store"}


def _resolve_dimension_keys(
    env: EnvConfig,
    df: DataFrame,
    event_time_column: str,
    need: tuple[str, ...] = ("customer", "product", "category", "store", "promotion"),
) -> DataFrame:
    """Attach surrogate keys, as-of for SCD2 dimensions.

    Missing dimensions resolve to -1 rather than dropping the fact row. Losing
    revenue because a dimension has not loaded yet is a far worse failure than
    an Unknown member on a report.
    """
    out = df
    for name in need:
        table_name, business_key, source_sk, target_sk = _DIM_SPEC[name]
        table = env.table("gold", table_name)

        if not table_exists(table) or business_key not in out.columns:
            out = out.withColumn(target_sk, F.lit(-1).cast("bigint"))
            continue

        dimension = get_spark().table(table)
        if source_sk not in dimension.columns:
            out = out.withColumn(target_sk, F.lit(-1).cast("bigint"))
            continue
        dimension = dimension.withColumnRenamed(source_sk, target_sk)

        if name in _SCD2_DIMS and "effective_start_ts" in dimension.columns:
            out = as_of_join(out, dimension, business_key, event_time_column, target_sk)
        else:
            out = out.join(
                dimension.select(F.col(business_key).alias("_bk"), F.col(target_sk)),
                out[business_key] == F.col("_bk"),
                "left",
            ).drop("_bk")

        out = out.withColumn(target_sk, F.coalesce(F.col(target_sk), F.lit(-1).cast("bigint")))
    return out


def _write_fact(
    env: EnvConfig,
    df: DataFrame,
    name: str,
    partition_by: list[str] | None = None,
    cluster_by: list[str] | None = None,
) -> int:
    """Full overwrite.

    Facts are a pure function of Silver, so a rebuild is the simplest thing
    that satisfies NFR-IDEM-03 - rerunning reproduces byte-identical output.
    Incremental merge via Change Data Feed is the Phase 13 optimisation, and
    only worth it once a rebuild stops fitting the window.
    """
    target = env.table("gold", name)
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(target)

    if cluster_by and not partition_by:
        get_spark().sql(f"ALTER TABLE {target} CLUSTER BY ({', '.join(cluster_by)})")

    count = get_spark().table(target).count()
    log.info(f"{name}: {count:,} rows -> {target}")
    return count
