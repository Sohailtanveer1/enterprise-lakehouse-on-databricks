# Databricks notebook source
# MAGIC %md
# MAGIC # Table maintenance
# MAGIC
# MAGIC OPTIMIZE and VACUUM across Silver and Gold. Runs regardless of the reconciliation outcome - the data is already written.
# MAGIC
# MAGIC Orchestration only. All logic lives in `src/northpeak` so it is
# MAGIC unit-testable and runs identically in the spark-dev container.

# COMMAND ----------

# MAGIC %pip install -q pyyaml pydantic
# MAGIC %restart_python

# COMMAND ----------

import sys

# The bundle deploys the repo alongside the notebooks, so src/ is two levels up.
sys.path.insert(0, "../../src")

dbutils.widgets.text("env", "dev")

env_name = dbutils.widgets.get("env")

print(f"env={env_name}")

# COMMAND ----------

from northpeak.common.config import list_entities, load_env
from northpeak.common.spark import get_spark, table_exists

env = load_env(env_name)
spark = get_spark()

targets = [env.table("silver", e.name) for e in list_entities()]
targets += [
    env.table("gold", t) for t in [
        "dim_date", "fact_sales", "fact_order_fulfillment",
        "fact_inventory_snapshot", "fact_returns", "fact_customer_events",
        "fact_payment", "agg_daily_sales", "agg_customer_lifetime",
        "agg_customer_cohort", "agg_product_performance",
    ]
]

optimized, skipped = 0, 0
for table in targets:
    if not table_exists(table):
        skipped += 1
        continue
    spark.sql(f"OPTIMIZE {table}")
    # 7 days is the floor, not a tuning knob. Vacuuming more aggressively
    # breaks time travel and can delete files a concurrent reader still needs.
    spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
    optimized += 1

summary = f"optimized {optimized} tables, skipped {skipped} absent"
print(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC
# MAGIC Exit value is picked up by the Lakeflow Jobs UI and by any downstream
# MAGIC task using `dbutils.jobs.taskValues`.

dbutils.notebook.exit(summary)
