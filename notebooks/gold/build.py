# Databricks notebook source
# MAGIC %md
# MAGIC # Gold star schema
# MAGIC
# MAGIC dim_date, six facts and four aggregate marts. Every fact resolves its dimension keys as-of the event time.
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

from northpeak.transformations import gold

results = gold.run(env_name)

failed = [r for r in results if r["status"] == "FAILED"]
summary = f"{len(results) - len(failed)}/{len(results)} gold objects built"
print(summary)
for r in results:
    print(f"  {r['object']:<30}{r['status']:<10}{r['rows']:>12,}")

if failed:
    raise RuntimeError(f"gold build failed for: {[r['object'] for r in failed]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC
# MAGIC Exit value is picked up by the Lakeflow Jobs UI and by any downstream
# MAGIC task using `dbutils.jobs.taskValues`.

dbutils.notebook.exit(summary)
