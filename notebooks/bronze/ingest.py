# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion
# MAGIC
# MAGIC Config-driven Auto Loader ingestion. Entities come from the task parameter so one notebook serves all four parallel task groups.
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
dbutils.widgets.text("entities", "")

env_name = dbutils.widgets.get("env")
entities = [e.strip() for e in dbutils.widgets.get("entities").split(",") if e.strip()]

print(f"env={env_name}")

# COMMAND ----------

from northpeak.ingestion import framework

results = framework.run(env_name, entities=entities or None)

failed = [r for r in results if r["status"] == "FAILED"]
summary = (
    f"{len(results) - len(failed)}/{len(results)} entities ingested; "
    f"{sum(r.get('row_count', 0) for r in results):,} rows"
)
print(summary)
for r in sorted(results, key=lambda x: x["entity"]):
    print(f"  {r['entity']:<16}{r['status']:<10}{r.get('row_count', 0):>10,}")

# Fail the task so the DAG stops rather than building Gold on a partial load.
if failed:
    raise RuntimeError(f"ingestion failed for: {[r['entity'] for r in failed]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC
# MAGIC Exit value is picked up by the Lakeflow Jobs UI and by any downstream
# MAGIC task using `dbutils.jobs.taskValues`.

dbutils.notebook.exit(summary)
