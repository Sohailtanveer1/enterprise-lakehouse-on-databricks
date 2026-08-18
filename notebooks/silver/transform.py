# Databricks notebook source
# MAGIC %md
# MAGIC # Silver transformation
# MAGIC
# MAGIC Debezium unwrap, standardise, deduplicate, data quality, CDC merge, SCD1/SCD2. Sequential and dependency-ordered.
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

from northpeak.transformations import silver

results = silver.run(env_name)

bad = [r for r in results if r["status"] in ("FAILED", "FATAL_DQ")]
summary = f"{len(results) - len(bad)}/{len(results)} entities transformed"
print(summary)
print(f"{'entity':<16}{'status':<12}{'in':>9}{'out':>9}{'quar':>7}{'dupes':>8}")
for r in results:
    print(f"{r['entity']:<16}{r['status']:<12}{r.get('rows_in', 0):>9,}"
          f"{r.get('rows_out', 0):>9,}{r.get('quarantined', 0):>7,}"
          f"{r.get('duplicates_removed', 0):>8,}")

if bad:
    raise RuntimeError(f"silver failed for: {[(r['entity'], r['status']) for r in bad]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC
# MAGIC Exit value is picked up by the Lakeflow Jobs UI and by any downstream
# MAGIC task using `dbutils.jobs.taskValues`.

dbutils.notebook.exit(summary)
