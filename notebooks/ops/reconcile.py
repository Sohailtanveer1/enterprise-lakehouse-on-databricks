# Databricks notebook source
# MAGIC %md
# MAGIC # Reconciliation
# MAGIC
# MAGIC Did we lose any rows? Compares source to Bronze to Silver to Gold, plus monetary totals and fact grain.
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
dbutils.widgets.text("control_totals_path", "")

env_name = dbutils.widgets.get("env")
control_totals = dbutils.widgets.get("control_totals_path") or None

print(f"env={env_name}")

# COMMAND ----------

from northpeak.common.config import list_entities, load_env
from northpeak.reconciliation import checks

env = load_env(env_name)
entities = [e.name for e in list_entities()]

outcome = checks.run_all(env, entities, control_totals)

summary = f"{outcome['total'] - outcome['failed']}/{outcome['total']} checks passed"
print(summary)
for r in outcome["results"]:
    mark = "ok  " if r["passed"] else "FAIL"
    print(f"  {mark} {r['check_name']:<34}{r['entity']:<22}"
          f"src={r['source_value']:>16}  tgt={r['target_value']:>16}")

# Reconciliation failure is a hard stop. Silent data loss that reaches a
# dashboard is worse than a failed pipeline that nobody can ignore.
if outcome["failed"]:
    raise RuntimeError(f"{outcome['failed']} reconciliation checks failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC
# MAGIC Exit value is picked up by the Lakeflow Jobs UI and by any downstream
# MAGIC task using `dbutils.jobs.taskValues`.

dbutils.notebook.exit(summary)
