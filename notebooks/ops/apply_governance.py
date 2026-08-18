# Databricks notebook source
# MAGIC %md
# MAGIC # Apply Unity Catalog governance
# MAGIC
# MAGIC Executes `sql/governance/*.sql` against the target catalog: column
# MAGIC masks, row filters, tags, comments and curated views.
# MAGIC
# MAGIC Run **after** Gold exists — masks and tags attach to tables, so they
# MAGIC cannot be applied to something that has not been built yet.
# MAGIC
# MAGIC Statements are applied individually and failures are collected rather
# MAGIC than aborting. On Free Edition several will fail by design:
# MAGIC `is_account_group_member()` references account-level groups that do
# MAGIC not exist there. Recording which ones failed, and why, is more useful
# MAGIC than a stack trace on the first one.

# COMMAND ----------

# MAGIC %pip install -q pyyaml pydantic
# MAGIC %restart_python

# COMMAND ----------

import re
import sys
from pathlib import Path

sys.path.insert(0, "../../src")

dbutils.widgets.text("env", "dev")
dbutils.widgets.dropdown("strict", "false", ["true", "false"])

env_name = dbutils.widgets.get("env")
strict = dbutils.widgets.get("strict") == "true"

from northpeak.common.config import load_env  # noqa: E402

env = load_env(env_name)
print(f"env={env_name} catalog={env.catalog} strict={strict}")

# COMMAND ----------


def split_statements(sql_text: str) -> list[str]:
    """Split on semicolons that actually terminate a statement.

    Naive `.split(";")` is wrong twice over: a semicolon inside a string
    literal splits one statement into two fragments, and a `--` comment
    containing a semicolon does the same. Both produce syntax errors that look
    like broken SQL when the splitter is what is broken.

    This walks the text tracking quote and comment state, so only a semicolon
    outside a literal terminates a statement.
    """
    statements: list[str] = []
    buffer: list[str] = []
    in_single = in_double = in_comment = False
    i, n = 0, len(sql_text)

    while i < n:
        char = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < n else ""

        if in_comment:
            if char == "\n":
                in_comment = False
                buffer.append(char)
            i += 1
            continue

        if not in_single and not in_double and char == "-" and nxt == "-":
            in_comment = True
            i += 2
            continue

        if char == "'" and not in_double:
            # '' is an escaped quote inside a literal, not a terminator.
            if in_single and nxt == "'":
                buffer.append("''")
                i += 2
                continue
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double

        if char == ";" and not in_single and not in_double:
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            i += 1
            continue

        buffer.append(char)
        i += 1

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements


def apply_file(path: Path) -> tuple[int, list[tuple[str, str]]]:
    text = path.read_text(encoding="utf-8").replace("${catalog}", env.catalog)
    applied, failures = 0, []
    for statement in split_statements(text):
        try:
            spark.sql(statement)
            applied += 1
        except Exception as exc:  # noqa: BLE001
            failures.append((statement.split("\n")[0][:90], str(exc)[:180]))
    return applied, failures


# COMMAND ----------

governance_dir = Path("../../sql/governance")
files = sorted(governance_dir.glob("*.sql"))

total_applied, all_failures = 0, []
for f in files:
    applied, failures = apply_file(f)
    total_applied += applied
    all_failures.extend(failures)
    print(f"{f.name}: {applied} applied, {len(failures)} failed")

# COMMAND ----------

if all_failures:
    print(f"\n{len(all_failures)} statements failed:\n")
    for statement, error in all_failures:
        print(f"  {statement}\n    -> {error}\n")

    # Expected on Free Edition: no account console means no account groups,
    # so every is_account_group_member() reference is unresolvable. That is a
    # platform limitation, not a defect in this code, and it is recorded in
    # SECURITY.md §2 as a stated simulation.
    group_related = [f for f in all_failures if "group" in f[1].lower()]
    if group_related:
        print(
            f"{len(group_related)} of these reference account-level groups. "
            "On Free Edition this is expected - see SECURITY.md section 2."
        )

summary = f"{total_applied} statements applied, {len(all_failures)} failed"
print(f"\n{summary}")

if strict and all_failures:
    raise RuntimeError(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC The payoff: "which columns contain PII, and are they masked?" is one
# MAGIC query rather than a meeting.

try:
    display(spark.sql(f"SELECT * FROM {env.catalog}.gold.v_pii_inventory ORDER BY classification"))
except Exception as exc:  # noqa: BLE001
    print(f"PII inventory unavailable: {exc}")

# COMMAND ----------

dbutils.notebook.exit(summary)
