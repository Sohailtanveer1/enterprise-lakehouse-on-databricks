"""Data quality engine — evaluation and severity routing.

Three severities, deliberately (ARCHITECTURE.md §8):

    WARN   record it, let the row through with a flag
    ERROR  quarantine the row, continue the run
    FATAL  stop the task, publish nothing

Failing a whole load because 12 rows out of 400,000 have a null phone number is
how pipelines get bypassed by frustrated humans. Failing nothing is how bad
data reaches the CFO.

All row-level rules are evaluated in **one pass**. The naive implementation
filters the DataFrame once per rule; with 10 rules on the events table that is
10 full scans, and it is the single easiest way to make a DQ framework the
slowest part of the pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..common.audit import dq_row
from ..common.config import EnvConfig, QualityConfig, RuleConfig, Severity
from ..common.logging import get_logger
from ..common.spark import get_spark, table_exists
from . import rules as R

log = get_logger(__name__)

FLAG_PREFIX = "_dq_"
WARNINGS_COL = "_dq_warnings"
FAILED_RULES_COL = "_dq_failed_rules"


class FatalDataQualityError(RuntimeError):
    """Raised on a FATAL rule breach. Stops the task; nothing is published."""


@dataclass
class QualityOutcome:
    valid: DataFrame
    quarantined: DataFrame
    results: list[dict]
    rows_in: int
    rows_valid: int
    rows_quarantined: int

    @property
    def pass_rate(self) -> float:
        return round(self.rows_valid / self.rows_in, 6) if self.rows_in else 1.0


def evaluate(
    df: DataFrame,
    quality: QualityConfig,
    env: EnvConfig,
    layer: str = "silver",
    trailing_mean: float | None = None,
) -> QualityOutcome:
    """Apply every rule, split valid from quarantined, record results."""
    entity = quality.entity
    results: list[dict] = []
    rows_in = df.count()

    if not quality.rules or rows_in == 0:
        empty = df.limit(0)
        return QualityOutcome(df, empty, results, rows_in, rows_in, 0)

    # ---- row-level: one pass, one flag column per rule ---------------------
    row_rules: list[RuleConfig] = []
    flagged = df
    for rule in quality.rules:
        if rule.type not in R.ROW_LEVEL:
            continue
        if rule.type == "regex" and rule.pattern:
            R.validate_pattern(rule.pattern)
        predicate = R.build_row_predicate(rule, df)
        if predicate is None:
            # A rule that cannot bind must never be reported as passing.
            log.warning(f"[{entity}] rule '{rule.name}' skipped: columns absent")
            results.append(
                dq_row(
                    entity,
                    layer,
                    rule.name,
                    rule.type,
                    rule.severity.value,
                    0,
                    0,
                    False,
                    "SKIPPED: target columns absent",
                    env.name,
                )
            )
            continue
        flagged = flagged.withColumn(f"{FLAG_PREFIX}{rule.name}", predicate)
        row_rules.append(rule)

    if row_rules:
        # Cache: the flag columns are consumed by the aggregate below and again
        # by the valid/quarantine split. Without this the predicates are
        # recomputed, which for a regex over the events table is expensive.
        flagged = flagged.cache()

        counts = flagged.agg(
            *[
                F.sum(F.when(~F.col(f"{FLAG_PREFIX}{r.name}"), 1).otherwise(0)).alias(r.name)
                for r in row_rules
            ]
        ).collect()[0]

        for rule in row_rules:
            failed = int(counts[rule.name] or 0)
            results.append(
                dq_row(
                    entity,
                    layer,
                    rule.name,
                    rule.type,
                    rule.severity.value,
                    rows_in,
                    failed,
                    failed == 0,
                    f"{failed}/{rows_in} rows failed",
                    env.name,
                )
            )
            if rule.severity is Severity.FATAL and failed:
                raise FatalDataQualityError(
                    f"[{entity}] FATAL rule '{rule.name}': {failed}/{rows_in} rows failed"
                )

    # ---- batch-level -------------------------------------------------------
    for rule in quality.rules:
        if rule.type not in R.BATCH_LEVEL:
            continue
        violations, detail = _evaluate_batch_rule(rule, df, env, rows_in, trailing_mean)
        results.append(
            dq_row(
                entity,
                layer,
                rule.name,
                rule.type,
                rule.severity.value,
                rows_in,
                violations,
                violations == 0,
                detail,
                env.name,
            )
        )
        if rule.severity is Severity.FATAL and violations:
            raise FatalDataQualityError(f"[{entity}] FATAL rule '{rule.name}': {detail}")

    # ---- split -------------------------------------------------------------
    error_rules = [r for r in row_rules if r.severity is Severity.ERROR]
    warn_rules = [r for r in row_rules if r.severity is Severity.WARN]

    if warn_rules:
        flagged = flagged.withColumn(
            WARNINGS_COL,
            F.array_compact(
                F.array(
                    *[F.when(~F.col(f"{FLAG_PREFIX}{r.name}"), F.lit(r.name)) for r in warn_rules]
                )
            ),
        )
    else:
        flagged = flagged.withColumn(WARNINGS_COL, F.array().cast("array<string>"))

    if error_rules:
        passes_all = F.lit(True)
        for rule in error_rules:
            passes_all = passes_all & F.col(f"{FLAG_PREFIX}{rule.name}")
        flagged = flagged.withColumn(
            FAILED_RULES_COL,
            F.array_compact(
                F.array(
                    *[F.when(~F.col(f"{FLAG_PREFIX}{r.name}"), F.lit(r.name)) for r in error_rules]
                )
            ),
        )
        valid = flagged.where(passes_all)
        quarantined = flagged.where(~passes_all)
    else:
        flagged = flagged.withColumn(FAILED_RULES_COL, F.array().cast("array<string>"))
        valid, quarantined = flagged, flagged.limit(0)

    flag_columns = [c for c in valid.columns if c.startswith(FLAG_PREFIX)]
    valid = valid.drop(*flag_columns, FAILED_RULES_COL)

    rows_quarantined = quarantined.count()
    rows_valid = rows_in - rows_quarantined

    # Overall gate. Individually-tolerable rules can still add up to a batch
    # that should not be published (NFR-DQ-10).
    pass_rate = rows_valid / rows_in if rows_in else 1.0
    if pass_rate < env.dq_fail_threshold:
        raise FatalDataQualityError(
            f"[{entity}] batch pass rate {pass_rate:.1%} below "
            f"fail threshold {env.dq_fail_threshold:.0%}"
        )
    if pass_rate < env.dq_warn_threshold:
        log.warning(f"[{entity}] pass rate {pass_rate:.1%} below warn threshold")

    return QualityOutcome(valid, quarantined, results, rows_in, rows_valid, rows_quarantined)


def _evaluate_batch_rule(
    rule: RuleConfig,
    df: DataFrame,
    env: EnvConfig,
    rows_in: int,
    trailing_mean: float | None,
) -> tuple[int, str]:
    if rule.type == "unique":
        columns = [c for c in rule.columns if c in df.columns]
        if not columns:
            return 0, "SKIPPED: columns absent"
        return R.evaluate_unique(df, columns)

    if rule.type == "referential":
        table = rule.reference_table or ""
        resolved = table if "." in table else env.table("silver", table)
        if not table_exists(resolved):
            # Not a pass. On a first run the reference genuinely does not exist
            # yet, and reporting that as success would hide a real break later.
            return 0, f"SKIPPED: reference table {resolved} does not exist yet"
        reference = get_spark().table(resolved)
        return R.evaluate_referential(
            df, rule.columns[0], reference, rule.reference_column or rule.columns[0]
        )

    if rule.type == "freshness":
        column = rule.columns[0]
        if column not in df.columns:
            return 1, f"SKIPPED: {column} absent"
        return R.evaluate_freshness(df, column, rule.max_age_hours or 26)

    if rule.type == "volume":
        return R.evaluate_volume(rows_in, trailing_mean, env.volume_anomaly_sigma)

    return 0, "unknown batch rule type"


def write_quarantine(outcome: QualityOutcome, env: EnvConfig, entity: str) -> None:
    """Persist quarantined rows with the rules they broke, so they can be
    fixed and replayed rather than lost."""
    if outcome.rows_quarantined == 0:
        return
    target = env.table("silver", f"quarantine_{entity}")
    (
        outcome.quarantined.withColumn("_quarantined_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target)
    )
    log.warning(f"[{entity}] quarantined {outcome.rows_quarantined} rows -> {target}")


def trailing_mean_rows(env: EnvConfig, entity: str, days: int = 7) -> float | None:
    """Mean rows_out for this entity over recent successful runs."""
    audit_table = env.table("audit", "pipeline_run_audit")
    if not table_exists(audit_table):
        return None
    row = (
        get_spark()
        .table(audit_table)
        .where(
            (F.col("entity") == entity)
            & (F.col("status") == "SUCCESS")
            & (F.col("start_ts") >= F.date_sub(F.current_timestamp(), days))
        )
        .agg(F.avg("rows_out").alias("m"))
        .collect()[0]
    )
    return float(row["m"]) if row["m"] else None
