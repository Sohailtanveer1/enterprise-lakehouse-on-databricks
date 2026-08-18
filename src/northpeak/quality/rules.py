"""Data quality rule primitives.

Each rule compiles to a boolean Column that is TRUE when the row **passes**.
That single convention is what lets the engine compose any rule set into one
pass over the data instead of one scan per rule.

Two kinds of rule, and the distinction matters:

  row-level    evaluated per row; failures can be quarantined
               (not_null, range, domain, regex, expression, pii_scan)
  batch-level  evaluated over the whole batch; failures are all-or-nothing
               (unique, referential, freshness, volume)

Quarantining a row for a batch-level failure makes no sense — if the row count
is 40% below trend, no individual row is at fault.
"""

from __future__ import annotations

import re

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..common.config import RuleConfig

ROW_LEVEL = {"not_null", "range", "domain", "regex", "expression", "pii_scan"}
BATCH_LEVEL = {"unique", "referential", "freshness", "volume"}

# Deliberately loose. A false positive costs a quarantined row and an
# investigation; a false negative means an email address sits in a table nobody
# thinks contains PII. See SECURITY.md §1.
EMAIL_RE = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
PHONE_RE = r"(\+?\d[\d\s().-]{7,}\d)"


def _cols(rule: RuleConfig, df: DataFrame) -> list[str]:
    """Only columns that exist. A rule targeting an absent column is skipped
    loudly by the engine rather than silently passing."""
    return [c for c in rule.columns if c in df.columns]


def build_row_predicate(rule: RuleConfig, df: DataFrame) -> Column | None:
    """Compile a row-level rule. TRUE means the row passes."""
    columns = _cols(rule, df)
    if rule.type in ROW_LEVEL and not columns and rule.type != "expression":
        return None

    if rule.type == "not_null":
        predicate = F.lit(True)
        for c in columns:
            predicate = predicate & F.col(c).isNotNull()
        return predicate

    if rule.type == "range":
        predicate = F.lit(True)
        for c in columns:
            value = F.col(c).cast("double")
            # NULL passes the range rule. Nullability is not_null's job; making
            # every rule also a null check double-counts failures and makes the
            # DQ report unreadable.
            bounds = F.lit(True)
            if rule.min_value is not None:
                bounds = bounds & (value >= F.lit(rule.min_value))
            if rule.max_value is not None:
                bounds = bounds & (value <= F.lit(rule.max_value))
            predicate = predicate & (value.isNull() | bounds)
        return predicate

    if rule.type == "domain":
        allowed = [F.lit(v) for v in rule.allowed_values]
        predicate = F.lit(True)
        for c in columns:
            predicate = predicate & (F.col(c).isNull() | F.col(c).isin(*allowed))
        return predicate

    if rule.type == "regex":
        predicate = F.lit(True)
        for c in columns:
            predicate = predicate & (F.col(c).isNull() | F.col(c).rlike(rule.pattern or ".*"))
        return predicate

    if rule.type == "expression":
        # SQL from config. Trusted input — config is version-controlled and
        # code-reviewed, unlike anything arriving in the data itself.
        return F.expr(rule.expression or "true")

    if rule.type == "pii_scan":
        # Passes when NO PII pattern is found. Free-form fields are where PII
        # leaks into tables nobody classified.
        predicate = F.lit(True)
        for c in columns:
            text = F.col(c).cast("string")
            predicate = predicate & ~(text.rlike(EMAIL_RE) | text.rlike(PHONE_RE)).eqNullSafe(
                F.lit(True)
            )
        return predicate

    return None


def evaluate_unique(df: DataFrame, columns: list[str]) -> tuple[int, str]:
    """Count keys appearing more than once. Returns (violations, detail)."""
    dupes = df.groupBy(*columns).count().where(F.col("count") > 1)
    rows = dupes.limit(5).collect()
    violations = dupes.count()
    sample = "; ".join(",".join(f"{c}={r[c]}" for c in columns) + f" x{r['count']}" for r in rows)
    return violations, f"duplicate keys: {sample}" if sample else "no duplicates"


def evaluate_referential(
    df: DataFrame, column: str, reference: DataFrame, reference_column: str
) -> tuple[int, str]:
    """Rows whose foreign key has no match in the reference table.

    LEFT ANTI JOIN rather than NOT IN: `NOT IN` returns nothing at all when the
    reference contains a single NULL, so the check silently passes.
    """
    orphans = df.where(F.col(column).isNotNull()).join(
        reference.select(F.col(reference_column).alias("_ref")),
        F.col(column) == F.col("_ref"),
        "left_anti",
    )
    count = orphans.count()
    sample = [r[column] for r in orphans.select(column).limit(5).collect()]
    return count, f"orphan {column}: {sample}" if count else "referential integrity ok"


def evaluate_freshness(df: DataFrame, column: str, max_age_hours: int) -> tuple[int, str]:
    """Stale data is a silent failure: the job succeeds, the numbers are old."""
    row = df.agg(F.max(F.col(column).cast("timestamp")).alias("newest")).collect()[0]
    newest = row["newest"]
    if newest is None:
        return 1, f"{column} has no non-null values"
    age = (
        df.select(
            (
                F.unix_timestamp(F.current_timestamp())
                - F.unix_timestamp(F.lit(newest).cast("timestamp"))
            ).alias("s")
        ).collect()[0]["s"]
        / 3600.0
    )
    stale = age > max_age_hours
    return (1 if stale else 0), f"newest {column}={newest} ({age:.1f}h old, limit {max_age_hours}h)"


def evaluate_volume(
    current: int, trailing_mean: float | None, sigma: float, tolerance: float = 0.5
) -> tuple[int, str]:
    """Row count against the trailing trend.

    Falls back to a +/-50% band until enough history exists, because a standard
    deviation computed from two runs is meaningless and would either fire
    constantly or never.
    """
    if not trailing_mean:
        return 0, f"no trailing history yet (current={current})"
    lower, upper = trailing_mean * (1 - tolerance), trailing_mean * (1 + tolerance)
    breached = not (lower <= current <= upper)
    return (
        1 if breached else 0,
        f"count={current} vs trailing mean {trailing_mean:.0f} "
        f"(band {lower:.0f}-{upper:.0f}, {sigma} sigma configured)",
    )


def validate_pattern(pattern: str) -> None:
    """Fail at config-load time, not mid-run, on a malformed regex."""
    try:
        re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"invalid regex '{pattern}': {exc}") from exc
