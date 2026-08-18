"""Surrogate and hash key generation.

The single most consequential decision in this module is ADR-09: surrogate keys
are **deterministic hashes**, never identity columns.

Identity columns are assigned in write order. Rebuild the warehouse and every
key changes, which breaks idempotency (NFR-IDEM-01), breaks any downstream
comparison, and makes cross-environment reconciliation impossible. A hash of
the business key plus the version start is reproducible on any machine, in any
order, with no coordination.

Cost of the choice: collisions are possible. At ~2.4M customers with a few
versions each the 64-bit collision probability is around 1e-7 — but "unlikely"
is not "handled", so a FATAL uniqueness rule runs on every surrogate key
column on every load. See config/quality/*.yaml.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Separator chosen to be absent from every business key in this domain.
# Without one, ('AB', 'C') and ('A', 'BC') hash identically — a real collision
# class that has nothing to do with hash strength.
SEP = ""

# Sentinel for the open end of an SCD2 version. A NULL end date forces every
# as-of join to write `(end IS NULL OR d < end)`, which is easy to forget and
# silently wrong when forgotten. A far-future timestamp keeps the predicate a
# simple half-open range.
SCD2_MAX_TS = "9999-12-31 23:59:59"


def _norm(columns: list[str]) -> list[Column]:
    """Normalise before hashing.

    Two rows that differ only by trailing whitespace or NULL-vs-empty are the
    same business entity. Hashing them differently produces duplicate dimension
    members that are invisible until someone counts customers twice.
    """
    return [F.coalesce(F.trim(F.col(c).cast("string")), F.lit("␀")) for c in columns]


def surrogate_key(*columns: str) -> Column:
    """SCD1 / non-versioned surrogate key: hash of the business key alone."""
    if not columns:
        raise ValueError("surrogate_key needs at least one column")
    return F.xxhash64(F.concat_ws(SEP, *_norm(list(columns))))


def versioned_surrogate_key(business_key: list[str], effective_start_col: str) -> Column:
    """SCD2 surrogate key: business key plus the version's start timestamp.

    Including the start timestamp is what makes each *version* uniquely
    identifiable, which is the whole point of SCD2. A fact row carrying this key
    is pinned to the dimension as it was when the event happened.
    """
    if not business_key:
        raise ValueError("versioned_surrogate_key needs a business key")
    parts = [
        *_norm(business_key),
        F.coalesce(F.col(effective_start_col).cast("string"), F.lit("␀")),
    ]
    return F.xxhash64(F.concat_ws(SEP, *parts))


def record_hash(columns: list[str]) -> Column:
    """Content hash over the given columns.

    Two uses:
      * Bronze `_record_hash` — cheap exact-duplicate detection.
      * SCD2 change detection — compare the hash of tracked columns instead of
        writing an N-way OR of `<=>` comparisons. Cheaper to compute, and much
        harder to get wrong when a column is added.

    Uses sha2/256 rather than xxhash64 because a false positive here means a
    *missed change* — a customer moves city and the dimension never notices.
    Collision resistance matters more than key width.
    """
    if not columns:
        raise ValueError("record_hash needs at least one column")
    return F.sha2(F.concat_ws(SEP, *_norm(columns)), 256)


def date_key(timestamp_col: str) -> Column:
    """Integer yyyyMMdd date key.

    Human-readable, sorts naturally, needs no join to interpret, and is 4 bytes
    instead of 8. `20260818` in a query result is self-explanatory in a way that
    a hash never is.
    """
    return F.date_format(F.col(timestamp_col).cast("timestamp"), "yyyyMMdd").cast("int")


def unknown_member_key() -> Column:
    """Key for the 'Unknown' dimension member.

    Used only when the natural key itself is missing or malformed. When the
    natural key *is* present but the dimension row has not arrived yet, use an
    inferred member instead (transformations/dimensional.py) — that preserves
    the linkage, which routing to Unknown destroys permanently.
    """
    return F.lit(-1).cast("bigint")
