"""Deliberate data-quality defect injection.

Implements the defect catalogue in DATA_MODEL.md §1. A pipeline that only ever
sees clean data proves nothing: the quarantine path, the dedup window, the
schema-evolution branch and the referential-integrity check are all dead code
until something exercises them.

Every injection is counted in ControlTotals.defects, so tests can assert that
the pipeline caught exactly what was planted — not merely that it caught
"some" bad rows.
"""

from __future__ import annotations

import copy
import random
from datetime import datetime, timedelta
from typing import Any

from .model import ControlTotals

# Fractions are per-entity and deliberately small. Real feeds are mostly clean;
# a 20%-broken feed would be a source-system incident, not a quality baseline.
RATES = {
    "exact_duplicate": 0.010,
    "null_required": 0.006,
    "negative_number": 0.004,
    "invalid_state": 0.020,
    "orphan_fk": 0.008,
    "late_arrival": 0.015,
    "out_of_order": 0.020,
    "duplicate_event_id": 0.012,
    "discount_exceeds_gross": 0.003,
}


class DefectInjector:
    def __init__(self, rng: random.Random, totals: ControlTotals, enabled: bool = True):
        self.rng = rng
        self.totals = totals
        self.enabled = enabled

    def _hit(self, kind: str) -> bool:
        return self.enabled and self.rng.random() < RATES[kind]

    # ------------------------------------------------------------ row-level

    def customers(self, rows: list[dict]) -> list[dict]:
        out: list[dict] = []
        for r in rows:
            if self._hit("invalid_state"):
                # Domain-rule violation. WARN severity: a bad state code is
                # worth knowing about but does not invalidate the order.
                r = dict(r, state=self.rng.choice(["XX", "ZZ", "99", ""]))
                self.totals.defect("invalid_state")
            if self._hit("null_required"):
                r = dict(r, customer_segment=None)
                self.totals.defect("null_customer_segment")
            out.append(r)
            if self._hit("exact_duplicate"):
                out.append(copy.deepcopy(r))
                self.totals.defect("duplicate_customer")
        return out

    def order_items(self, items: list[dict]) -> list[dict]:
        out = []
        for it in items:
            if self._hit("negative_number"):
                # Range-rule violation -> ERROR -> quarantine. These rows must
                # NOT reach fact_sales, or revenue goes negative.
                it = dict(it, quantity=-abs(it["quantity"]))
                self.totals.defect("negative_quantity")
            elif self._hit("discount_exceeds_gross"):
                # Cross-field violation. Individually every column is valid,
                # which is exactly why single-column rules miss it.
                gross = float(it["unit_price"]) * it["quantity"]
                it = dict(it, discount_amount=f"{gross * 1.4:.2f}")
                self.totals.defect("discount_exceeds_gross")
            out.append(it)
            if self._hit("exact_duplicate"):
                out.append(copy.deepcopy(it))
                self.totals.defect("duplicate_order_item")
        return out

    def orders(self, order: dict) -> dict:
        if self._hit("orphan_fk"):
            # Referential-integrity violation -> inferred dimension member.
            # The natural key is present but unknown, so the linkage is kept.
            order = dict(order, customer_id=f"CUST-{self.rng.randint(90_000_000, 99_999_999)}")
            self.totals.defect("orphan_customer_id")
        if self._hit("null_required"):
            order = dict(order, region=None)
            self.totals.defect("null_region")
        return order

    def events(self, rows: list[dict]) -> list[dict]:
        out = []
        for e in rows:
            if self._hit("out_of_order"):
                # event_time moves backwards relative to arrival order. Batch
                # dedup must order by event_time, not by file position.
                t = datetime.fromisoformat(e["event_time"]) - timedelta(
                    minutes=self.rng.randint(2, 45)
                )
                e = dict(e, event_time=t.isoformat())
                self.totals.defect("out_of_order_event")
            out.append(e)
            if self._hit("duplicate_event_id"):
                # Same event_id, different ingest_time — an at-least-once
                # delivery artefact. Dedup keeps exactly one.
                dup = dict(
                    e,
                    ingest_time=(
                        datetime.fromisoformat(e["ingest_time"])
                        + timedelta(seconds=self.rng.randint(1, 120))
                    ).isoformat(),
                )
                out.append(dup)
                self.totals.defect("duplicate_event_id")
        return out

    # ----------------------------------------------------------- batch-level

    def late_records(self, rows: list[dict], watermark_col: str) -> tuple[list[dict], list[dict]]:
        """Split off rows that will be delivered in a *later* file than their
        timestamp implies. Tests late-arriving batch handling."""
        on_time, late = [], []
        for r in rows:
            (late if self._hit("late_arrival") else on_time).append(r)
        self.totals.defect("late_arrival", len(late))
        return on_time, late

    def corrupt_file_content(self, text: str) -> str:
        """Truncate a file mid-record. Auto Loader's rescued-data column should
        capture the fragment rather than the run failing."""
        self.totals.defect("corrupt_file")
        return text[: int(len(text) * 0.6)] + '\n{"order_id": "ORD-TRUNC'

    def add_unexpected_column(self, rows: list[dict], name: str = "loyalty_tier") -> list[dict]:
        """A new column appears mid-stream. WARN: auto-evolve and continue —
        an added column cannot break existing logic."""
        self.totals.defect("schema_added_column", len(rows))
        return [dict(r, **{name: self.rng.choice(["bronze", "silver", "gold"])}) for r in rows]

    def drop_column(self, rows: list[dict], name: str) -> list[dict]:
        """A column disappears. FATAL: this WILL break downstream logic, often
        silently by producing nulls. It is a human decision, not an automatic
        one."""
        self.totals.defect("schema_dropped_column", len(rows))
        return [{k: v for k, v in r.items() if k != name} for r in rows]

    def replay_batch(self, rows: list[dict]) -> list[dict]:
        """Return the same rows again, to be written as a second file. Auto
        Loader must ingest the new file; dedup must collapse the rows. Tests
        idempotency (NFR-IDEM-02) rather than file-level dedup."""
        self.totals.defect("replayed_batch", len(rows))
        return copy.deepcopy(rows)


def summarise(totals: ControlTotals) -> dict[str, Any]:
    """What was planted, for the test suite to assert against."""
    return {
        "total_defects": sum(totals.defects.values()),
        "by_kind": dict(sorted(totals.defects.items())),
    }
