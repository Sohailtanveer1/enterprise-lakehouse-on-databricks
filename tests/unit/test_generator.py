"""Generator tests — pure Python, no Spark.

The generator is the ground truth for every reconciliation check, so a bug
here produces false pipeline failures that cost hours to chase. These tests
guard the properties the rest of the project assumes.
"""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from generator.defects import DefectInjector
from generator.generate import customer_scd2_changes, debezium, mutation_stream
from generator.model import STATES_BY_REGION, Generator, money


@pytest.fixture(scope="module")
def gen():
    return Generator("small", seed=7)


# ------------------------------------------------------------ determinism


def test_same_seed_produces_identical_output():
    """NFR-LOCAL-04. Without this, a failing assertion is intermittent and a
    bug report becomes a shrug."""
    a = Generator("small", seed=99).customers()
    b = Generator("small", seed=99).customers()
    assert a == b


def test_different_seeds_diverge():
    a = Generator("small", seed=1).customers()
    b = Generator("small", seed=2).customers()
    assert a != b


# -------------------------------------------------------------- money type


def test_money_accepts_the_serialised_string_form():
    """Amounts round-trip through JSON as strings. money() rejecting its own
    output crashed the first generator run."""
    assert money("12.5") == Decimal("12.50")
    assert money(12.5) == Decimal("12.50")
    assert money(Decimal("12.499")) == Decimal("12.50")


def test_money_is_never_float():
    g = Generator("small", seed=3)
    for row in g.products(g.categories())[:20]:
        # Prices are floats in the source row (a real source would be too);
        # the invariant is that anything money() touches becomes Decimal.
        assert isinstance(money(row["price"]), Decimal)


# ---------------------------------------------------- referential integrity


def test_orders_reference_real_customers_and_products(gen):
    customers = gen.customers()
    products = gen.products(gen.categories())
    stores, promos = gen.stores(), gen.promotions()

    customer_ids = {c["customer_id"] for c in customers}
    product_ids = {p["product_id"] for p in products}

    checked = 0
    for order, items, payments in gen.orders(customers, products, stores, promos):
        assert order["customer_id"] in customer_ids
        for item in items:
            assert item["product_id"] in product_ids
            assert item["order_id"] == order["order_id"]
        for payment in payments:
            assert payment["order_id"] == order["order_id"]
        checked += 1
        if checked >= 200:
            break
    assert checked == 200


def test_order_line_numbers_are_unique_within_an_order(gen):
    """This pair is the grain of fact_sales. A duplicate here means the MERGE
    matches twice and Delta raises."""
    customers = gen.customers()[:200]
    products = gen.products(gen.categories())
    for _, items, _ in list(gen.orders(customers, products, gen.stores(), gen.promotions()))[:100]:
        lines = [i["order_line_number"] for i in items]
        assert len(lines) == len(set(lines))


# ------------------------------------------------------- control totals


def test_control_totals_exclude_cancelled_orders():
    """Gold excludes cancelled orders by definition (BUSINESS_REQUIREMENTS §4).
    If the generator counted them, reconciliation would fail forever on a
    definition mismatch rather than a real defect."""
    g = Generator("small", seed=11)
    customers, cats = g.customers(), g.categories()
    products, stores, promos = g.products(cats), g.stores(), g.promotions()

    expected_net = Decimal("0.00")
    for order, items, _ in g.orders(customers, products, stores, promos):
        if order["order_status"] == "CANCELLED":
            continue
        for item in items:
            expected_net += (
                money(item["unit_price"]) * item["quantity"] - money(item["discount_amount"])
            )
    assert g.totals.net_revenue == expected_net


def test_net_revenue_is_below_gross():
    g = Generator("small", seed=13)
    customers, cats = g.customers(), g.categories()
    list(g.orders(customers, g.products(cats), g.stores(), g.promotions()))
    assert Decimal("0") < g.totals.net_revenue <= g.totals.gross_revenue


# ------------------------------------------------------- Debezium envelope


def test_envelope_shape_matches_debezium():
    """Silver's unwrap keys off these exact field names."""
    row = {"order_id": "ORD-1", "order_status": "PLACED"}
    env = debezium("u", "orders", before=row, after={**row, "order_status": "SHIPPED"},
                   lsn=42, ts_ms=1)
    assert set(env) == {"op", "ts_ms", "before", "after", "source"}
    assert {"db", "schema", "table", "lsn", "ts_ms", "snapshot"} <= set(env["source"])


def test_delete_has_null_after_and_populated_before():
    """after is NULL for deletes — a naive after.* expansion drops every one,
    and the target silently never loses rows."""
    row = {"order_id": "ORD-1"}
    env = debezium("d", "orders", before=row, after=None, lsn=1, ts_ms=1)
    assert env["after"] is None
    assert env["before"] == row


def test_mutations_are_never_no_ops():
    """An update whose before and after are identical proves nothing: SCD2
    change detection correctly ignores it, so the test passes while testing
    nothing. This shipped as a bug in the first generator run."""
    import random

    rows = [{"order_id": f"ORD-{i}", "order_status": "PLACED"} for i in range(300)]
    events = mutation_stream(rows, "orders", random.Random(5), start_lsn=1,
                             update_frac=1.0, delete_frac=0.0)
    assert events, "expected mutations"
    for e in events:
        assert e["before"]["order_status"] != e["after"]["order_status"]


def test_deletes_emit_a_tombstone():
    import random

    rows = [{"order_id": f"ORD-{i}"} for i in range(200)]
    events = mutation_stream(rows, "orders", random.Random(5), start_lsn=1,
                             update_frac=0.0, delete_frac=1.0)
    deletes = [e for e in events if e["op"] == "d" and not e.get("__tombstone")]
    tombstones = [e for e in events if e.get("__tombstone")]
    assert len(deletes) == len(tombstones) == len(rows)


def test_lsn_is_monotonic_and_non_contiguous():
    """Nothing downstream may assume LSNs increment by one."""
    import random

    rows = [{"order_id": f"ORD-{i}", "order_status": "PLACED"} for i in range(100)]
    events = mutation_stream(rows, "orders", random.Random(5), start_lsn=1000,
                             update_frac=1.0, delete_frac=0.0)
    lsns = [e["source"]["lsn"] for e in events]
    assert lsns == sorted(lsns)
    assert any(b - a > 1 for a, b in zip(lsns, lsns[1:]))


# ------------------------------------------------------------ SCD2 source


def test_scd2_changes_actually_change_the_tracked_columns():
    """These rows are the only thing that exercises the SCD2 path. If they do
    not differ, apply_scd2 opens no versions and the feature is untested."""
    import random

    g = Generator("small", seed=17)
    customers = g.customers()
    changed = customer_scd2_changes(customers, random.Random(3), frac=1.0)
    by_id = {c["customer_id"]: c for c in customers}

    assert len(changed) == len(customers)
    for row in changed:
        original = by_id[row["customer_id"]]
        assert row["state"] != original["state"]
        # New state must belong to a genuinely different region, or the SCD2
        # version records a move that changes nothing analytically.
        old_region = next(r for r, s in STATES_BY_REGION.items() if original["state"] in s)
        assert row["state"] not in STATES_BY_REGION[old_region]


# ---------------------------------------------------------------- defects


def test_defects_are_counted_not_just_created():
    """Tests assert what was planted, not merely that something was caught."""
    import random

    g = Generator("small", seed=23)
    injector = DefectInjector(random.Random(1), g.totals, enabled=True)
    injector.customers(g.customers())
    assert sum(g.totals.defects.values()) > 0
    assert "duplicate_customer" in g.totals.defects or "invalid_state" in g.totals.defects


def test_defects_can_be_disabled_for_a_clean_baseline():
    import random

    g = Generator("small", seed=29)
    injector = DefectInjector(random.Random(1), g.totals, enabled=False)
    rows = g.customers()
    assert injector.customers(rows) == rows
    assert g.totals.defects == {}


def test_negative_quantities_are_injected_for_the_range_rule():
    import random

    g = Generator("small", seed=31)
    injector = DefectInjector(random.Random(2), g.totals, enabled=True)
    items = [
        {"order_id": f"O{i}", "order_line_number": 1, "quantity": 2,
         "unit_price": "10.00", "discount_amount": "0.00"}
        for i in range(2000)
    ]
    out = injector.order_items(items)
    assert any(i["quantity"] < 0 for i in out), "range rule would go untested"


# ----------------------------------------------------------- end to end


def test_full_run_writes_files_and_control_totals(small_dataset):
    """small_dataset is the session fixture; this asserts its shape."""
    control = list((small_dataset / "_control_totals").glob("*.json"))
    assert len(control) == 1

    totals = json.loads(control[0].read_text(encoding="utf-8"))
    assert totals["profile"] == "small"
    assert totals["counts"]["orders"] == 10_000
    assert Decimal(totals["net_revenue"]) > 0
    assert totals["defect_summary"]["total_defects"] > 0

    # Landing layout must match what the entity configs glob for.
    assert (small_dataset / "customers").exists()
    assert (small_dataset / "erp" / "orders").exists()
    assert list((small_dataset / "events").glob("dt=*"))
