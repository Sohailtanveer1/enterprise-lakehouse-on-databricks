"""Synthetic data generator CLI.

    python -m generator.generate --profile small --out ./local_lake/landing
    python -m generator.generate --profile medium --postgres --out gs://.../landing

Writes each entity to the landing layout the entity configs expect:

    <out>/customers/dt=YYYY-MM-DD/customers-<batch>.json
    <out>/erp/orders/dt=YYYY-MM-DD/orders-<batch>.parquet      (Debezium envelope)
    <out>/events/dt=YYYY-MM-DD/events-<batch>.json.gz

Two CDC modes:
  --postgres   seed Postgres and let real Debezium capture the WAL
  default      emit the identical Debezium envelope directly (NFR-LOCAL-05)

The envelope is byte-compatible between the two, so no pipeline code knows
which one produced it. That is what makes Debezium optional rather than
load-bearing.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import sys
import uuid
from collections.abc import Iterable
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .defects import DefectInjector, summarise
from .model import WINDOW_END, Generator

try:  # optional: Parquet output
    import pyarrow as pa
    import pyarrow.parquet as pq

    HAVE_ARROW = True
except ImportError:  # pragma: no cover
    HAVE_ARROW = False


def _json_default(o: Any) -> Any:
    if isinstance(o, Decimal):
        return str(o)
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    raise TypeError(f"not JSON serialisable: {type(o)}")


# ------------------------------------------------------------------- writers


class Landing:
    """Writes to a local directory tree. GCS upload is a separate concern —
    `gcloud storage rsync` or the cdc-sink container — so this stays testable
    with no cloud credentials."""

    def __init__(self, root: Path, batch_id: str):
        self.root = root
        self.batch_id = batch_id
        self.manifest: list[dict] = []

    def _path(self, entity: str, dt: str, ext: str) -> Path:
        p = self.root / entity / f"dt={dt}" / f"{entity.replace('/', '_')}-{self.batch_id}.{ext}"
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def _record(self, path: Path, rows: int) -> None:
        self.manifest.append(
            {
                "path": str(path.relative_to(self.root)),
                "rows": rows,
                "bytes": path.stat().st_size,
            }
        )

    def json_lines(self, entity: str, rows: list[dict], dt: str, gzipped: bool = False) -> None:
        if not rows:
            return
        body = "\n".join(json.dumps(r, default=_json_default) for r in rows) + "\n"
        path = self._path(entity, dt, "json.gz" if gzipped else "json")
        if gzipped:
            path.write_bytes(gzip.compress(body.encode()))
        else:
            path.write_text(body, encoding="utf-8")
        self._record(path, len(rows))

    def csv_file(self, entity: str, rows: list[dict], dt: str) -> None:
        if not rows:
            return
        # union of keys, not rows[0] — a dropped-column defect makes rows ragged
        cols: list[str] = []
        for r in rows:
            for k in r:
                if k not in cols:
                    cols.append(k)
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})
        path = self._path(entity, dt, "csv")
        path.write_text(buf.getvalue(), encoding="utf-8")
        self._record(path, len(rows))

    def parquet(self, entity: str, rows: list[dict], dt: str) -> None:
        if not rows:
            return
        if not HAVE_ARROW:  # graceful degradation, clearly signposted
            self.json_lines(entity, rows, dt)
            return
        path = self._path(entity, dt, "parquet")
        pq.write_table(pa.Table.from_pylist(rows), path, compression="snappy")
        self._record(path, len(rows))

    def raw(self, entity: str, text: str, dt: str, ext: str = "json") -> None:
        path = self._path(entity, dt, ext)
        path.write_text(text, encoding="utf-8")
        self._record(path, -1)  # -1: deliberately unparseable


# --------------------------------------------------------- Debezium envelope


def debezium(
    op: str, table: str, before: dict | None, after: dict | None, lsn: int, ts_ms: int
) -> dict:
    """Emit the Debezium envelope shape verbatim.

    Field names and nesting match `debezium/connect` with schemas disabled, so
    Silver's unwrap logic is identical whether Kafka was involved or not.

    op: c=create  r=snapshot read  u=update  d=delete
    """
    return {
        "op": op,
        "ts_ms": ts_ms,
        "before": before,
        "after": after,
        "source": {
            "db": "northpeak",
            "schema": "erp",
            "table": table,
            "lsn": lsn,
            "txId": lsn // 7 + 1,
            "ts_ms": ts_ms,
            "snapshot": "true" if op == "r" else "false",
            "connector": "postgresql",
            "name": "northpeak",
        },
    }


def envelope_stream(
    rows: Iterable[dict],
    table: str,
    start_lsn: int = 1_000_000,
) -> list[dict]:
    """Wrap generated rows as snapshot reads. LSN increments monotonically —
    it is the ordering key Silver dedups on, so it must be a total order."""
    out, lsn = [], start_lsn
    for r in rows:
        lsn += 7  # non-unit stride: nothing may assume LSNs are contiguous
        ts = int(datetime.now(UTC).timestamp() * 1000)
        out.append(debezium("r", table, None, r, lsn, ts))
    return out


def mutation_stream(
    rows: list[dict],
    table: str,
    rng,
    start_lsn: int,
    update_frac: float = 0.05,
    delete_frac: float = 0.01,
) -> list[dict]:
    """Post-snapshot updates and deletes, so the CDC path has real u and d
    events and not only the initial load."""
    out, lsn = [], start_lsn
    for r in rows:
        roll = rng.random()
        if roll >= update_frac + delete_frac:
            continue
        lsn += 7
        ts = int(datetime.now(UTC).timestamp() * 1000)
        if roll < delete_frac:
            out.append(debezium("d", table, r, None, lsn, ts))
            # Tombstone: null value keyed on the PK. Debezium emits one after
            # every delete and dropping it makes deletes unrecoverable.
            out.append(
                {
                    "op": "d",
                    "ts_ms": ts,
                    "before": r,
                    "after": None,
                    "source": out[-1]["source"],
                    "__tombstone": True,
                }
            )
        else:
            # A real diff, not a no-op. An update event whose before and after
            # are identical proves nothing: SCD2 change detection correctly
            # ignores it, so the test would pass while testing nothing.
            after = dict(r)
            cur = r.get("order_status")
            advance = {
                "PLACED": "CONFIRMED",
                "CONFIRMED": "SHIPPED",
                "SHIPPED": "DELIVERED",
                "DELIVERED": "RETURNED",
            }
            after["order_status"] = advance.get(cur, "CONFIRMED")
            after["shipping_status"] = (
                "DELIVERED" if after["order_status"] == "DELIVERED" else "IN_TRANSIT"
            )
            after["updated_at"] = datetime.now(UTC).isoformat()
            assert after["order_status"] != cur, "mutation must change something"
            out.append(debezium("u", table, r, after, lsn, ts))
    return out


def customer_scd2_changes(customers: list[dict], rng, frac: float = 0.04) -> list[dict]:
    """Customers who relocate to a different region.

    This is the SCD2 demonstration from ARCHITECTURE.md §7: an order placed
    while the customer lived in the old region must stay attributed there
    forever. Without these rows the SCD2 code path is never exercised.
    """
    from .model import CITIES, STATES_BY_REGION

    changed = []
    for c in customers:
        if rng.random() >= frac:
            continue
        old_state = c.get("state")
        new_region = rng.choice(
            [r for r in STATES_BY_REGION if old_state not in STATES_BY_REGION[r]]
        )
        new_state = rng.choice(STATES_BY_REGION[new_region])
        changed.append(
            dict(
                c,
                state=new_state,
                city=CITIES[new_state],
                # A segment upgrade alongside the move, so the tracked-column set
                # is exercised with more than one attribute at a time.
                customer_segment=rng.choice(["PREMIUM", "WHOLESALE"]),
                updated_at=datetime.now(UTC).isoformat(),
            )
        )
    return changed


# ---------------------------------------------------------------------- main


def run(profile: str, out: Path, seed: int, inject: bool, days_inventory: int) -> dict:
    gen = Generator(profile, seed)
    inj = DefectInjector(gen.rng, gen.totals, enabled=inject)
    batch = uuid.uuid4().hex[:10]
    land = Landing(out, batch)
    today = WINDOW_END.isoformat()

    print(f"profile={profile} seed={seed} defects={'on' if inject else 'off'} batch={batch}")

    categories = gen.categories()
    stores = gen.stores()
    customers = inj.customers(gen.customers())
    products = gen.products(categories)
    promotions = gen.promotions()

    land.csv_file("categories", categories, today)
    land.csv_file("stores", stores, today)
    land.csv_file("products", products, today)
    land.json_lines("customers", customers, today)
    land.json_lines("promotions", promotions, today)

    # Schema drift: a later products file gains a column (WARN, auto-evolve).
    if inject:
        land.csv_file("products", inj.add_unexpected_column(products[:200]), "2026-08-30")

    # SCD2 source: customers who moved region, delivered in a later batch so
    # the incremental watermark picks them up as genuine updates.
    scd2_updates = customer_scd2_changes(customers, gen.rng)
    land.json_lines("customers", scd2_updates, "2026-08-30")
    gen.totals.defect("customer_region_change", len(scd2_updates))

    orders, items, payments = [], [], []
    for o, its, pays in gen.orders(customers, products, stores, promotions):
        orders.append(inj.orders(o))
        items.extend(its)
        payments.extend(pays)
    items = inj.order_items(items)

    # ERP entities travel as Debezium envelopes, snapshot then mutations.
    for name, rows in (("orders", orders), ("order_items", items), ("payments", payments)):
        env = envelope_stream(rows, name)
        if inject and name == "orders":
            env += mutation_stream(
                orders[: max(1, len(orders) // 10)], name, gen.rng, start_lsn=9_000_000
            )
        land.parquet(f"erp/{name}", env, today)

    ships, rets = gen.shipments_and_returns(orders)
    land.json_lines("shipments", ships, today)
    land.json_lines("returns", rets, today)

    inv = list(gen.inventory(products, stores, days=days_inventory))
    # Inventory arrives one file per snapshot day, as a real WMS feed would.
    by_day: dict[str, list[dict]] = {}
    for row in inv:
        by_day.setdefault(row["snapshot_date"], []).append(row)
    for day, rows in by_day.items():
        land.csv_file("inventory", rows, day)

    events = inj.events(list(gen.events(customers, products, orders)))
    ev_by_day: dict[str, list[dict]] = {}
    for e in events:
        ev_by_day.setdefault(e["event_time"][:10], []).append(e)
    for day, rows in ev_by_day.items():
        land.json_lines("events", rows, day, gzipped=True)

    # A replayed batch and a truncated file: the two ingestion edge cases that
    # only show up in production if they were never tested.
    if inject:
        land.json_lines("returns", inj.replay_batch(rets[:50]), today)
        land.raw(
            "shipments",
            inj.corrupt_file_content(
                "\n".join(json.dumps(s, default=_json_default) for s in ships[:100])
            ),
            "2026-08-29",
        )

    totals = gen.totals.to_dict()
    totals["defect_summary"] = summarise(gen.totals)
    totals["batch_id"] = batch
    totals["profile"] = profile
    totals["seed"] = seed
    totals["generated_at"] = datetime.now(UTC).isoformat()
    totals["files"] = land.manifest

    ct = out / "_control_totals" / f"control_totals-{batch}.json"
    ct.parent.mkdir(parents=True, exist_ok=True)
    ct.write_text(json.dumps(totals, indent=2, default=_json_default), encoding="utf-8")

    print(f"\nfiles written : {len(land.manifest)}")
    for entity, n in sorted(totals["counts"].items()):
        print(f"  {entity:14s} {n:>10,}")
    print(f"\ngross revenue : {totals['gross_revenue']}")
    print(f"net revenue   : {totals['net_revenue']}")
    print(f"defects       : {totals['defect_summary']['total_defects']:,}")
    print(f"control totals: {ct}")
    return totals


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="NorthPeak synthetic data generator")
    ap.add_argument("--profile", default="small", choices=["small", "medium", "large"])
    ap.add_argument("--out", default="./local_lake/landing", type=Path)
    ap.add_argument("--seed", type=int, default=20260818)
    ap.add_argument(
        "--no-defects", action="store_true", help="clean data only — for baseline comparison"
    )
    ap.add_argument("--inventory-days", type=int, default=30)
    a = ap.parse_args(argv)
    run(a.profile, a.out, a.seed, not a.no_defects, a.inventory_days)
    return 0


if __name__ == "__main__":
    sys.exit(main())
