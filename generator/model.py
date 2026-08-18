"""Synthetic NorthPeak data — entity generators and control totals.

Pure Python, no Spark. Runs in the file-gen container, in tests, and on a
laptop. Deterministic from a seed so every run is reproducible (NFR-LOCAL-04)
and reconciliation has ground truth to compare against.

Referential integrity is guaranteed by construction: orders draw from the
customer ids already generated, order_items from product ids, and so on.
Orphans exist only where defects.py deliberately creates them.
"""
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterator

# --------------------------------------------------------------- reference data

REGIONS = ["WEST", "MIDWEST", "SOUTH", "NORTHEAST"]
SEGMENTS = ["RETAIL", "PREMIUM", "WHOLESALE"]
ORDER_STATUS = ["PLACED", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
PAYMENT_STATUS = ["PENDING", "AUTHORIZED", "CAPTURED", "FAILED", "REFUNDED"]
SHIPPING_STATUS = ["NOT_SHIPPED", "PICKING", "IN_TRANSIT", "DELIVERED"]
PAYMENT_METHODS = ["CARD", "PAYPAL", "APPLE_PAY", "GIFT_CARD", "BNPL"]
RETURN_REASONS = ["DAMAGED", "WRONG_ITEM", "NOT_AS_DESCRIBED", "CHANGED_MIND", "SIZE_ISSUE"]
CHANNELS = ["WEB", "MOBILE_APP", "MARKETPLACE"]
DEVICES = ["desktop", "mobile", "tablet"]
EVENT_TYPES = [
    "product_view", "cart_add", "cart_remove", "checkout_started",
    "order_created", "payment_completed", "shipment_created",
]
CARRIERS = ["UPS", "FEDEX", "USPS", "DHL"]
PROMO_TYPES = ["PERCENT", "FIXED", "BOGO", "FREESHIP"]

STATES_BY_REGION = {
    "WEST": ["CA", "WA", "OR", "NV", "AZ"],
    "MIDWEST": ["IL", "OH", "MI", "WI", "MN"],
    "SOUTH": ["TX", "FL", "GA", "NC", "TN"],
    "NORTHEAST": ["NY", "MA", "PA", "NJ", "CT"],
}
CITIES = {
    "CA": "Los Angeles", "WA": "Seattle", "OR": "Portland", "NV": "Las Vegas",
    "AZ": "Phoenix", "IL": "Chicago", "OH": "Columbus", "MI": "Detroit",
    "WI": "Milwaukee", "MN": "Minneapolis", "TX": "Austin", "FL": "Miami",
    "GA": "Atlanta", "NC": "Charlotte", "TN": "Nashville", "NY": "New York",
    "MA": "Boston", "PA": "Philadelphia", "NJ": "Newark", "CT": "Hartford",
}
DEPARTMENTS = {
    "Electronics": ["Phones", "Laptops", "Audio"],
    "Home": ["Kitchen", "Furniture", "Decor"],
    "Apparel": ["Mens", "Womens", "Footwear"],
    "Sports": ["Fitness", "Outdoor", "Cycling"],
}
BRANDS = ["Northwind", "Cascade", "Ironwood", "Lumen", "Vertex", "Harbor", "Sable"]
SUPPLIERS = ["Acme Supply", "Global Trade", "Pacific Imports", "Summit Goods"]
FIRST = ["Alex", "Jordan", "Sam", "Riley", "Casey", "Morgan", "Taylor", "Jamie",
         "Avery", "Quinn", "Devon", "Rowan", "Skyler", "Emerson", "Hayden"]
LAST = ["Patel", "Nguyen", "Garcia", "Kim", "Okafor", "Rossi", "Silva", "Haddad",
        "Larsen", "Fischer", "Novak", "Mensah", "Ivanov", "Tanaka", "Costa"]

WINDOW_START = date(2024, 9, 1)
WINDOW_END = date(2026, 8, 31)

PROFILES = {
    "small": dict(customers=3_000, products=800, orders=10_000, events=100_000),
    "medium": dict(customers=120_000, products=45_000, orders=500_000, events=3_000_000),
    "large": dict(customers=400_000, products=45_000, orders=3_000_000, events=15_000_000),
}


@dataclass
class ControlTotals:
    """Ground truth for reconciliation (NFR-REC-05).

    The generator knows exactly what it produced. Any divergence at Gold is a
    pipeline defect, not an accounting opinion. Money is Decimal throughout;
    float accumulation over millions of rows drifts at the cent level and makes
    exact reconciliation impossible.
    """

    counts: dict[str, int] = field(default_factory=dict)
    gross_revenue: Decimal = Decimal("0.00")
    net_revenue: Decimal = Decimal("0.00")
    tax_total: Decimal = Decimal("0.00")
    refund_total: Decimal = Decimal("0.00")
    defects: dict[str, int] = field(default_factory=dict)

    def bump(self, entity: str, n: int = 1) -> None:
        self.counts[entity] = self.counts.get(entity, 0) + n

    def defect(self, kind: str, n: int = 1) -> None:
        self.defects[kind] = self.defects.get(kind, 0) + n

    def to_dict(self) -> dict[str, Any]:
        return {
            "counts": self.counts,
            "gross_revenue": str(self.gross_revenue),
            "net_revenue": str(self.net_revenue),
            "tax_total": str(self.tax_total),
            "refund_total": str(self.refund_total),
            "defects": self.defects,
        }


def money(value: float | str | Decimal) -> Decimal:
    """Money is Decimal everywhere. Accepts the str form too, because rows are
    serialised with string amounts to survive the JSON round trip without
    float drift."""
    return Decimal(str(value)).quantize(Decimal("0.01"))


def _ts(rng: random.Random, d: date) -> datetime:
    return datetime(
        d.year, d.month, d.day,
        rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59),
        tzinfo=timezone.utc,
    )


def _seasonal_weight(d: date) -> float:
    """November/December peak, January trough — so date partitions differ in
    size and partitioning decisions have something real to bite on."""
    return {11: 2.4, 12: 3.1, 1: 0.6, 2: 0.7}.get(d.month, 1.0)


class Generator:
    def __init__(self, profile: str = "small", seed: int = 20260818):
        if profile not in PROFILES:
            raise ValueError(f"unknown profile '{profile}'; choose {list(PROFILES)}")
        self.profile = profile
        self.cfg = PROFILES[profile]
        self.rng = random.Random(seed)
        self.totals = ControlTotals()
        self._order_days = self._weighted_days()

    def _weighted_days(self) -> list[date]:
        days, d = [], WINDOW_START
        while d <= WINDOW_END:
            days.extend([d] * int(_seasonal_weight(d) * 10))
            d += timedelta(days=1)
        return days

    # ------------------------------------------------------------- dimensions

    def categories(self) -> list[dict]:
        rows, cid = [], 0
        for dept, subs in DEPARTMENTS.items():
            cid += 1
            parent = f"CAT-{cid:04d}"
            rows.append(dict(category_id=parent, category_name=dept,
                             parent_category_id=None, department=dept,
                             updated_at=_ts(self.rng, WINDOW_START)))
            for sub in subs:
                cid += 1
                rows.append(dict(category_id=f"CAT-{cid:04d}", category_name=sub,
                                 parent_category_id=parent, department=dept,
                                 updated_at=_ts(self.rng, WINDOW_START)))
        self.totals.bump("categories", len(rows))
        return rows

    def stores(self) -> list[dict]:
        rows = []
        for i in range(1, 13):
            region = REGIONS[i % len(REGIONS)]
            state = self.rng.choice(STATES_BY_REGION[region])
            rows.append(dict(
                store_id=f"STR-{i:03d}",
                store_name=f"NorthPeak {CITIES[state]} {'DC' if i <= 8 else 'Retail'}",
                store_type="DC" if i <= 8 else "RETAIL",
                city=CITIES[state], state=state, region=region,
                opened_date=(WINDOW_START - timedelta(days=self.rng.randint(400, 2000))).isoformat(),
                is_active=True,
            ))
        self.totals.bump("stores", len(rows))
        return rows

    def customers(self) -> list[dict]:
        rows = []
        for i in range(1, self.cfg["customers"] + 1):
            region = self.rng.choice(REGIONS)
            state = self.rng.choice(STATES_BY_REGION[region])
            first, last = self.rng.choice(FIRST), self.rng.choice(LAST)
            signup = WINDOW_START - timedelta(days=self.rng.randint(0, 900))
            rows.append(dict(
                customer_id=f"CUST-{i:08d}",
                first_name=first, last_name=last,
                email=f"{first.lower()}.{last.lower()}{i}@example.com",
                # Inconsistent formatting on purpose — Silver must standardise it.
                phone=self.rng.choice([
                    f"({self.rng.randint(200,999)}) {self.rng.randint(200,999)}-{self.rng.randint(1000,9999)}",
                    f"{self.rng.randint(200,999)}-{self.rng.randint(200,999)}-{self.rng.randint(1000,9999)}",
                    f"+1{self.rng.randint(2000000000,9999999999)}",
                ]),
                address_line1=f"{self.rng.randint(1,9999)} {self.rng.choice(['Oak','Maple','Cedar','Pine'])} St",
                city=CITIES[state], state=state, country="US",
                postal_code=f"{self.rng.randint(10000, 99999)}",
                signup_date=signup.isoformat(),
                customer_segment=self.rng.choices(SEGMENTS, weights=[70, 22, 8])[0],
                updated_at=_ts(self.rng, signup).isoformat(),
            ))
        self.totals.bump("customers", len(rows))
        return rows

    def products(self, categories: list[dict]) -> list[dict]:
        leaf = [c["category_id"] for c in categories if c["parent_category_id"]]
        rows = []
        for i in range(1, self.cfg["products"] + 1):
            cost = round(self.rng.uniform(4, 700), 2)
            rows.append(dict(
                product_id=f"SKU-{i:06d}",
                product_name=f"{self.rng.choice(BRANDS)} Model {self.rng.randint(100,999)}",
                category_id=self.rng.choice(leaf),
                brand=self.rng.choice(BRANDS),
                price=round(cost * self.rng.uniform(1.25, 2.6), 2),
                cost=cost,
                supplier=self.rng.choice(SUPPLIERS),
                status=self.rng.choices(
                    ["ACTIVE", "DISCONTINUED", "PENDING"], weights=[88, 8, 4])[0],
                updated_at=_ts(self.rng, WINDOW_START).isoformat(),
            ))
        self.totals.bump("products", len(rows))
        return rows

    def promotions(self) -> list[dict]:
        rows = []
        for i in range(1, 61):
            start = WINDOW_START + timedelta(days=self.rng.randint(0, 690))
            rows.append(dict(
                promotion_id=f"PROMO-{i:04d}",
                promotion_name=f"Campaign {i}",
                promo_type=self.rng.choice(PROMO_TYPES),
                discount_value=round(self.rng.uniform(5, 40), 2),
                start_date=start.isoformat(),
                end_date=(start + timedelta(days=self.rng.randint(3, 30))).isoformat(),
                channel=self.rng.choice(CHANNELS),
                updated_at=_ts(self.rng, start).isoformat(),
            ))
        self.totals.bump("promotions", len(rows))
        return rows

    # ----------------------------------------------------------------- facts

    def orders(self, customers: list[dict], products: list[dict],
               stores: list[dict], promotions: list[dict]) -> Iterator[tuple[dict, list[dict], list[dict]]]:
        """Yields (order, items, payments) so referential integrity holds by
        construction and memory stays flat at the large profile."""
        cust_ids = [c["customer_id"] for c in customers]
        cust_region = {c["customer_id"]: c["state"] for c in customers}
        prods = {p["product_id"]: p for p in products}
        prod_ids = list(prods)
        store_ids = [s["store_id"] for s in stores]
        promo_ids = [p["promotion_id"] for p in promotions]
        state_region = {s: r for r, ss in STATES_BY_REGION.items() for s in ss}

        for i in range(1, self.cfg["orders"] + 1):
            oid = f"ORD-{i:09d}"
            cid = self.rng.choice(cust_ids)
            d = self.rng.choice(self._order_days)
            placed = _ts(self.rng, d)
            status = self.rng.choices(
                ORDER_STATUS, weights=[4, 6, 12, 68, 6, 4])[0]
            pay_status = "CAPTURED" if status not in ("CANCELLED",) else "REFUNDED"

            order = dict(
                order_id=oid, customer_id=cid,
                order_date=placed.isoformat(),
                order_status=status, payment_status=pay_status,
                shipping_status=("DELIVERED" if status == "DELIVERED"
                                 else self.rng.choice(SHIPPING_STATUS)),
                store_id=self.rng.choice(store_ids),
                region=state_region.get(cust_region[cid], "WEST"),
                promotion_id=self.rng.choice(promo_ids) if self.rng.random() < 0.22 else None,
                currency="USD",
                updated_at=placed.isoformat(),
            )

            items = []
            for line in range(1, self.rng.choices([1, 2, 3, 4, 5], weights=[42, 27, 17, 9, 5])[0] + 1):
                p = prods[self.rng.choice(prod_ids)]
                qty = self.rng.choices([1, 2, 3, 4], weights=[68, 20, 8, 4])[0]
                unit = money(p["price"])
                gross = unit * qty
                disc = money(float(gross) * self.rng.uniform(0.05, 0.30)) if order["promotion_id"] else Decimal("0.00")
                tax = money(float(gross - disc) * 0.08)
                items.append(dict(
                    order_id=oid, order_line_number=line, product_id=p["product_id"],
                    quantity=qty, unit_price=str(unit),
                    discount_amount=str(disc), tax_amount=str(tax),
                    updated_at=placed.isoformat(),
                ))
                # Cancelled orders are excluded from revenue by definition
                # (BUSINESS_REQUIREMENTS.md §4). Control totals must agree.
                if status != "CANCELLED":
                    self.totals.gross_revenue += gross
                    self.totals.net_revenue += gross - disc
                    self.totals.tax_total += tax

            order_net = sum(
                (money(i2["unit_price"]) * i2["quantity"] - money(i2["discount_amount"]))
                for i2 in items
            )
            payments = [dict(
                payment_id=f"PAY-{i:09d}-1", order_id=oid,
                payment_method=self.rng.choice(PAYMENT_METHODS),
                payment_amount=str(order_net + sum(money(x["tax_amount"]) for x in items)),
                payment_status=pay_status, attempt_number=1,
                paid_at=placed.isoformat(), updated_at=placed.isoformat(),
            )]
            # ~6% of orders take two attempts — needed for payment success rate.
            if self.rng.random() < 0.06:
                payments.insert(0, dict(
                    payments[0], payment_id=f"PAY-{i:09d}-0",
                    payment_status="FAILED", attempt_number=0,
                ))

            self.totals.bump("orders")
            self.totals.bump("order_items", len(items))
            self.totals.bump("payments", len(payments))
            yield order, items, payments

    def inventory(self, products: list[dict], stores: list[dict],
                  days: int = 30) -> Iterator[dict]:
        """Periodic snapshot: every product at every DC, every day. This is the
        entity that makes partitioning matter — it is the widest cross product
        in the model."""
        dcs = [s["store_id"] for s in stores if s["store_type"] == "DC"]
        sample = products if len(products) <= 800 else self.rng.sample(products, 800)
        for offset in range(days):
            snap = WINDOW_END - timedelta(days=offset)
            for p in sample:
                for loc in dcs:
                    on_hand = max(0, int(self.rng.gauss(120, 70)))
                    reserved = min(on_hand, max(0, int(self.rng.gauss(15, 12))))
                    self.totals.bump("inventory")
                    yield dict(
                        snapshot_date=snap.isoformat(), product_id=p["product_id"],
                        location_id=loc, quantity_on_hand=on_hand,
                        quantity_reserved=reserved,
                        quantity_available=on_hand - reserved,
                        reorder_point=40,
                        updated_at=_ts(self.rng, snap).isoformat(),
                    )

    def shipments_and_returns(self, orders: list[dict]) -> tuple[list[dict], list[dict]]:
        ships, rets = [], []
        for n, o in enumerate(orders, 1):
            if o["order_status"] in ("CANCELLED", "PLACED"):
                continue
            placed = datetime.fromisoformat(o["order_date"])
            shipped = placed + timedelta(hours=self.rng.randint(6, 96))
            delivered = shipped + timedelta(hours=self.rng.randint(24, 216))
            ships.append(dict(
                shipment_id=f"SHP-{n:09d}", order_id=o["order_id"],
                carrier=self.rng.choice(CARRIERS),
                tracking_number=hashlib.md5(o["order_id"].encode()).hexdigest()[:16].upper(),
                shipped_at=shipped.isoformat(),
                delivered_at=delivered.isoformat() if o["order_status"] == "DELIVERED" else None,
                ship_from_location=o["store_id"],
                updated_at=shipped.isoformat(),
            ))
            self.totals.bump("shipments")
            if self.rng.random() < 0.07:
                rdate = delivered + timedelta(days=self.rng.randint(1, 25))
                refund = money(self.rng.uniform(10, 400))
                rets.append(dict(
                    return_id=f"RET-{n:09d}", order_id=o["order_id"],
                    product_id=None, return_reason=self.rng.choice(RETURN_REASONS),
                    quantity=self.rng.randint(1, 2), return_date=rdate.isoformat(),
                    refund_amount=str(refund), updated_at=rdate.isoformat(),
                ))
                self.totals.refund_total += refund
                self.totals.bump("returns")
        return ships, rets

    def events(self, customers: list[dict], products: list[dict],
               orders: list[dict]) -> Iterator[dict]:
        """Clickstream. Sessions follow a realistic funnel so conversion rate is
        a measurable number rather than noise."""
        cust_ids = [c["customer_id"] for c in customers]
        prod_ids = [p["product_id"] for p in products]
        order_by_cust = {o["customer_id"]: o for o in orders}
        target, made = self.cfg["events"], 0
        session = 0

        while made < target:
            session += 1
            sid = f"SESS-{session:010d}"
            # 35% of sessions are anonymous — customer_id nulls are normal here,
            # not a defect, and the DQ rules must not treat them as one.
            cid = self.rng.choice(cust_ids) if self.rng.random() > 0.35 else None
            d = self.rng.choice(self._order_days)
            t = _ts(self.rng, d)
            channel, device = self.rng.choice(CHANNELS), self.rng.choice(DEVICES)

            funnel = ["product_view"] * self.rng.randint(1, 5)
            if self.rng.random() < 0.30:
                funnel.append("cart_add")
                if self.rng.random() < 0.45:
                    funnel.append("checkout_started")
                    if self.rng.random() < 0.62:
                        funnel += ["order_created", "payment_completed"]

            for step in funnel:
                if made >= target:
                    break
                t += timedelta(seconds=self.rng.randint(5, 400))
                made += 1
                self.totals.bump("events")
                yield dict(
                    event_id=f"EVT-{made:012d}", event_type=step,
                    event_time=t.isoformat(),
                    ingest_time=(t + timedelta(seconds=self.rng.randint(1, 30))).isoformat(),
                    session_id=sid, customer_id=cid,
                    product_id=self.rng.choice(prod_ids) if step != "payment_completed" else None,
                    order_id=(order_by_cust.get(cid, {}).get("order_id")
                              if step in ("order_created", "payment_completed") and cid else None),
                    channel=channel, device_type=device,
                    properties={"page": step, "ab_variant": self.rng.choice(["A", "B"])},
                )
