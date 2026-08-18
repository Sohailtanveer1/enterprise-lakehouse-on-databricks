# Business Requirements

**Project:** Enterprise E-Commerce Lakehouse & Real-Time Analytics Platform
**Phase:** 1 — Architecture (design only)
**Status:** Draft for build

---

## 1. The business

**NorthPeak Retail Group** is a US direct-to-consumer and marketplace retailer, founded 2019.

| Attribute | Value |
|---|---|
| Channels | Own web storefront, mobile app, three marketplaces |
| Customers | ~2.4 M registered, ~500 K ordering in the analysis window |
| Catalogue | ~45 K active SKUs across 12 top-level categories |
| Fulfilment | 8 distribution centres, 4 US regions (West, Midwest, South, Northeast) |
| Order volume | ~500 K orders over 24 months (~700/day, seasonal peaks to ~2,500/day) |
| Analysis window | 2024-09-01 → 2026-08-31 |

> **SIMULATED:** NorthPeak is a fictional company and all data is synthetic. Volumes and
> distributions are chosen to be *representative of a real mid-market retailer* so that
> partitioning, join and skew behaviour resemble production. No real customer data is used.

---

## 2. The problem being solved

NorthPeak's data currently lives in five disconnected systems. Analysts extract from each
separately, reconcile in spreadsheets, and publish numbers that disagree between departments.

| Pain | Consequence today |
|---|---|
| No single source of truth for revenue | Finance and Merchandising publish different monthly revenue figures |
| Customer record fragmented across commerce API and ERP | Customer lifetime value cannot be calculated reliably |
| No history on dimensions | "Revenue by region" silently re-states history when a customer moves region |
| Inventory reported once daily, by email | Stockouts discovered after they cost sales |
| Clickstream never joined to orders | Conversion rate is estimated, not measured |
| No data quality gate | Bad source records reach dashboards; nobody knows until a number looks wrong |
| No lineage | "Where did this number come from?" takes days to answer |

**The platform must produce one governed, tested, historically-accurate analytical model that
every department reads from.**

---

## 3. Stakeholders and what each one needs

| Stakeholder | Primary need | Tolerance for latency |
|---|---|---|
| CFO / Finance | Revenue, margin, AOV, month-end close accuracy | Daily, must be *correct* over fast |
| VP Merchandising | Product and category performance, promotion impact | Daily |
| Head of Supply Chain | Inventory availability, stockout risk, fulfilment time | Daily, intra-day preferred |
| CMO / Growth | Acquisition, retention, conversion, segment behaviour | Daily |
| Ops / Trading floor | Live order and revenue rate during peak events | **Near real-time (< 5 min)** |
| Customer Service | Return rate and reasons | Daily |
| Data Governance | PII controlled, access auditable, lineage visible | Continuous |

The split between "correct daily" and "fast now" is the reason this platform has **both** a batch
path and a streaming path. That is a business requirement, not a technology preference.

---

## 4. Business questions → analytical model

Every question below must be answerable by a single query against the Gold layer. This table is
the contract that `DATA_MODEL.md` must satisfy.

| # | Business question | Gold object | Grain used | Key dimensions | Freshness SLA |
|---|---|---|---|---|---|
| 1 | Total revenue by day / month | `fact_sales` | order line | date | Daily by 07:00 |
| 2 | Revenue by region | `fact_sales` | order line | date, customer (region), store | Daily by 07:00 |
| 3 | Revenue by product and category | `fact_sales` | order line | date, product, category | Daily by 07:00 |
| 4 | Average order value | `fact_sales` → order rollup | order line | date, region, segment | Daily by 07:00 |
| 5 | Most valuable customers (CLV) | `agg_customer_lifetime` | customer | customer, segment | Daily by 07:30 |
| 6 | Return rate | `fact_returns` ÷ `fact_sales` | returned line | date, product, category, reason | Daily by 07:30 |
| 7 | Inventory availability | `fact_inventory_snapshot` | product × location × day | product, store, date | Daily by 06:00 |
| 8 | Frequently out-of-stock products | `fact_inventory_snapshot` | product × location × day | product, store, date | Daily by 06:00 |
| 9 | Customer acquisition and retention | `agg_customer_cohort` | customer × cohort month | date, segment, region | Daily by 07:30 |
| 10 | Conversion rate | `fact_customer_events` + `fact_sales` | event / order line | date, channel, device | Daily + streaming |
| 11 | Order fulfilment time | `fact_order_fulfillment` | order | date, store, region | Daily by 07:00 |
| 12 | Promotion impact | `fact_sales` | order line | promotion, date, product | Daily by 07:00 |
| 13 | Real-time vs historical sales | `rt_sales_by_minute` vs `agg_daily_sales` | minute / day | date, region | **< 5 min** |

### Metric definitions (agreed, non-negotiable)

Ambiguous metric definitions are the most common cause of departments disagreeing. These are
fixed here and implemented once, in Gold.

| Metric | Definition |
|---|---|
| **Gross revenue** | `SUM(quantity × unit_price)` before discount and tax, order lines only, excludes cancelled orders |
| **Net revenue** | `SUM(quantity × unit_price − discount_amount)`, excludes cancelled orders, **excludes tax** |
| **Recognised revenue** | Net revenue where `payment_status = 'CAPTURED'` **and** order not fully returned |
| **AOV** | Net revenue ÷ distinct orders, at order grain, excludes cancelled |
| **Return rate** | Returned quantity ÷ shipped quantity, over the *shipment* date window, not the return date window |
| **Conversion rate** | Distinct sessions with `order_created` ÷ distinct sessions with `product_view` |
| **Fulfilment time** | `shipped_at − order_created_at`, business hours not applied, cancelled orders excluded |
| **CLV** | Cumulative net revenue per customer, all time, no discounting (documented simplification) |
| **New customer** | First *recognised* order in the period, by `customer_id` |

> Two of these are deliberately opinionated. **Return rate** is measured against the shipment
> cohort rather than the calendar month, because measuring returns against the month they arrive
> understates the rate in growth periods. **Net revenue excludes tax** because tax is a liability,
> not revenue — a mistake this project's DQ rules explicitly test for.

---

## 5. Functional requirements

| ID | Requirement |
|---|---|
| FR-01 | Ingest from 5 heterogeneous source systems: REST API, SFTP/file drop, relational CDC extracts, warehouse file feeds, and a streaming event bus |
| FR-02 | Preserve raw source payloads immutably and replayably in Bronze |
| FR-03 | Support full-load, incremental-by-watermark, and CDC (insert/update/delete) load patterns from one configurable framework |
| FR-04 | Detect and process only new files, without reprocessing or missing any |
| FR-05 | Deduplicate at both record level and event level |
| FR-06 | Apply, record and enforce data quality rules; quarantine failures without stopping the pipeline |
| FR-07 | Maintain full history for Customer, Product and Store dimensions (SCD Type 2) |
| FR-08 | Apply latest-value-only semantics where history has no business value (SCD Type 1) |
| FR-09 | Propagate hard deletes from source through to the analytical model |
| FR-10 | Handle late-arriving and out-of-order records in both batch and streaming |
| FR-11 | Publish a conformed star schema serving all 13 business questions |
| FR-12 | Publish real-time metrics with under 5 minutes end-to-end latency |
| FR-13 | Reconcile record counts and monetary totals from source to Gold on every run |
| FR-14 | Record every pipeline run with status, row counts, duration and errors |
| FR-15 | Classify and protect PII; restrict access by role |
| FR-16 | Deploy all code and configuration through version control and CI/CD |
| FR-17 | Support full historical reprocessing without manual cleanup |

---

## 6. Scope

### In scope
Ingestion, modelling, quality, orchestration, governance, streaming, testing, CI/CD, IaC,
dashboards, documentation.

### Out of scope (and why)
| Excluded | Reason |
|---|---|
| ML / forecasting / recommendations | This is a data *engineering* portfolio, not a DS one. Adding models dilutes the signal. |
| Reverse ETL back to source systems | No genuine consumer; would be decoration. |
| Real customer or payment data | Synthetic only, by design. |
| Multi-region DR deployment | Documented in `ARCHITECTURE.md`, not built. Free Edition has one workspace. |
| Cost chargeback / FinOps tooling | Cost strategy is documented in `COST.md`; tooling would be theatre at this scale. |

---

## 7. Success criteria

The platform is complete when:

1. All 13 business questions are answerable from Gold with a single query, and the numbers match
   an independently computed control total from the source generator.
2. A source-side update, insert **and** delete each demonstrably flow Bronze → Silver → Gold with
   the correct SCD behaviour.
3. Re-running any pipeline produces byte-identical results (idempotency), verified by test.
4. A deliberately corrupted source file is quarantined without failing the run, and appears in
   `data_quality_results`.
5. Reconciliation catches an injected row-loss defect.
6. Streaming metrics land in under 5 minutes and reconcile against the batch equivalent.
7. CI blocks a merge that breaks a test.
8. PII columns are masked for a non-privileged role.
9. Every design decision has a written rationale and an interview answer.

---

## 8. Traceability

| Business question | FR coverage | Verified by |
|---|---|---|
| 1–4, 12 | FR-01…05, FR-11 | Integration test: Silver → Gold revenue totals |
| 5, 9 | FR-07, FR-11 | SCD2 history test |
| 6 | FR-06, FR-11 | Return-cohort test |
| 7, 8 | FR-01, FR-11 | Snapshot completeness test |
| 10, 13 | FR-10, FR-12 | Streaming latency + batch/stream reconciliation test |
| 11 | FR-11 | Accumulating snapshot milestone test |
| All | FR-13, FR-14 | Reconciliation + audit tables |
