# Resume Material

> **Read the honesty rules first.** Everything below is written to survive an
> interviewer who probes. Nothing claims production use, business impact, or a
> team. Placeholders marked `[X]` must be replaced with numbers you have
> actually measured — an unverified number is the fastest way to lose a
> technical interview.

---

## Project title

**Enterprise E-Commerce Lakehouse on Databricks (GCP) — Personal Project**

Alternatives, depending on the role you're targeting:

| Emphasis | Title |
|---|---|
| Databricks roles | Batch Lakehouse Platform — Databricks, Delta Lake, Unity Catalog |
| Platform / framework roles | Metadata-Driven Ingestion Framework on Databricks Lakehouse |
| Hybrid / integration roles | Hybrid CDC Lakehouse — Debezium → GCS → Databricks |

**Always label it "Personal Project".** A reader who discovers on their own that
it wasn't production work will discount everything else on the page.

---

## Resume bullets (4, ATS-friendly)

> Replace every `[X]` with a measured value before using these.

**Architected and built** a batch lakehouse on **Databricks (Google Cloud)** processing
**[X]M synthetic records** across **12 entities and 5 source systems** into a governed
**Bronze/Silver/Gold** medallion model using **Delta Lake, PySpark, Auto Loader and
Unity Catalog**, serving a **6-fact star schema** through **Databricks SQL**.

**Engineered a metadata-driven ingestion framework** in which **12 source entities**
are onboarded through **YAML configuration with zero code change**, implementing
**full, incremental-watermark and CDC** load patterns with **Delta MERGE**,
**SCD Type 1/Type 2** history, and idempotent reprocessing via **deterministic
hash surrogate keys**.

**Implemented real change data capture** with **Debezium** streaming the **PostgreSQL
write-ahead log** through **Kafka** to **Google Cloud Storage**, applying **LSN-ordered
deduplication**, tombstone-aware delete propagation and **68 configurable data-quality
rules** across **WARN/ERROR/FATAL** severities with row quarantine and
**source-to-target reconciliation**.

**Established the full delivery pipeline** — **Terraform** for Unity Catalog governance
(RBAC, PII column masks, row filters), **Databricks Asset Bundles** for job deployment,
**GitHub Actions** CI running **[X] automated tests** with lint, secret-scanning and
config validation, and a **containerised local Spark environment** enabling development
and testing at **zero cloud cost**.

---

## Numbers to measure before you use them

Run the pipeline, then fill these in. Do not estimate.

| Placeholder | Where to get it |
|---|---|
| `[X]M synthetic records` | `_control_totals/*.json` → sum of `counts` |
| `[X] automated tests` | `pytest tests/ --collect-only -q \| tail -1` |
| Pipeline runtime | `audit.pipeline_run_audit` → `duration_s` |
| Data quality pass rate | `audit.v_dq_summary` |
| Optimisation gain | Phase 13 before/after benchmark |

**Currently verified:** 48 tests passing; ~174K rows and $19.3M synthetic net
revenue at the `small` profile. The `medium` profile targets ~6M rows — measure
it before claiming it.

---

## Things NOT to claim

Each of these collapses under one follow-up question.

| Do not say | Why it fails | Say instead |
|---|---|---|
| "Production lakehouse" | It has never served a real user | "Production-*style*, personal project" |
| "Reduced costs by 40%" | There was no prior cost | "Designed and operated at $0 within free-tier limits" |
| "Improved data quality by X%" | No baseline existed | "Built a 68-rule quality framework with quarantine and reconciliation" |
| "Processed TBs daily" | It is ~6M rows | "~6M records, with the scale-out path documented" |
| "Led a team" | You were alone | Omit entirely |
| "Real-time streaming" | Scope is batch only | "Incremental batch on the Structured Streaming engine" |
| "Handled PII" | The data is synthetic | "Designed PII classification and masking against synthetic data" |

**The last one matters most.** Saying "all data is synthetic" *unprompted* reads
as rigour. Being caught implying otherwise reads as dishonesty, and interviewers
remember that far longer than any technical answer.

---

## LinkedIn / portfolio summary

> Built a batch lakehouse on Databricks (Google Cloud) as a personal project:
> 12 source entities across 5 systems flowing through a config-driven ingestion
> framework into a governed Bronze/Silver/Gold model on Delta Lake.
>
> The parts I'd want to talk about: real Debezium CDC off the Postgres WAL
> (ordered by log sequence number, not application timestamp, because
> timestamps lie); SCD Type 2 applied as a single MERGE so no window exists
> where a key has zero current versions; a 68-rule data-quality framework with
> three severities and row quarantine; and source-to-target reconciliation that
> catches the silent row loss that quality checks alone never will.
>
> Everything runs locally in Docker against a real Spark runtime before it
> reaches Databricks — so the code is executed, not just written. All data is
> synthetic; every free-tier limitation is documented rather than hidden.

---

## Skills this project evidences

**Databricks** — Unity Catalog · Delta Lake · Auto Loader · Lakeflow Jobs ·
Databricks SQL · Asset Bundles · Change Data Feed · liquid clustering

**Data engineering** — medallion architecture · dimensional modelling (star
schema, accumulating snapshot, periodic snapshot) · SCD1/SCD2 · CDC · idempotent
pipeline design · data quality frameworks · reconciliation

**Platform** — Terraform · GitHub Actions · Docker Compose · Debezium · Kafka ·
PostgreSQL logical replication · Google Cloud Storage · Secret Manager

**Engineering practice** — config-driven framework design · pytest ·
CI/CD gating · structured logging with run-id propagation · PII classification
and masking · cost-aware architecture

---

## Interview positioning

**When asked why you built it:** *"I've worked on GCP for four years — BigQuery,
Dataproc, Iceberg. I wanted to understand the lakehouse pattern on the platform
job descriptions actually name, and to build something where I'd made every
architectural decision myself and could defend it."*

**When asked about scale:** Be direct. *"~6 million records. Enough that
partitioning, join strategy and file sizing produce measurable differences —
I documented what changes at 10 TB/day, and the honest answer is that Auto
Loader moves to file-notification mode, MERGE needs partition-scoped predicates,
and the dedup window becomes the bottleneck. The medallion layering and the
config framework wouldn't change."*

**Your strongest single answer** — have this ready, it separates you from most
candidates: *"The bug I'm proudest of catching was in my own test data. My CDC
update events had identical before and after images. Every test passed, because
SCD2 change detection correctly ignores a row where nothing changed — so the
pipeline looked healthy while testing nothing. It taught me that a green test
suite and a correct one are different things."*
