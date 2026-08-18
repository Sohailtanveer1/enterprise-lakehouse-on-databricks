# Enterprise E-Commerce Lakehouse on Databricks

**Batch Lakehouse · Google Cloud + Databricks · Delta Lake · Unity Catalog · Debezium CDC**

A production-style batch lakehouse for a fictional US e-commerce retailer. Five
operational source systems run as containers standing in for an on-premise
estate, land files into Google Cloud Storage, and Databricks ingests them with
Auto Loader into a governed Bronze → Silver → Gold model served through
Databricks SQL.

Deliberately not a `CSV → notebook → chart` project.

> **Status:** all code written and committed. Not yet executed end to end on
> Databricks — see [Verification status](#verification-status) for exactly what
> has run and what has not. **All data is synthetic.**

---

## Architecture

```mermaid
flowchart LR
    subgraph D["Docker — on-prem source estate"]
        PG[("Postgres ERP")] --> DZ["Debezium"] --> K["Kafka"] --> SK["cdc-sink"]
        API["FastAPI"] --> EX["extractors"]
        SF["SFTP"] --> EX
        FG["file-gen"]
    end
    subgraph G["Google Cloud"]
        GCS[("Cloud Storage<br/>landing zone")]
    end
    subgraph DB["Databricks — Unity Catalog"]
        BR["Bronze"] --> SI["Silver"] --> GO["Gold"]
    end
    SK --> GCS
    EX --> GCS
    FG --> GCS
    GCS -->|"Auto Loader"| BR
    GO --> DS["Databricks SQL<br/>+ dashboards"]
```

Full diagrams, ten architecture decision records and the GCP→Databricks concept
mapping are in [ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## What it does

| | |
|---|---|
| **Sources** | 12 entities across 5 systems: REST API, SFTP, Postgres CDC, WMS files, clickstream |
| **Ingestion** | One config-driven engine. Adding an entity is a YAML file, **no code change** |
| **CDC** | Real Debezium capture off the Postgres write-ahead log, ordered by LSN |
| **History** | SCD Type 1 and Type 2, applied as a single MERGE |
| **Quality** | 68 rules, three severities, row quarantine |
| **Reconciliation** | Source→Bronze→Silver→Gold counts, money to the cent, fact grain |
| **Model** | 6 facts, 6 dimensions, 4 aggregate marts — all 13 business questions answered |
| **Governance** | Unity Catalog RBAC, PII column masks, row filters, classification tags |
| **Delivery** | Terraform + Asset Bundles + GitHub Actions, 48 tests |

---

## Documentation

**Start here:** [ARCHITECTURE.md](docs/ARCHITECTURE.md) · [DATA_MODEL.md](docs/DATA_MODEL.md) · [INTERVIEW_QA.md](docs/INTERVIEW_QA.md)

| Document | Contents |
|---|---|
| [PHASE0-FEASIBILITY.md](docs/PHASE0-FEASIBILITY.md) | Free Edition limits verified against official docs, cost model, ranked traps |
| [BUSINESS_REQUIREMENTS.md](docs/BUSINESS_REQUIREMENTS.md) | Scenario, 13 questions, agreed metric definitions |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | 7 diagrams, 10 ADRs, where the GCP analogies break down |
| [DATA_MODEL.md](docs/DATA_MODEL.md) | Source contracts, Debezium envelope, star schema, fact grain, key strategy |
| [NON_FUNCTIONAL_REQUIREMENTS.md](docs/NON_FUNCTIONAL_REQUIREMENTS.md) | Idempotency contract, DQ thresholds, scale-out analysis |
| [SECURITY.md](docs/SECURITY.md) | PII classification, role model, masking, identity flow to storage |
| [COST.md](docs/COST.md) | $0 budget model, 12 ranked cost traps |
| [PHASE2-SETUP.md](docs/PHASE2-SETUP.md) | Docker, GCP and Databricks setup runbook, plus the two spikes |
| [PERFORMANCE.md](docs/PERFORMANCE.md) | Physical design, when each optimisation hurts, benchmarking |
| [INTERVIEW_QA.md](docs/INTERVIEW_QA.md) | ~40 questions answered with the trade-off stated |
| [RESUME.md](docs/RESUME.md) | Title, 4 bullets, and what **not** to claim |
| [REPO_STRUCTURE.md](docs/REPO_STRUCTURE.md) | Layout, deviations, branching model |

---

## Quick start

```bash
cp docker/.env.example docker/.env
```

```bash
docker compose --profile core --profile cdc up -d --build
```

```bash
docker compose --profile dev run --rm spark-dev python -m generator.generate --profile small --out /work/local_lake/landing
```

```bash
docker compose --profile dev run --rm spark-dev pytest tests/ -q
```

Then follow [PHASE2-SETUP.md](docs/PHASE2-SETUP.md) for GCP, Databricks and the
blocking Spike 1.

---

## Verification status

Stated precisely, because "I built it" and "I ran it" are different claims.

| Component | Status |
|---|---|
| Config layer (12 entities, 4 envs, 68 rules) | ✅ **Executed** — loads and validates clean |
| Synthetic data generator | ✅ **Executed** — 747 files, ~174K rows, $19.3M synthetic revenue |
| Test suite | ✅ **Executed** — 48 passing |
| Lint + format gate | ✅ **Executed** — ruff and black clean across 31 files |
| Job DAG, bundle, workflow YAML | ✅ Parsed and dependency-checked |
| SQL (governance, dashboards) | ✅ Statement-split verified, no fragments |
| Spark transformations | ⚠️ **Syntax-checked, not executed** — needs the spark-dev container |
| Docker estate | ⚠️ **Built, not started** — needs the Docker daemon |
| Databricks deployment | ⚠️ **Not run** — needs a workspace and Spike 1 |

---

## Honest scope

Where a capability is unavailable it is **labelled**, never hand-waved:

- **Batch only.** No streaming path. Auto Loader runs on the Structured
  Streaming engine in incremental-batch mode — checkpointing and exactly-once
  file handling are in scope; watermarks and sub-minute SLAs are not.
- **DEV/TEST/PROD** are three Unity Catalog catalogs in one workspace, because
  Free Edition permits one workspace.
- **Multi-user RBAC** is designed and coded but cannot be enforced — Unity
  Catalog groups are account-level and Free Edition has no account console. The
  account layer lives in `terraform/reference-only/`, written and never applied.
- **The ERP is Postgres** standing in for a commercial system. The **CDC itself
  is real** — Debezium off the write-ahead log.
- **All data is synthetic.** No production deployment is claimed.

---

## Tech stack

Databricks (Unity Catalog, Delta Lake, Auto Loader, Lakeflow Jobs, Databricks
SQL, Asset Bundles, Change Data Feed) · Google Cloud (Cloud Storage, Secret
Manager) · Docker (Postgres, Debezium, Kafka, FastAPI, SFTP, Spark) · PySpark ·
Python · SQL · Terraform · GitHub Actions · pytest

## Related work

Deliberately non-overlapping siblings in this portfolio:

- **`real-time-analytics-on-gcp`** — streaming platform on GCP
- **`supply-chain-batch-platform`** — batch on GCP with Iceberg, BigQuery, Airflow

This project covers what those do not: Delta Lake, Unity Catalog governance,
Lakeflow Jobs, Asset Bundles, and Debezium-based CDC — and unlike the sibling
batch project, the code here executes rather than only compiling.
