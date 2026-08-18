# Phase 0 — Feasibility, Environment & Constraints

**Project:** Enterprise E-Commerce Lakehouse & Real-Time Analytics Platform (GCP + Databricks)
**Author:** Sohail Tanveer
**Date of assessment:** 2026-08-18
**Status:** Phase 0 complete and validated · Phase 1 complete (R2)
**Revision:** R2 — scope narrowed to **batch only**; **Docker Desktop** source estate adopted

**Confirmed environment constraints (2026-08-18):**

- **No GCP Organization available.** Personal `@gmail.com` account, no domain, no Cloud Identity.
  → The GCP-native classic Databricks workspace is **permanently out of scope**. It is documented
  as an architecture chapter, never deployed. Finding A in §1 is now a settled constraint, not a risk.
  → The Databricks Free Trial's GCP-native path is unavailable; only the *serverless* trial
  workspace is reachable, which limits the §9 burn plan to serverless-visible features.
- **Fresh GCP account, $300 / 90-day credit unused.** Budget alert at $1 must be set on day one of
  Phase 2. The `e2-micro` VM (Postgres + SFTP), Pub/Sub, Cloud Run and Secret Manager are all
  comfortably affordable; the design still targets **$0 steady-state** so the project survives
  after the credit expires.

> ⚠️ **Read §13 first.** Sections 1–12 record the original Phase 0 assessment and are kept as a
> historical record. **R2 superseded parts of it**: scope is now batch only, and the source estate
> moved from GCP compute (Cloud Run, `e2-micro`, Pub/Sub) into Docker Desktop. Where this document
> and `ARCHITECTURE.md` disagree, `ARCHITECTURE.md` wins.

> Everything below was verified against official Databricks / Google Cloud documentation on
> 2026-08-18. Platform capabilities in this space change fast. Every claim is tagged with a
> confidence level and a source. **Re-verify anything marked `[VERIFY]` before you build on it.**

---

## 1. Executive summary — read this first

Two findings materially change the shape of the project.

### Finding A — The "real" GCP-native Databricks path is probably closed to you (HIGH confidence)

The Databricks **Free Trial on Google Cloud** requires *"a Google Cloud project with an
associated **Organization**"* and a Google account *"enabled for Google Workspace or Cloud
Identity."*

A personal `@gmail.com` account has **no GCP Organization**. Projects created under a personal
Gmail live outside any org. To get an Organization you must own a domain and set up Cloud
Identity (a free tier exists, but it requires a domain you control plus DNS verification).

**Consequence:** the classic, GCP-native Databricks workspace (deployed into *your* GCP project,
running GKE + GCE under your billing account) is either unavailable to you or requires domain
ownership, a real credit card, and direct Google billing that is **not covered by Databricks
trial credits**.

### Finding B — Free Edition is serverless-only and Databricks-managed (HIGH confidence)

Databricks **Free Edition** (the permanent free tier that replaced Community Edition) is:

- serverless compute only — no custom compute, no cluster configuration
- one workspace, one metastore, **no account console, no account-level APIs**
- outbound internet restricted to an allow-list of trusted domains
- Python and SQL only — **R and Scala unsupported**
- non-commercial use only, no SLA
- hard caps: **1 SQL warehouse (2X-Small)**, **max 5 concurrent job tasks**, **1 active pipeline
  per pipeline type**, and a daily compute quota that shuts the workspace down when exceeded

### The resulting recommendation

**Build on Databricks Free Edition as the compute/lakehouse plane, and use your own GCP project
(on the $300 / 90-day trial plus the Always Free tier) as the source-system plane.**

```
   YOUR GCP PROJECT (personal Gmail, $300 trial + Always Free)
   ├── Cloud Run        → Customer REST API (synthetic source system)
   ├── e2-micro VM      → Postgres "operational DB" + CDC extracts + SFTP
   ├── GCS bucket       → landing zone: files, CDC extracts, SFTP drop
   ├── Pub/Sub          → clickstream / order events
   └── Secret Manager   → credentials (never in Git)
                    │
                    │  Unity Catalog storage credential + external location
                    ▼
   DATABRICKS FREE EDITION (serverless, UC-enabled)
   ├── Bronze / Silver / Gold Delta tables
   ├── Auto Loader, Structured Streaming, MERGE, CDF
   ├── Lakeflow Jobs orchestration
   └── Databricks SQL warehouse + dashboards
```

### The one thing that must be validated before anything else

**`[VERIFY — SPIKE 1, blocking]` Can a Databricks *Free Edition* workspace create a Unity Catalog
storage credential and external location pointing at a **GCS** bucket?**

Why this is uncertain:

- Databricks docs confirm Free Edition **does** support external locations via Unity Catalog
  (confirmed by a Databricks Community Manager for the AWS case).
- On GCP-hosted Databricks, a GCS storage credential works by Databricks generating a **Google
  Cloud service account** for you; you then grant it `Storage Legacy Bucket Reader` and
  `Storage Object Admin` on your bucket.
- **But** Free Edition signup does not clearly let you choose the hosting cloud, and if your Free
  Edition workspace turns out to be AWS-hosted, the storage-credential UI may only offer AWS IAM
  roles.

This is **Task 1 of Phase 2** and it gates the whole GCP story. Fallbacks are in §7.

---

## 2. Free Edition vs Free Trial — verified comparison

| | **Free Edition** | **Free Trial (GCP)** |
|---|---|---|
| Cost | Free forever, daily quotas | Up to **$400** Databricks credits, **14 days** |
| Compute | **Serverless only**, small sizes | Full platform, serverless *or* classic |
| Deployment | Databricks-managed | Serverless trial workspace (Databricks-managed) **or** classic workspace in *your* GCP project |
| Prerequisites | Email / Google / Microsoft sign-in | **GCP project with an Organization** + Google Workspace or Cloud Identity + active billing account + Billing Administrator IAM |
| Unity Catalog | Yes (1 metastore, auto-created) | Yes |
| Account console | **No** | Yes |
| Account-level APIs | **No** | Yes |
| SQL warehouse | 1 × 2X-Small | Normal |
| Concurrent job tasks | **5 max** | Normal |
| Lakeflow pipelines | **1 active per pipeline type** | Normal |
| Scala / R | **Not supported** | Supported |
| Outbound internet | **Restricted allow-list** (expandable via LinkedIn identity verification) | Normal |
| SSO / SCIM | No | Yes |
| Commercial use | **Prohibited** | Permitted |
| Support / SLA | None | Standard |
| Google billing | None | **Google bills you directly** for any classic-workspace GKE/VPC/GCS resources — *separate from and not covered by* Databricks trial credits |

**Verdict: build on Free Edition.** Keep the 14-day Free Trial in your pocket as a *deliberate,
time-boxed* burn at the very end (§9) to capture screenshots of features Free Edition locks out.
Do not start on the trial — you will spend it learning and have nothing left when you need it.

---

## 3. Capability-by-capability feasibility matrix

Legend: ✅ works on Free Edition · ⚠️ works with caveats · 🔶 must be simulated or downgraded · ❌ unavailable

### Databricks

| Capability | FE | Notes / caveat | Cost |
|---|---|---|---|
| Unity Catalog (catalogs, schemas, tables, views, volumes) | ✅ | 1 metastore, 1 workspace. Default catalog `workspace`. | Free |
| UC grants / RBAC / ownership | ⚠️ | Only one human identity exists. Create **service principals and groups** to *demonstrate* RBAC; you cannot run a true multi-user test. | Free |
| UC lineage | ✅ | Table and column lineage captured automatically. | Free |
| UC external location → **GCS** | `[VERIFY]` | **Spike 1. Blocking.** See §7 for fallbacks. | GCS storage |
| UC Volumes (managed) | ✅ | Strong fallback landing zone if Spike 1 fails. | Free |
| Delta Lake, MERGE, time travel, Change Data Feed | ✅ | Core; fully available. | Free |
| Auto Loader (`cloudFiles`) | ⚠️ | **Directory listing mode** is the practical mode on serverless. File-notification mode needs Pub/Sub + UC service credentials + DBR 16.2+ — treat as *documented but not implemented*. | Free |
| Structured Streaming | ⚠️ *(R2: batch only)* | Used **only** via Auto Loader with `Trigger.AvailableNow` — incremental batch on the streaming engine. Watermarks, event-time windows and continuous execution are **out of scope**. | Free (quota) |
| **Kafka** source | ✅ *(R2)* | No longer a Databricks concern. Kafka runs **locally in Docker** carrying Debezium CDC, and the sink lands Parquet in GCS. Databricks reads files, never a broker — so the outbound allow-list is irrelevant. **Kafka is now real, not simulated.** | Free |
| Lakeflow Jobs (orchestration) | ⚠️ | **Max 5 concurrent tasks.** Design the DAG mostly sequential with narrow fan-out. Retries, parameters, dependencies and notifications all work. | Free (quota) |
| Lakeflow Declarative Pipelines (ex-DLT / Spark Declarative Pipelines) | ⚠️ | **1 active pipeline per type.** Enough for one showcase pipeline with `EXPECT` expectations. Build the *main* pipeline in plain PySpark + Jobs so you are not boxed in. | Free (quota) |
| Databricks SQL warehouse | ⚠️ | Exactly **one, 2X-Small**. Fine for dashboards over a Gold layer of a few million rows. Set aggressive auto-stop. | Free (quota) |
| DBSQL / AI-BI dashboards | ✅ | Your BI layer. | Free |
| Databricks CLI + workspace REST API | ⚠️ `[VERIFY]` | Workspace-level API expected to work; **account-level API is explicitly unavailable**. Spike 2. | Free |
| Databricks Asset Bundles (DABs) | ⚠️ `[VERIFY]` | Depends on Spike 2. This is the **recommended 2026 deployment path** — note Databricks is moving DABs to a *Direct Deployment Engine* and deprecating the internal Terraform engine during 2026. | Free |
| Terraform (`databricks` provider) | ⚠️ | **Workspace-scoped only** (catalogs, schemas, grants, jobs, warehouses). Account-scoped resources (metastore, workspace creation, users) are **impossible** on FE — write them as code you *would* run in a real account, do not try to apply them. | Free |
| Git folders (Repos) | ✅ | Connect GitHub with a PAT. | Free |
| Databricks Secrets (secret scopes) | ⚠️ `[VERIFY]` | Spike 2. If unavailable, use GCP Secret Manager with short-lived tokens. | Free |
| Scala / R | ❌ | Python + SQL only. Irrelevant here. | — |
| Delta Sharing / Clean Rooms / online tables | ❌ | Out of scope. | — |
| Model serving / Vector Search | ❌ / ⚠️ | Out of scope. | — |

### Google Cloud

| Service | Free? | Use here | Trap |
|---|---|---|---|
| Cloud Storage | **5 GB-months Standard, `us-east1` / `us-west1` / `us-central1` only** | Landing zone, CDC extracts, SFTP drop | Any other region is paid. Nearline/Coldline early-delete fees. **Egress to Databricks if cross-cloud is ≈ $0.12/GB.** |
| Pub/Sub | 10 GB/month free `[VERIFY]` | Event stream source | Unacked messages are retained and billed as storage |
| Cloud Run | 2M requests/month free | Customer REST API | `min-instances > 0` means always-on billing |
| Secret Manager | 6 active secret versions + 10k access ops free | Credentials | Extra versions billed |
| Cloud Scheduler | 3 jobs free | Trigger the event producer | — |
| Cloud SQL | ❌ **No free tier** | — | **COST TRAP.** Use Postgres on the free `e2-micro` VM, or DuckDB/SQLite in GCS |
| Compute Engine `e2-micro` | 1/month free, `us-*` regions | SFTP server, Postgres | Only one; 30 GB disk cap |
| Cloud Logging | 50 GB/project/month free | Pipeline logs | Beyond that, billed |
| BigQuery | 1 TB query + 10 GB storage/month free | *Optional* — a "why not BigQuery only?" comparison | Easy to burn 1 TB with `SELECT *` |
| **GCP $300 credit** | 90 days | Safety net | **Do not enable auto-upgrade to paid.** Set a $1 budget alert on day one. |

---

## 4. COST WARNING — the traps that will actually bill you

Ranked by likelihood × damage.

1. **🔴 Deploying a *classic* Databricks workspace into your own GCP project.**
   This provisions a GKE cluster, VPC, subnets and GCS buckets. **Google bills you directly**,
   during *and after* the trial, and Databricks trial credits **do not cover it**. A GKE control
   plane alone runs roughly $70+/month before any worker nodes. **Never do this.**
2. **🔴 GCS bucket in the wrong region.** Always Free Cloud Storage is `us-east1`, `us-west1` and
   `us-central1` only, Standard class, 5 GB. Anything else bills from the first byte.
3. **🟠 Cross-cloud egress.** If your Free Edition workspace turns out to be AWS-hosted and reads
   from GCS, every byte is GCS internet egress (≈ $0.12/GB). 50 GB of reprocessing ≈ $6.
   Manageable, but keep the landing zone small and cache into Delta.
4. **🟠 An always-on Structured Streaming job.** On Free Edition it burns the daily quota and gets
   the workspace suspended; on the Trial it burns credits at full rate. **Always use bounded
   triggers.**
5. **🟠 Cloud SQL.** No free tier. `db-f1-micro` still bills ≈ $8-10/month. Avoid entirely.
6. **🟠 SQL warehouse left running.** Set auto-stop to the minimum (10 minutes).
7. **🟡 Pub/Sub subscription with no subscriber.** Messages accumulate and are billed as storage.
   Set a short message retention.
8. **🟡 GCP trial auto-upgrade.** At the end of 90 days Google prompts you to upgrade. Decline.
   Set a **$1 budget alert** on day one so any charge reaches you immediately.
9. **🟡 Generating 10M records as files in GCS.** 10M order_items as JSON ≈ 3-4 GB — that blows
   the 5 GB free tier. Generate large volumes **directly in Spark**, not as files (§5).

**Realistic total cost if you follow this plan: $0.**
Worst realistic case with sloppy egress: **under $15.**

---

## 5. Data volume and compute sizing

Free Edition serverless is small and quota-limited. "5-10M records" as *files* is both impractical
and pointless here — it proves nothing extra and risks quota suspension.

**Recommended three-tier profile, all generator-configurable:**

| Profile | Orders | Order items | Events | Landing size | Purpose |
|---|---|---|---|---|---|
| `small` | 10 K | ~35 K | 100 K | ~50 MB | Unit and integration tests, CI |
| `medium` (default) | 500 K | ~1.8 M | 3 M | ~1.2 GB | The build-and-demo dataset |
| `large` (one-off) | 3 M | ~10 M | 15 M | **generated in-Spark, never written as source files** | A single documented benchmark run for the performance chapter |

Total across all entities at `medium` is roughly **6M rows** — enough to make partitioning,
`OPTIMIZE`, liquid clustering, broadcast joins and shuffle tuning produce *measurable,
screenshot-able* differences, which is the actual point of the exercise.

For `large`, generate with `spark.range(10_000_000)` plus deterministic expressions inside
Databricks. No GCS cost, no egress, and it still exercises real shuffle behaviour. Document this
choice — it is itself a good senior-engineer answer.

**Honest resume framing:** "~6M synthetic records across 11 entities, with a documented 10M-row
scale benchmark." Do not claim TB-scale.

---

## 6. Scope: Must / Should / Nice / Simulated

### MUST BUILD — the project is not credible without these

- Unity Catalog: catalog-per-environment layout, schemas, grants, ownership, lineage
- Medallion Bronze / Silver / Gold on Delta
- **Config-driven ingestion framework** (YAML or metadata table → one engine, N entities) — *the single most senior-level thing in this project*
- Auto Loader incremental file ingestion with checkpoints and schema evolution
- CDC: insert / update / delete → Delta `MERGE`, with Change Data Feed
- SCD Type 1 and SCD Type 2 (surrogate keys, effective dates, `is_current`)
- Star-schema Gold: 6 dimensions, 5 facts, documented grain and key strategy
- Data quality framework: rule config, valid / invalid / quarantine split, results table
- Source-to-target reconciliation with a results table
- Reusable Python package under `src/` — notebooks orchestrate only
- Lakeflow Jobs DAG with retries, parameters, dependencies, alerts
- Audit tables: `pipeline_run_audit`, `data_quality_results`, `reconciliation_results`
- pytest unit and integration tests running in CI on the `small` profile
- GitHub Actions CI/CD: lint → test → validate → deploy
- Terraform for workspace-scoped Unity Catalog objects
- DBSQL dashboards
- Full documentation set, Mermaid diagrams, interview Q&A

### SHOULD BUILD

- GCS external location (pending Spike 1)
- Cloud Run REST API as a genuine API source with pagination and auth
- Databricks Asset Bundles as the deployment mechanism
- One Lakeflow Declarative Pipeline with `EXPECT` expectations, to showcase the declarative style
- Performance benchmark chapter with before/after numbers
- PII tagging plus column masks and row filters in Unity Catalog

### NICE TO HAVE

- BigQuery side-by-side comparison ("why not BigQuery only?") — strong interview material
- `e2-micro` SFTP server for a genuine SFTP source
- `dbldatagen` integration for the generator
- Liquid clustering vs partitioning comparison

### SIMULATED — and clearly labelled as such in the repo

- **Multi-environment DEV / TEST / PROD** → three catalogs (`ecom_dev`, `ecom_test`, `ecom_prod`) in one workspace plus environment-parameterised config. Real separation means separate workspaces: documented, not built.
- **Multi-user RBAC** → service principals and groups; only one human identity exists.
- **On-prem SQL Server** → **Docker Postgres 16 with Debezium WAL capture** (R2). This is now a *real* CDC implementation, not a simulation — the only simulated part is which RDBMS it is.
- **Production alerting (PagerDuty and similar)** → job email notifications plus an audit table.

Every simulation gets a `> **SIMULATED:**` callout in the docs explaining what production would do
instead. **Interviewers respect this. They do not respect quiet hand-waving.**

---

## 7. Fallback plan if Spike 1 (GCS external location) fails

**Option A — UC Volumes as the landing zone.**
Push files from GCP into a Unity Catalog managed Volume via the Databricks Files API
(`databricks fs cp` or REST) from a Cloud Run job or GitHub Action. Auto Loader reads the Volume
path. *Trade-off:* loses the "Databricks reads directly from GCS" story, keeps every other
capability including Auto Loader. **Recommended fallback.**

**Option B — Service-account-key Spark conf.**
Classic Databricks reads GCS via `google.cloud.auth.service.account.json.keyfile`. Serverless plus
Unity Catalog heavily restricts Spark confs, so this will likely fail. Low probability — try second.

**Option C — Burn the 14-day Free Trial to prove the GCS integration.**
Create the GCS external location on a serverless *trial* workspace, screenshot it thoroughly,
write it up, then continue the build on Free Edition using Option A. This converts a limitation
into a documented, evidenced design decision.

**Do not let Spike 1 failing stop the project.** GCP still supplies the source systems either way,
so "GCP + Databricks" stays honest.

---

## 8. Implementation roadmap

| Phase | Deliverable | Estimate |
|---|---|---|
| **0** | *This document* | ✅ done |
| **1** | Business, functional and non-functional requirements; architecture; technology rationale; security model; cost strategy; Mermaid diagrams | 1 session |
| **2** | GCP project + budget alert + GCS + Free Edition signup + **Spikes 1 and 2** + Git + local dev + CLI auth | 1-2 sessions |
| **3** | Synthetic data generator (`small`/`medium`/`large`, with duplicates, nulls, late and out-of-order records, invalid rows, updates, deletes, schema drift) + validation | 2 sessions |
| **4** | Bronze: config-driven ingestion framework + Auto Loader + metadata + idempotency | 2 sessions |
| **5** | Silver: standardise, dedup, DQ, quarantine, CDC MERGE, SCD1 and SCD2 | 3 sessions |
| **6** | Gold: dimensions, facts, surrogate keys, aggregates | 2 sessions |
| **7** | ~~Streaming~~ — **removed in R2.** Effort redirected to the Docker estate and local test coverage | — |
| **8** | Lakeflow Jobs DAG, parameters, retries, alerts | 1 session |
| **9** | Unity Catalog governance: catalogs, grants, PII tags, masks, lineage | 1 session |
| **10** | Tests, audit, reconciliation, monitoring dashboard | 2 sessions |
| **11** | Terraform (workspace-scoped) + Databricks Asset Bundles | 1-2 sessions |
| **12** | GitHub Actions CI/CD | 1 session |
| **13** | Performance benchmarks and optimisation writeup | 1-2 sessions |
| **14** | DBSQL dashboards | 1 session |
| **15** | README, docs, diagrams, screenshots, resume bullets, interview Q&A | 2 sessions |

**Roughly 22-26 focused sessions.** No phase starts until the previous one is validated.

---

## 9. Free Trial burn plan — do this LAST

Reserve the 14-day / $400 trial for a single deliberate week near the end of the build, to capture
what Free Edition cannot show:

- GCS external location and storage credential (if Spike 1 failed)
- Multiple concurrent job tasks / a wide DAG
- Multiple Lakeflow pipelines
- A larger SQL warehouse for a real performance comparison
- Account console and account-level Terraform screenshots

Screenshot everything, then let it lapse. **Never deploy a classic workspace.**

> **Constraint (confirmed):** with no GCP Organization, only the *serverless* trial workspace is
> reachable. Anything on this list that requires a classic, GCP-project-hosted workspace is
> unavailable and must be covered by documentation rather than screenshots.

---

## 10. What this project demonstrates on your resume

Your existing GCS / Spark / SQL / medallion / CI-CD / dimensional-modelling experience transfers
directly. This project *re-platforms* skills you already have onto the toolchain that job
descriptions are asking for, which is exactly the right move — it is not starting from zero.

New, hiring-manager-relevant signals it adds:

| Signal | Evidence in the project |
|---|---|
| Databricks platform depth | Unity Catalog, Delta, Auto Loader, Lakeflow Jobs, DBSQL, serverless |
| Delta Lake vs Iceberg fluency | You know Iceberg; the docs will contrast both — rare and valuable |
| CDC and SCD2 at implementation level | `MERGE`, Change Data Feed, surrogate keys, effective dating |
| Framework thinking, not script thinking | Config-driven ingestion: one engine, N entities |
| Streaming semantics | Event time, watermarks, dedup, exactly-once reasoning |
| Data quality and reconciliation as first-class concerns | Quarantine, DQ results, source-to-target checks |
| Governance and PII | Unity Catalog RBAC, tags, masks, least privilege |
| IaC and CI/CD for data | Terraform + DABs + GitHub Actions |
| Cost engineering | Documented, quantified, with trade-offs |
| Engineering honesty | Every simulation labelled and justified |

That last row matters more than people expect. A portfolio project that says *"this is simulated
because Free Edition restricts outbound networking; here is what production would do instead"*
reads as senior. One that quietly pretends becomes a liability the moment an interviewer probes.

---

## 11. Phase 1 deliverables — the next step

1. `docs/BUSINESS_REQUIREMENTS.md` — company scenario, stakeholders, the 13 business questions mapped to Gold tables, SLAs
2. `docs/ARCHITECTURE.md` — full target architecture with every technology choice justified, plus a GCP → Databricks concept mapping including **where the mapping breaks down**
3. `docs/DATA_MODEL.md` — conceptual model, all 7 source-system contracts, star schema, grain, key strategy, SCD type assigned per dimension
4. Non-functional requirements — freshness SLA, DQ thresholds, recovery objectives, idempotency contract
5. `docs/SECURITY.md` (design only) — PII classification, RBAC matrix, secret handling
6. `docs/COST.md` — the budget model from §4
7. Mermaid diagrams 1-4: overall, batch, streaming, medallion
8. Final repository structure decision, with rationale for any deviation from the proposed layout

Phase 1 is **design only** — no implementation code.

---

## 12. Sources

- [Databricks Free Edition limitations (GCP)](https://docs.databricks.com/gcp/en/getting-started/free-edition-limitations)
- [Databricks Free Edition limitations (AWS)](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Sign up for Databricks for free — GCP free trial](https://docs.databricks.com/gcp/en/getting-started/free-trial)
- [Free Trial vs Free Edition](https://docs.databricks.com/gcp/en/getting-started/free-trial-vs-free-edition)
- [Databricks Free Edition signup (GCP)](https://docs.databricks.com/gcp/en/getting-started/free-edition)
- [Connect to a GCS external location (Unity Catalog)](https://docs.databricks.com/gcp/en/connect/unity-catalog/cloud-storage/external-locations-gcs)
- [Unity Catalog requirements and limitations (GCP)](https://docs.databricks.com/gcp/en/data-governance/unity-catalog/requirements)
- [Serverless compute limitations (GCP)](https://docs.databricks.com/gcp/en/compute/serverless/limitations)
- [Auto Loader file notification mode (GCP)](https://docs.databricks.com/gcp/en/ingestion/cloud-object-storage/auto-loader/file-notification-mode)
- [Connect to Apache Kafka (GCP)](https://docs.databricks.com/gcp/en/connect/streaming/kafka)
- [Lakeflow Spark Declarative Pipelines release notes 2026](https://docs.databricks.com/gcp/en/release-notes/dlt/2026)
- [Service principals for CI/CD (GCP)](https://docs.databricks.com/gcp/en/dev-tools/auth/service-principals)
- [Databricks Terraform provider](https://registry.terraform.io/providers/Databricks/databricks/latest/docs)
- [Databricks Community — Free Edition and external locations](https://community.databricks.com/t5/data-governance/if-use-databricks-free-version-not-free-trail-can-use-external/m-p/127421)


---

## 13. R2 amendment — batch only, Docker source estate

Two decisions after Phase 1 changed the shape of the build. This section records them; the detail
lives in `ARCHITECTURE.md` §1–§4 and `COST.md` §1.

**Batch only.** The streaming path, Pub/Sub, watermarking and the `rt_*` marts are removed.
Justified by portfolio context: `real-time-analytics-on-gcp` already demonstrates streaming.
Auto Loader still runs on the Structured Streaming engine in incremental-batch mode, so
checkpointing, exactly-once file handling and schema evolution all remain in scope — described
accurately as *incremental batch*, never as a streaming SLA.

**Docker Desktop available (16 GB Windows).** This resolved three things at once:

| Phase 0 concern | R2 resolution |
|---|---|
| "Kafka must be simulated — outbound allow-list" | **Kafka is now real**, running locally, carrying Debezium CDC |
| "CDC uses a synthetic `op` column" | **Real Debezium capture of the Postgres write-ahead log**, LSN-ordered |
| "Free Edition daily quota limits iteration" | **Local Spark container** — most development consumes no Databricks quota at all |

**Spike 1 is unchanged and still blocking.** The GCS external location question is independent of
both decisions, and the fallback remains UC Volumes.

**New risk introduced:** local resource exhaustion replaces cloud spend as the binding constraint —
container RAM, Docker disk growth, and orphaned Debezium replication slots inflating the Postgres
WAL. Tracked as traps 9–11 in `COST.md` §6.
