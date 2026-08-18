# Cost Design

**Phase:** 1 — design · **Revision:** R2 (batch only, Docker source estate)
**Confirmed context:** personal Gmail (no GCP Organization), fresh GCP account with the **$300 /
90-day credit unused**, Databricks **Free Edition**, **Docker Desktop on 16 GB Windows**.

**Design target: $0 steady state.** The $300 credit is a safety margin, not a budget. A portfolio
project that stops working when a credit expires has failed as a portfolio project.

---

## 1. What R2 changed

Moving the source estate into Docker removed every remaining GCP compute dependency:

| R1 component | R1 tier | R2 replacement | Effect |
|---|---|---|---|
| Cloud Run — Commerce API | Always Free (2M req) | **Docker FastAPI** | One less service to configure, no cold-start quirks |
| `e2-micro` VM — Postgres + SFTP | Always Free (1 instance) | **Docker Postgres + `atmoz/sftp`** | Frees the single always-free VM entirely |
| Pub/Sub — event stream | Always Free (10 GB) | **Docker file-gen → hourly files** | Streaming removed from scope |
| Cloud Scheduler | Always Free (3 jobs) | **Docker container schedules / manual runs** | One less moving part |

**GCP's remaining footprint is two services: Cloud Storage and Secret Manager.** That is the
integration boundary and nothing more, which is both cheaper and a cleaner architectural story.

Docker costs nothing but **local RAM and disk**, which is the constraint that replaced cloud spend.

---

## 2. Budget model

| Component | Tier | Design limit | Cost |
|---|---|---|---|
| Databricks Free Edition | Free forever | Daily quota (undisclosed) | **$0** |
| GCS landing zone | Always Free | 5 GB, `us-central1`, Standard | **$0** |
| Secret Manager | Always Free | ≤ 6 active versions | **$0** |
| Docker Desktop | Personal use licence | ~6 GB RAM peak, ~15 GB disk | **$0** |
| GitHub Actions | Free (public repo) | Unlimited minutes on public repos | **$0** |
| **Steady-state total** | | | **$0 / month** |

**Realistic worst case with mistakes:** under $10, essentially all of it GCS egress if the Free
Edition workspace turns out to be AWS-hosted and reprocessing is heavy. Lower than R1's $15 because
there is no longer any GCP compute to leave running.

> **Docker Desktop licensing:** free for personal use, education and small businesses. Paid
> subscription required for larger commercial organisations. Personal portfolio use is covered.

---

## 3. Local resource budget — the new binding constraint

16 GB total; roughly 8 GB reaches containers under default WSL2 settings.

| Profile | Containers | ~RAM | ~Disk |
|---|---|---|---|
| `core` | postgres, commerce-api, sftp, file-gen | ~1.5 GB | ~2 GB |
| `cdc` | + kafka, debezium-connect | ~2.5 GB | ~3 GB |
| `dev` | + spark-dev | ~2–3 GB active | ~2 GB image |
| `test` | + fake-gcs-server | ~0.2 GB | ~0.1 GB |
| **Peak (`core+cdc+dev`)** | | **~6 GB** | **~8 GB** |

Controls:

- **Never run all profiles at once by habit.** `core` alone covers most work.
- If Docker Desktop is memory-starved, raise the WSL2 limit in `%UserProfile%\.wslconfig`:
  `[wsl2]` / `memory=10GB`.
- **Swap Kafka for Redpanda** if RAM is tight — Kafka-API compatible, no JVM, roughly half the
  footprint. "Kafka protocol" stays an honest description.
- `docker system prune` periodically. Postgres WAL and Kafka log segments grow; cap Kafka retention
  at a few hours and Postgres WAL with `max_slot_wal_keep_size`.
- **An orphaned Debezium replication slot will grow the Postgres WAL without limit.** Drop unused
  slots. This is the local equivalent of leaving a cloud resource running, and it is the one that
  will actually bite.

---

## 4. Hard rules

1. **Never deploy a classic Databricks workspace into a GCP project.** GKE control plane plus
   workers is the single largest cost risk in this project, billed by Google directly, and
   Databricks trial credits do not cover it.
2. **GCS bucket must be `us-central1`, `us-east1` or `us-west1`, Standard class.** Always Free is
   region- and class-specific; any other choice bills from the first byte.
3. **No Cloud SQL.** No free tier; even `db-f1-micro` runs ~$8–10/month. Postgres is in Docker.
4. **No Cloud Composer.** No free tier; smallest environment runs on the order of $100–300/month.
   Lakeflow Jobs is used instead — see ADR-05, where the primary reason is now differentiation
   rather than cost.
5. **SQL warehouse auto-stop at the minimum setting.**
6. **Budget alert at $1** configured before any GCP resource is created.
7. **Decline the GCP paid-account upgrade** when the 90-day trial ends.
8. **Drop unused Debezium replication slots.**

---

## 5. Storage budget

The 5 GB Always Free allowance is the binding cloud constraint.

| Data | Format | Size at `medium` |
|---|---|---|
| Customers + promotions (hourly incremental) | JSON | ~180 MB |
| Products + categories (daily full × 30 retained) | CSV | ~450 MB |
| Orders + order_items + payments (Debezium Parquet) | Parquet | ~750 MB |
| Inventory (daily delta) | CSV | ~700 MB |
| Shipments + returns | CSV/JSON | ~150 MB |
| Clickstream events (hourly files, gzipped) | JSON.gz | ~450 MB |
| **Total landing zone** | | **~2.7 GB** |

Headroom ~2.3 GB, better than R1's 1.5 GB — the Debezium envelope adds overhead, but gzipped hourly
event files are far smaller than raw streaming micro-batches.

Controls:
- **Lifecycle rule: delete objects older than 14 days.** Bronze is the durable copy; the landing
  zone is a transfer buffer, not an archive.
- Events written gzipped.
- The `large` (10 M row) benchmark is generated **inside Spark**, never landed as files — avoiding
  3–4 GB and any egress.

Delta storage inside Databricks uses Free Edition default storage and does not touch this
allowance. Local Delta tables in `spark-dev` sit on your disk, not in the cloud.

---

## 6. Ranked cost traps

| # | Trap | Damage | Control |
|---|---|---|---|
| 1 | Classic Databricks workspace on GCP | $70+/mo, ongoing | Never deploy one. Serverless only. |
| 2 | GCS bucket in a non-free region | Bills from byte one | Region asserted in Terraform and in the setup script |
| 3 | Cross-cloud GCS egress on heavy reprocessing | ~$0.12/GB | Small landing zone; cache into Delta; avoid repeated full replays |
| 4 | GCP trial auto-upgrade at day 90 | Unbounded | Decline; $1 budget alert |
| 5 | Cloud SQL, if ever tempted | ~$8–10/mo | Postgres is in Docker |
| 6 | Cloud Composer, if ever tempted | ~$100–300+/mo | Lakeflow Jobs |
| 7 | SQL warehouse left running | Quota burn | Minimum auto-stop |
| 8 | Generating 10 M rows as files | Blows the 5 GB free tier | Generate in Spark |
| 9 | **Orphaned Debezium replication slot** | Postgres WAL grows until the disk fills | Drop unused slots; `max_slot_wal_keep_size` |
| 10 | **Docker images and volumes accumulating** | Tens of GB of local disk | `docker system prune`; capped Kafka retention |
| 11 | **All Compose profiles running by habit** | RAM exhaustion, machine unusable | Profile discipline; `core` is usually enough |
| 12 | Large `VACUUM` retention windows | Storage growth | 30-day time travel, 7-day vacuum floor |

Traps 9–11 are new in R2 and replace the cloud-service traps that Docker eliminated. The risk did
not disappear — it moved from your billing account to your laptop.

---

## 7. Free Edition quota management

The daily compute quota is undisclosed, and exceeding it suspends the workspace for the rest of the
day. **The local Spark container is the primary defence** — most iterations never need Databricks
at all.

| Practice | Reason |
|---|---|
| **Develop and test in `spark-dev`, not on Databricks** | The single biggest quota saving available |
| Develop against the `small` profile (10 K orders) | Most iterations do not need 6 M rows |
| Promote to `medium` on Databricks only for validation runs | Reserve quota for runs that matter |
| Use `LIMIT` during exploration | Full scans burn quota invisibly |
| Cache only where reused ≥ 3 times, then uncache | Caching costs memory and gains nothing on a single pass |
| Avoid `display()` on large DataFrames | Triggers a full job |
| Batch related work into one session | Serverless start-up cost is repaid across a session |

**If suspended:** data and settings persist and the quota resets. Plan for it rather than being
surprised by it — and note that with local Spark you can keep working through a suspension, which
was not true in R1.

---

## 8. What each cost decision demonstrates

| Decision | Underlying principle |
|---|---|
| Local Spark for the development loop | Move iteration off metered infrastructure |
| Docker source estate over cloud compute | Pay for the integration boundary, not for simulated sources |
| Lakeflow Jobs over Composer | Evaluate orchestration on total cost and on what it adds |
| Generate benchmark data in-compute, not in storage | Do not pay to store data read once |
| Landing-zone lifecycle deletion | Distinguish a transfer buffer from an archive |
| Liquid clustering over partitioning on mid-size tables | Avoid the small-file problem that inflates storage and query cost |
| Compose profiles | Right-size the local environment the way you would right-size a cluster |

---

## 9. Monitoring

| Control | Setting | When |
|---|---|---|
| GCP budget alert | $1, email at 50% / 90% / 100% | Phase 2, before creating any resource |
| Free Edition usage | Databricks workspace usage view | Weekly |
| GCS object count and size | `gsutil du -s` in the ops runbook | Weekly |
| Docker disk usage | `docker system df` | Weekly |
| Postgres WAL size / replication slot lag | `pg_replication_slots` query in the runbook | After every CDC session |

A $1 budget alert is deliberately absurd. Its purpose is not to cap spending — it is to make any
unexpected charge arrive as an email within hours, while it is still one dollar.
