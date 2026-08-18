# Cost Design

**Phase:** 1 — design
**Confirmed context:** personal Gmail (no GCP Organization), fresh GCP account with the **$300 /
90-day credit unused**, Databricks **Free Edition**.

**Design target: $0 steady state.** The $300 credit is a safety margin, not a budget. A portfolio
project that stops working when a credit expires has failed as a portfolio project.

---

## 1. Budget model

| Component | Tier used | Design limit | Expected cost |
|---|---|---|---|
| Databricks Free Edition | Free forever | Daily quota (undisclosed) | **$0** |
| GCS landing zone | Always Free | 5 GB, `us-central1`, Standard | **$0** |
| Pub/Sub | Always Free | 10 GB/month, short retention | **$0** |
| Cloud Run (Commerce API + event generator) | Always Free | 2M requests/mo, `min-instances=0` | **$0** |
| Compute Engine `e2-micro` (Postgres + SFTP) | Always Free | 1 instance, `us-central1`, 30 GB disk | **$0** |
| Secret Manager | Always Free | ≤ 6 active versions | **$0** |
| Cloud Scheduler | Always Free | ≤ 3 jobs | **$0** |
| Cloud Logging | Always Free | ≤ 50 GB/month | **$0** |
| GitHub Actions | Free (public repo) | Unlimited minutes on public repos | **$0** |
| **Steady-state total** | | | **$0 / month** |

**Realistic worst case with mistakes:** under $15, dominated by GCS egress if the Free Edition
workspace turns out to be AWS-hosted and reprocessing is heavy.

---

## 2. Hard rules

1. **Never deploy a classic Databricks workspace into a GCP project.** GKE control plane plus
   worker nodes is the single largest cost risk in this entire project, it is billed by Google
   directly, and Databricks trial credits do not cover it.
2. **GCS bucket must be `us-central1`, `us-east1` or `us-west1`, Standard class.** Always Free is
   region- and class-specific. Any other choice bills from the first byte.
3. **`min-instances=0` on every Cloud Run service.** A single always-warm instance converts a free
   service into a monthly bill.
4. **No Cloud SQL.** No free tier; even `db-f1-micro` costs roughly $8–10/month. Postgres runs on
   the free `e2-micro` instead.
5. **No Cloud Composer.** Managed Airflow has no free tier and its smallest environment runs on
   the order of $100–300/month depending on sizing — disqualifying on its own. This is the
   substantive reason for ADR-05, over and above "Lakeflow Jobs is native".
6. **No always-on streaming.** Bounded triggers only.
7. **SQL warehouse auto-stop at the minimum setting.**
8. **Budget alert at $1** configured on day one of Phase 2, before any resource is created.
9. **Decline the GCP paid-account upgrade** when the 90-day trial ends.

---

## 3. Storage budget

The 5 GB Always Free allowance is the binding constraint on the landing zone.

| Data | Format | Size at `medium` |
|---|---|---|
| Customers (hourly incremental JSON) | JSON | ~180 MB |
| Products + categories (daily full CSV × 30 retained) | CSV | ~450 MB |
| Orders + order_items + payments (CDC Parquet) | Parquet | ~600 MB |
| Inventory (daily delta CSV) | CSV | ~700 MB |
| Shipments + returns | CSV/JSON | ~150 MB |
| Events (streaming micro-batches) | JSON | ~1.4 GB |
| **Total landing zone** | | **~3.5 GB** |

Headroom: ~1.5 GB. Controls:

- **Lifecycle rule: delete objects older than 14 days.** Bronze is the durable copy; the landing
  zone is a transfer buffer, not an archive.
- Events written as gzipped JSON, reducing that line by roughly 70%.
- The `large` (10 M row) benchmark is generated **inside Spark**, never landed as files — this
  alone avoids 3–4 GB and any associated egress.

Delta storage inside Databricks uses Free Edition default storage and does not touch the GCS
allowance.

---

## 4. Ranked cost traps

Ranked by likelihood × damage, carried forward from Phase 0.

| # | Trap | Damage | Control |
|---|---|---|---|
| 1 | Classic Databricks workspace on GCP | $70+/mo minimum, ongoing | Never deploy one. Serverless only. |
| 2 | GCS bucket in a non-free region | Bills from byte one | Region asserted in Terraform and checked in setup script |
| 3 | Cross-cloud GCS egress on heavy reprocessing | ~$0.12/GB | Keep landing zone small; cache into Delta; avoid repeated full replays |
| 4 | Always-on streaming job | Quota suspension / credit burn | `Trigger.AvailableNow` only |
| 5 | Cloud SQL | ~$8–10/mo | Postgres on `e2-micro` |
| 6 | Cloud Composer | ~$100–300+/mo | Lakeflow Jobs |
| 7 | SQL warehouse left running | Quota burn | Minimum auto-stop |
| 8 | Pub/Sub subscription with no subscriber | Message storage billed | 10-minute retention; drain on schedule |
| 9 | GCP trial auto-upgrade at day 90 | Unbounded | Decline; $1 budget alert |
| 10 | Generating 10 M rows as files | Blows 5 GB free tier | Generate in Spark |
| 11 | `e2-micro` in the wrong region or a second instance | Bills normally | One instance, `us-central1`, asserted in Terraform |
| 12 | Large `VACUUM` retention windows | Storage growth | 30-day time travel, 7-day vacuum floor |

---

## 5. Free Edition quota management

The daily compute quota is undisclosed, and exceeding it suspends the workspace for the rest of the
day — which is a *productivity* cost even though it is not a financial one.

| Practice | Reason |
|---|---|
| Develop against the `small` profile (10 K orders) | Most iterations do not need 6 M rows |
| Promote to `medium` only for validation runs | Reserve quota for runs that matter |
| Use `LIMIT` liberally during exploration | Full scans on every cell burns quota invisibly |
| Cache only where reused ≥ 3 times, and uncache after | Caching costs memory and gains nothing on a single pass |
| Avoid `display()` on large DataFrames | Triggers a full job |
| Batch related work into one session | Serverless start-up cost is repaid across a session |
| Run the streaming demo in short windows | Not continuously |

**If suspended:** work does not disappear — data and settings persist, and the quota resets. Plan
for it rather than being surprised by it.

---

## 6. What each cost decision demonstrates

Cost engineering is a senior competency, and this project's constraints force real decisions rather
than performative ones.

| Decision | Underlying principle |
|---|---|
| Lakeflow Jobs over Composer | Evaluate orchestration on total cost, not familiarity |
| Bounded triggers over continuous streaming | Match latency spend to the actual SLA (< 5 min, not < 5 s) |
| Generate benchmark data in-compute, not in storage | Avoid paying to store data that exists only to be read once |
| Landing-zone lifecycle deletion | Distinguish a transfer buffer from an archive |
| Liquid clustering over partitioning on mid-size tables | Avoid the small-file problem that inflates both storage and query cost |
| `small` profile for development | Right-size the feedback loop |

Each of these is a defensible interview answer about cost-aware design, which is a more useful
thing to be able to discuss than "we used serverless because it is cheaper".

---

## 7. Monitoring

| Control | Setting | When |
|---|---|---|
| GCP budget alert | $1, email at 50% / 90% / 100% | Phase 2, before creating any resource |
| GCP billing export | Optional; BigQuery free tier covers it | Phase 2 |
| Free Edition usage | Databricks workspace usage view | Weekly |
| GCS object count and size | `gsutil du -s` in the ops runbook | Weekly |

A $1 budget alert is deliberately absurd. Its purpose is not to cap spending — it is to make *any*
unexpected charge arrive as an email within hours, while it is still one dollar.
