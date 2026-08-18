# Phase 2 — Environment Setup

**Goal:** a working Docker source estate, a GCP landing zone, a Databricks Free Edition workspace,
and two spikes answered — before any pipeline code is written.

**Order matters.** Docker first (no external dependencies, fastest feedback), then GCP, then
Databricks, then the spikes. Track 1 and Track 2 are independent, so start Docker building and do
the GCP work while images pull.

---

## Track 1 — Docker source estate

### 1.1 Check Docker Desktop

```bash
docker --version && docker compose version && docker info --format '{{.MemTotal}}'
```

`MemTotal` is what containers can actually use. On a 16 GB machine WSL2 typically offers ~8 GB.
Below ~6 GB, raise it — create or edit `%UserProfile%\.wslconfig`:

```ini
[wsl2]
memory=10GB
processors=4
```

Then `wsl --shutdown` and restart Docker Desktop.

### 1.2 Create your local env file

```bash
cd enterprise-lakehouse-on-databricks/docker && cp .env.example .env
```

Edit `docker/.env` and change every `change_me_locally` value. These are local-only container
credentials, not production secrets — but reusing a password you use elsewhere is still a bad
habit to build. `.env` is gitignored; only `.env.example` is committed.

### 1.3 Bring up the core profile

```bash
docker compose --profile core up -d --build
```

First build pulls Postgres, Alpine, Python and builds two images — expect 3–6 minutes.

**Validate:**

```bash
docker compose --profile core ps
```

Expected: `np-postgres` and `np-commerce-api` **healthy**, `np-sftp` running.

### 1.4 Verify Postgres and the Debezium prerequisites

```bash
docker exec -it np-postgres psql -U northpeak -d northpeak -c "\dt erp.*" -c "SHOW wal_level;" -c "SELECT pubname FROM pg_publication;"
```

Expected: three `erp` tables, `wal_level = logical`, publication `northpeak_erp`.

If `wal_level` says `replica`, the `command:` override in `docker-compose.yml` is not being
applied and Debezium will fail later. Fix it now.

### 1.5 Verify the Commerce API

```bash
curl -s localhost:8000/health
```

```bash
curl -s -H "Authorization: Bearer local_dev_token_change_me" "localhost:8000/api/v1/customers?page_size=5" | python -m json.tool
```

Expected: `{"status":"ok"}`, then `{"data": [], "pagination": {...}}` — **empty is correct**. The
tables have no rows until Phase 3 seeds them. What you are proving here is auth, routing and
pagination structure.

Also confirm the failure injection works — repeat the call ~20 times and expect roughly one 500.
If you never see one, `API_ERROR_RATE` is not being read, and Phase 4's retry logic will go
untested.

### 1.6 Bring up the CDC profile

```bash
docker compose --profile core --profile cdc up -d --build
```

```bash
chmod +x scripts/*.sh && ./scripts/register_debezium.sh
```

**Validate — the connector is RUNNING, not just created:**

```bash
./scripts/register_debezium.sh status
```

Expected: connector state `RUNNING` and one task `RUNNING`. A connector that registers but whose
task is `FAILED` is the common outcome, and the task trace is where the real error is.

**Validate — the replication slot exists:**

```bash
docker exec -it np-postgres psql -U northpeak -d northpeak -c "SELECT slot_name, plugin, active FROM pg_replication_slots;"
```

Expected: `northpeak_slot`, plugin `pgoutput`, `active = t`.

**Prove CDC actually captures a change:**

```bash
docker exec -it np-postgres psql -U northpeak -d northpeak -c "INSERT INTO erp.orders (order_id, customer_id, order_date, order_status, payment_status, shipping_status) VALUES ('ORD-SPIKE-1','CUST-1',now(),'PLACED','PENDING','NOT_SHIPPED');"
```

```bash
docker exec -it np-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic northpeak.erp.orders --from-beginning --max-messages 1
```

Expected: a JSON envelope with `"op":"c"`, an `after` object, and a `source` block containing
`lsn`. **Seeing that envelope is the moment CDC stops being simulated.**

### 1.7 Build the dev profile

```bash
docker compose --profile dev up -d --build
```

```bash
docker exec -it np-spark-dev python -c "
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
b = SparkSession.builder.appName('smoke').master('local[2]') \
  .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
  .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
s = configure_spark_with_delta_pip(b).getOrCreate()
s.range(5).write.format('delta').mode('overwrite').save('/tmp/smoke')
print('delta rows:', s.read.format('delta').load('/tmp/smoke').count())
s.stop()"
```

Expected: `delta rows: 5`. **This is the milestone that separates this project from its sibling** —
real JDK, real Spark, real Delta, actually executing.

### 1.8 Shut down cleanly

```bash
docker compose --profile core --profile cdc --profile dev down
```

Use `down -v` only when you intend to wipe Postgres and re-run the init scripts. Note that `down`
leaves the replication slot inside the volume; `down -v` removes it with the volume.

---

## Track 2 — Google Cloud

> **COST WARNING.** Everything below stays inside the Always Free tier. The one action that would
> cost real money — deploying a classic Databricks workspace into this project — is never
> performed. See `COST.md` §4 rule 1.

### 2.1 Install and authenticate gcloud

```bash
gcloud version && gcloud auth login
```

### 2.2 Run the setup script

```bash
chmod +x scripts/setup_gcp.sh && ./scripts/setup_gcp.sh
```

It creates the project, links billing, **creates the $1 budget alert before any billable
resource**, enables Storage and Secret Manager, creates the bucket in `us-central1` with uniform
access, versioning and a 14-day lifecycle rule, then sets up ADC.

If the budget step fails on permissions, **create it manually in the console before continuing**.
It is the single control that turns an unnoticed mistake into a same-day email.

### 2.3 Validate

```bash
gcloud storage ls && gcloud storage buckets describe gs://$GCS_BUCKET --format="value(location,storageClass,iamConfiguration.uniformBucketLevelAccess.enabled)"
```

Expected: `US-CENTRAL1`, `STANDARD`, `True`. A different location means you are outside the free
tier — recreate the bucket rather than living with it.

### 2.4 Record the values

Put `GCP_PROJECT_ID` and `GCS_BUCKET` into `docker/.env`.

---

## Track 3 — Databricks Free Edition

### 3.1 Sign up

Go to the Databricks Free Edition signup page and register. A workspace is created automatically
with serverless compute, default storage and a Unity Catalog metastore (default catalog
`workspace`).

Do **not** start the 14-day Free Trial. It is reserved for a deliberate burn week at the end —
`PHASE0-FEASIBILITY.md` §9.

### 3.2 Verify identity to raise limits

Free Edition raises some limits after LinkedIn identity verification, including the outbound
domain allow-list. Do it now; it costs nothing and avoids a confusing block later.

### 3.3 Connect Git

Settings → Linked accounts → Git integration. Add a GitHub personal access token with `repo`
scope. Then Workspace → Create → Git folder, pointing at
`https://github.com/Sohailtanveer1/enterprise-lakehouse-on-databricks`.

### 3.4 Install and authenticate the CLI

```bash
databricks auth login --host https://<your-workspace-host>
```

```bash
databricks current-user me
```

---

## Track 4 — The spikes

### Spike 1 — GCS external location (BLOCKING)

Open `scripts/spike1_gcs_external_location.py` and work through blocks 1–7 as notebook cells.

**The decisive check is block 1.** In Catalog Explorer → External Data → Credentials → Create
credential, look at the Credential Type dropdown:

| You see | Meaning | Action |
|---|---|---|
| **GCP Service Account** | Workspace is GCP-hosted | Continue — the spike will almost certainly pass |
| **Only AWS IAM Role** | Workspace is AWS-hosted | **FAIL.** Stop. Take the Volumes fallback immediately — do not burn hours |

**Record the outcome here:**

```
SPIKE 1 RESULT: [ ] PASS   [ ] FAIL
Date:
Workspace host:
Credential types offered:
Notes:
```

**If FAIL** → Option A in `PHASE0-FEASIBILITY.md` §7: push files into a Unity Catalog Volume via
the Files API; Auto Loader reads the Volume path. Auto Loader, CDC, SCD2, Gold, governance and
CI/CD are all unaffected. Update `ARCHITECTURE.md` §3 to redraw one edge, and state the constraint
plainly in the README — a documented limitation reads as senior, a hidden one does not.

### Spike 2 — automation surface (not blocking)

```bash
./scripts/spike2_workspace_api.sh
```

**Record the outcome:**

```
SPIKE 2 RESULT
  workspace REST API .... [ ] PASS [ ] FAIL
  service principals .... [ ] PASS [ ] FAIL   (expected FAIL)
  secret scopes ......... [ ] PASS [ ] FAIL
  Notes:
```

Service principals failing is expected and fine — CI/CD then uses a PAT in GitHub secrets, and
`CI_CD.md` must say so rather than implying otherwise.

---

## Phase 2 exit criteria

Do not start Phase 3 until every line is ticked.

- [ ] `docker compose --profile core ps` shows postgres and commerce-api healthy
- [ ] `wal_level = logical` and publication `northpeak_erp` exist
- [ ] Commerce API returns 200 with auth, 401 without, and ~5% 500s
- [ ] Debezium connector and task both `RUNNING`
- [ ] Replication slot `northpeak_slot` active
- [ ] A real INSERT produced a Debezium envelope on the Kafka topic
- [ ] `spark-dev` wrote and read a Delta table
- [ ] GCS bucket exists in `us-central1`, Standard, uniform access, lifecycle set
- [ ] **$1 budget alert is live** (non-negotiable)
- [ ] Databricks Free Edition workspace reachable; CLI authenticated
- [ ] Git folder connected
- [ ] **Spike 1 recorded** — PASS or FAIL, with the fallback decision made
- [ ] Spike 2 recorded

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `wal_level` is `replica` | `command:` override not applied | Check compose indentation; `down -v` and recreate |
| Debezium task `FAILED`, "replication slot already exists" | Slot left by a previous run | `SELECT pg_drop_replication_slot('northpeak_slot');` then re-register |
| Debezium task `FAILED`, "must be superuser or replication role" | `02_debezium_user.sh` did not run | Init scripts run **only on first init**. `down -v` and recreate |
| No messages on the topic | Publication missing the table, or no changes yet | Check `pg_publication_tables`; make an INSERT |
| Postgres disk growing fast | Orphaned replication slot pinning WAL | Drop unused slots. `max_slot_wal_keep_size=1GB` caps the damage |
| `spark-dev` fails: "JAVA_HOME not set" | JRE package name changed upstream | Exec in and check `/usr/lib/jvm/` |
| Spark hangs on start | Delta JARs downloading, or offline | JARs are pre-fetched at build; rebuild without `--no-cache` |
| Delta version mismatch errors | pyspark/delta-spark pairing wrong | delta-spark 3.2.x ↔ Spark 3.5.x. Check the Delta release notes |
| `${APPDATA}` empty in compose | Running from Git Bash, not PowerShell | Set `APPDATA` explicitly or use an absolute path for the ADC mount |
| Containers OOM-killed | WSL2 memory too low | Raise it in `.wslconfig`; run fewer profiles |
| Everything slow, disk full | Accumulated images and volumes | `docker system prune` and `docker system df` |

---

## What Phase 3 needs from this phase

The synthetic data generator writes into **all** of these, so each must be reachable:

1. Postgres `erp.*` — inserts, updates and deletes flowing through Debezium
2. Postgres `commerce.*` — served by the Commerce API
3. SFTP upload directory — daily PIM snapshots
4. GCS landing zone — WMS feeds and clickstream files
5. `spark-dev` — validating the generated data before it is used
