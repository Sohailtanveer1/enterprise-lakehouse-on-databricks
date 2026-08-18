# Interview Q&A

Answers grounded in decisions actually made in this repo, with the trade-off
stated. A senior answer names what the choice *cost*; only a junior one claims
a choice was free.

---

## Platform choices

**Why Databricks rather than BigQuery alone?**
This workload has heavy row-level mutation (CDC MERGE, SCD2 version closing),
needs open storage readable by multiple engines, and needs PySpark-native
transformation. Delta on object storage covers all three with one storage layer.
**The honest counter:** at ~6M rows BigQuery would be cheaper, simpler, and
require no compute tuning at all. It wins on operational simplicity and loses on
open storage and unified transformation. **Do not claim BigQuery can't do CDC** —
it can, via `MERGE`. The distinction is storage openness, not capability.

**Why Delta Lake rather than Iceberg?**
Everything on this platform assumes Delta: Change Data Feed, liquid clustering,
deletion vectors, MERGE optimisation, Unity Catalog integration. Choosing
Iceberg means fighting the platform. **What Iceberg does better:** true hidden
partitioning and partition evolution, a pluggable catalog, broader multi-engine
adoption, and better BigQuery external-table support.

**Where does the Delta/Iceberg analogy break down?**
Metadata architecture. Delta is an ordered JSON transaction log with periodic
Parquet checkpoints; Iceberg is metadata-file → manifest-list → manifest tree.
Consequences: Iceberg gets hidden partitioning, Delta gets generated columns and
liquid clustering instead. Iceberg's catalog is central to correctness; Delta's
log is self-describing. Delta's CDF is a stored feature; Iceberg uses changelog
scans over snapshots. Concurrency control differs too — log version bump vs
metadata pointer swap.

**Why Lakeflow Jobs rather than Airflow?**
Differentiation, honestly. I've already built Airflow and Cloud Composer
orchestration in another project, so repeating it added nothing. **The real
answer for an enterprise:** Airflow is a general orchestrator with hundreds of
operators and far better backfill and sensors; Lakeflow Jobs orchestrates
Databricks work and little else. With non-Databricks steps in the estate,
Airflow calling Databricks jobs is the correct pattern. *"We use Databricks Jobs
so we don't need Airflow" is a weak answer.*

---

## Ingestion

**How does Auto Loader work?**
It's a Structured Streaming source that keeps its own file-state store (RocksDB),
so each file is processed exactly once. It handles schema inference, evolution,
and a rescued-data column for malformed fields, with checkpointed progress.

**What happens if the same file arrives twice?**
Nothing. The checkpoint's file-state store already recorded it, so it is never
re-read. That is file-level idempotency. **Row-level** duplication — the same
rows in a *differently named* file — is a separate problem, solved by dedup in
Silver, not by Auto Loader.

**What if a file arrives late?**
Auto Loader picks it up on the next trigger; it doesn't care about ordering.
The Silver dedup window then decides whether the late row wins, ordered by LSN
or watermark. That's why the ordering key matters more than arrival order.

**What if the schema changes?**
An added column: `addNewColumns` fails the stream once, records the new schema,
and succeeds on restart — which is why the ERP ingest task carries 3 retries.
A **removed or retyped** column is FATAL, because it will break downstream logic,
often silently by producing nulls. That's a human decision, not an automatic one.

**What if the job crashes halfway?**
Delta is ACID, so a failed write commits nothing. The checkpoint isn't advanced,
so the next run reprocesses those files. No partial state is visible.

**Auto Loader in local testing?**
It doesn't exist outside Databricks — `cloudFiles` is proprietary. Local uses
Spark's file streaming source behind the same function signature. Shared
semantics (checkpointing, exactly-once files) cover logic testing; schema
evolution and rescued data can only be validated on Databricks.

---

## CDC and SCD

**How did you implement CDC?**
Debezium reads the Postgres write-ahead log and publishes change events to
Kafka; a sink batches them to Parquet in GCS; Auto Loader lands them in Bronze
with the envelope intact; Silver unwraps and MERGEs them.

**Why keep the envelope in Bronze instead of flattening at the sink?**
Because flattening destroys the `before` image, and with it any chance of SCD
Type 2. Bronze's job is to preserve what the source said.

**What are the three ways CDC breaks silently?**
1. `after` is NULL on deletes — a naive `after.*` expansion drops every delete
   and the target simply never loses rows.
2. Tombstones (null value, non-null key) follow each delete and must not become
   all-null rows.
3. Ordering by `updated_at` instead of `source.lsn`.

**Why order by LSN rather than a timestamp?**
The LSN is the database's own total order over commits. Application timestamps
suffer clock skew, are shared across every row of a bulk UPDATE, and go stale on
backfills. Ordering by timestamp is a heuristic; ordering by LSN is correct.

**Walk me through a customer moving from Delhi to Mumbai.**
Postgres UPDATE → WAL → Debezium envelope with `before.city=Delhi`,
`after.city=Mumbai`, `op=u`, an LSN → Kafka → Parquet → Bronze (append-only, so
both versions coexist) → Silver unwraps, dedups by LSN, MERGEs → SCD2 compares
the hash of tracked columns, closes the old version at `new_start - 1ms`, opens
a new one with a new surrogate key. `fact_sales` for a January order still
points at the Delhi version, because the join is as-of the order date.

**Why is SCD2 a single MERGE and not two statements?**
Two statements leave a window in which a key has zero current rows. Any Gold
build landing in that window loses those rows entirely. The union trick — NULL
merge key for inserts, real key for closes — makes it one transaction.

**Why hash only the tracked columns?**
Hashing every column opens a new version whenever anything changes, including
`_ingest_ts`, which changes on every load. The dimension would grow by its full
size nightly.

**Why not join facts on `is_current`?**
Because it attributes every historical order to the customer's *present* region.
History silently restates itself whenever somebody moves, and the report still
runs — so nobody notices. That is the entire reason SCD2 exists.

**How do you handle a fact arriving before its dimension?**
An inferred member: insert a placeholder with the natural key and `UNKNOWN`
attributes, and update it in place when the real record arrives. The fact keeps
its surrogate key and never needs restating. Routing to a shared `-1 Unknown`
member loses the linkage permanently, so that's reserved for cases where the
natural key itself is missing.

---

## Correctness

**How do you achieve idempotency?**
Three decisions made early, not a retry policy bolted on later:
1. **No identity columns.** Surrogate keys are `xxhash64` of the business key
   plus version start, so a full rebuild reproduces every key exactly.
2. **MERGE, never blind append.**
3. **Deterministic ordering in every window function**, with a content-hash
   tiebreaker so ties never resolve randomly.

**What's wrong with `ROW_NUMBER() OVER (ORDER BY updated_at DESC)`?**
It isn't deterministic when rows share `updated_at` — which happens constantly,
because a bulk UPDATE stamps them all identically. Spark picks a winner from
whatever the shuffle produced. Re-run the job and a different row can win, with
no code change and no error. That's the worst failure mode a pipeline has.

**Why hash surrogate keys instead of identity columns?**
Identity columns are assigned in write order. Rebuild the warehouse and every
key changes, which breaks idempotency, downstream comparison, and
cross-environment reconciliation. **The cost:** 64-bit collisions are possible
(~10⁻⁷ here), so a FATAL uniqueness rule runs on every surrogate key.

**Data quality vs reconciliation — what's the difference?**
Quality asks *"are these rows valid?"* Reconciliation asks *"did we lose any?"*
A pipeline can pass every rule while dropping 3% of orders through an inner join
against an incomplete dimension. Every surviving row is valid; the total is
wrong; nothing fails. Only reconciliation catches that.

**Why three severities?**
Failing a whole load because 12 rows of 400,000 have a null phone number is how
pipelines get bypassed by frustrated humans. Failing nothing is how bad data
reaches the CFO. WARN records, ERROR quarantines the row, FATAL stops the run —
per rule, in config, reviewable.

**Why does `events` have no not-null rule on `customer_id`?**
Because ~35% of sessions are anonymous. That rule would quarantine a third of
the funnel and make conversion rate meaningless. Knowing which nulls are
*expected* is most of data quality.

---

## Performance

**What's the small-file problem?**
Many tiny files mean metadata overhead and per-file open cost dominate the
actual read. It appears as a pipeline that gets steadily slower rather than one
that fails, which is why the ops dashboard tracks duration trend.

**Partitioning or liquid clustering?**
Partitioning where access is genuinely partition-scoped and volume per partition
is large — `fact_inventory_snapshot` by date, since snapshot semantics mean
whole-partition reads and writes. Liquid clustering on high-cardinality filter
columns, because it adapts without the full rewrite that changing a partition
column requires. **Never both on one table** — they fight.

**When does partitioning hurt?**
On small tables. 45K products partitioned by category gives 12 files of a few
hundred KB; metadata overhead exceeds any pruning benefit. Anything under ~1 GB
should be unpartitioned with periodic `OPTIMIZE`.

**How would this scale from 10 GB/day to 10 TB/day?**
Auto Loader moves to file-notification mode (directory listing becomes the
bottleneck). Dedup moves from full-batch windows to partition-scoped incremental.
MERGE needs pruning predicates so it stops rewriting the whole table. Quality
checks sample or run incrementally on new partitions only. Compute moves off
serverless to dedicated clusters with Photon. **What wouldn't change:** the
medallion layering and the config framework — those scale by adding config.

---

## Governance and delivery

**How is Unity Catalog different from Dataplex?**
Dataplex is largely metadata and discovery; enforcement still happens in IAM on
the underlying resource. Unity Catalog *is* the enforcement point — it issues
short-lived down-scoped credentials per query, and a query cannot bypass it.
That's why UC does column masks and row filters natively.

**How does a user reach GCS?**
They don't. Cloud IAM grants access to *Databricks'* service account; Unity
Catalog decides whether this user may use it. The analyst is never a principal
on the bucket. Two permission systems, both of which must be right.

**Terraform or Asset Bundles?**
Both, split by change velocity. Terraform owns slow-moving governance objects
(catalogs, schemas, grants); Asset Bundles own fast-moving project assets (jobs,
code). Mixing them on one resource causes state conflicts. Note Databricks is
migrating bundles to a Direct Deployment Engine through 2026 — pin the CLI.

**Why is prod deployed by a service principal?**
So no human holds MODIFY on prod. That single control prevents the most common
serious incident: someone running an ad-hoc fix straight against production and
destroying history.

**What would you change for production?**
Separate workspaces and metastores per environment instead of catalogs (real
blast-radius and quota isolation). File-notification Auto Loader. Airflow if the
estate has non-Databricks steps. Private networking. Customer-managed keys. A
real service principal for CI/CD. And a genuine answer to right-to-erasure,
which currently conflicts with immutable Bronze.

---

## About the project itself

**What's simulated, and why?**
Multi-user RBAC (Free Edition has one identity and no account console, so groups
can't exist). Environment separation via catalogs rather than workspaces. The
ERP is Postgres standing in for a commercial system — but the **CDC itself is
real**, captured off the WAL by Debezium. Everything simulated is labelled in
the docs.

**What would you do differently?**
Build the local Spark container first. I wrote three phases of transformation
logic before having a runtime to execute it against, so bugs surfaced later than
they should have. The generator caught two real ones the moment it ran.

**What's the bug you're proudest of catching?**
My CDC update events had identical `before` and `after` images — the status was
"SHIPPED" on both sides. Every test passed, because SCD2 change detection
*correctly* ignores a row where nothing changed. The pipeline looked healthy
while testing nothing. There's now an assertion in the generator that a mutation
must change something. A green test suite and a correct one are different things.
