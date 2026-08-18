# Performance

**Phase 13.** Baselines are captured by running the pipeline; the targets in
`NON_FUNCTIONAL_REQUIREMENTS.md` §6 are revised against measurement, not guessed
at. This document records the *decisions* and how to verify them.

> **Measure before optimising.** Every technique below can hurt as easily as
> help. An optimisation applied without a baseline is a guess with extra steps.

---

## 1. Current physical design

| Table | Layout | Why |
|---|---|---|
| `bronze.*` | Partition `_ingest_date` | Cheap replay of one load window; time-based retention |
| `silver.orders`, `order_items` | Liquid cluster `(order_date, customer_id)` | High-cardinality filters; adapts without a rewrite |
| `silver.events` | Partition `event_date` | High volume, always queried by date |
| `fact_sales` | Liquid cluster `(order_date_sk, product_sk)` | The two columns every dashboard filters on |
| `fact_inventory_snapshot` | Partition `snapshot_date_sk` | Snapshot semantics: whole-partition reads and writes |
| `dim_*` | Unpartitioned | Small. Partitioning would only create small files |

**Never both partitioning and liquid clustering on one table** — they fight for
the same file layout. The entity config rejects it at load.

---

## 2. When each technique helps, and when it hurts

| Technique | Helps when | Hurts when |
|---|---|---|
| Partitioning | Predicate is partition-scoped and each partition is >1 GB | Small tables — 12 files of 300 KB, metadata cost exceeds pruning benefit |
| Liquid clustering | High-cardinality filters, evolving query patterns | Very small tables; clustering maintenance is not free |
| `OPTIMIZE` | Many small files from frequent appends | Immediately after a bulk load already producing large files |
| Broadcast join | One side comfortably under the broadcast threshold | Both sides large — the driver OOMs, and it fails late |
| `cache()` | The same DataFrame is reused 3+ times | Single-pass work; it costs memory and returns nothing |
| Z-order | Multi-column filters on a static access pattern | Superseded by liquid clustering on Delta; needs a rewrite to change |
| Deletion vectors | Frequent small deletes | Read-heavy tables that rarely change |

The DQ engine already applies one of these deliberately: all row-level rules
evaluate in **one pass** with a `cache()` before the aggregate and the split.
The naive implementation filters once per rule — 10 rules on `events` is 10 full
scans, and it is the easiest way to make quality checking the slowest stage.

---

## 3. Known hot spots

**Dedup window functions.** `ROW_NUMBER` partitioned by primary key shuffles the
entire batch. It is the single most expensive operation in Silver and the first
thing to change at scale (partition-scoped incremental dedup, or a Bloom-filter
pre-filter).

**Full-table `MERGE`.** Without pruning predicates, MERGE rewrites far more
files than the change set requires. Liquid clustering on the merge key helps;
partition-scoped predicates help more.

**Gold facts rebuild fully.** Deliberate — facts are a pure function of Silver,
which is what makes NFR-IDEM-03 hold. Incremental via Change Data Feed
(`cdc.read_changes`) is already written and unused; switch when a rebuild stops
fitting the window, not before.

**Reconciliation counts.** Several `.count()` calls, each a full scan.
`dedup_report` already collapses three into one `agg`; reconciliation could do
the same if it becomes material.

---

## 4. How to benchmark

```bash
docker compose --profile dev run --rm spark-dev \
  python -m generator.generate --profile medium --out /work/local_lake/landing
```

Then run Bronze → Silver → Gold and read the timings back out of the audit
table — the instrumentation already exists, so no separate harness is needed:

```sql
SELECT entity, layer, task_name,
       round(avg(duration_s), 1) AS avg_s,
       sum(rows_out)             AS rows,
       round(sum(rows_out) / nullif(sum(duration_s), 0), 0) AS rows_per_s
FROM audit.pipeline_run_audit
WHERE status = 'SUCCESS'
GROUP BY ALL ORDER BY avg_s DESC;
```

Ranking by `avg_s` descending tells you where to spend effort. Optimising
anything below the top two is premature.

**File-size check after `OPTIMIZE`** — target 128 MB–1 GB (NFR-PERF-05):

```sql
DESCRIBE DETAIL gold.fact_sales;   -- numFiles vs sizeInBytes
```

---

## 5. Deliberately not done

- **Photon tuning.** Serverless decides; there is no knob on Free Edition.
- **Cluster sizing.** No clusters to size. Real experience here needs a paid
  workspace, and claiming otherwise would be false.
- **Z-ordering.** Liquid clustering supersedes it on Delta.
- **Incremental Gold.** Written (`cdc.read_changes`), not switched on. A full
  rebuild is simpler and satisfies idempotency; complexity should follow a
  measured need.

---

## 6. What changes at 10 TB/day

Carried from `NON_FUNCTIONAL_REQUIREMENTS.md` §7, because it is the most likely
interview follow-up.

| Component | Today | At 10 TB/day |
|---|---|---|
| Auto Loader | Directory listing | File notification — listing millions of files becomes the bottleneck |
| Dedup | Full-batch window | Partition-scoped incremental, or Bloom pre-filter |
| CDC apply | Single MERGE | Partition-scoped MERGE with pruning predicates |
| Gold facts | Full rebuild | Incremental via Change Data Feed |
| Quality | Full scans | Sampled, or incremental on new partitions only |
| Compute | Serverless small | Dedicated clusters, Photon, autoscaling |
| Small files | Periodic OPTIMIZE | Auto-compaction and optimised writes on by default |

**What would not change:** the medallion layering and the config-driven
framework. Those scale by adding configuration, not by being rewritten — which
is the point of building them that way.
