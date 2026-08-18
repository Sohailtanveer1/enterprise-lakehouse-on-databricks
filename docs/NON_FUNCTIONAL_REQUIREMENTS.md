# Non-Functional Requirements

**Phase:** 1 — design only

These are the requirements that make the difference between a pipeline that runs and a platform
that can be operated. Each has an ID, a target, and a stated verification method — an NFR without
a test is an aspiration.

---

## 1. Data freshness

| ID | Requirement | Target | Verified by |
|---|---|---|---|
| NFR-FRESH-01 | Batch Gold available each morning | 07:00 UTC, ≥ 95% of days | `pipeline_run_audit` completion time |
| NFR-FRESH-02 | Inventory Gold available before ops standup | 06:00 UTC | Same |
| NFR-FRESH-03 | Streaming Gold end-to-end latency | < 5 min p95 | `event_time` → `rt_*` write time delta |
| NFR-FRESH-04 | Bronze freshness rule | `MAX(updated_at)` within 26 h for daily sources | DQ freshness rule, `FATAL` beyond 48 h |

Freshness is a **data quality rule**, not just a schedule. A job that succeeds while ingesting a
stale file is a silent failure; the freshness rule is what catches it.

---

## 2. Idempotency and reprocessing — the core contract

| ID | Requirement |
|---|---|
| **NFR-IDEM-01** | Re-running any pipeline task with the same input **must** produce identical output. No duplicated rows, no incremented keys, no changed surrogate keys. |
| **NFR-IDEM-02** | The same source file processed twice must contribute its rows exactly once. |
| **NFR-IDEM-03** | Full historical reprocessing from Bronze must reproduce Silver and Gold byte-identically, with no manual cleanup step. |
| **NFR-IDEM-04** | A task that fails partway must leave no partially-written state visible to consumers. |

**How each is achieved:**

| Requirement | Mechanism |
|---|---|
| IDEM-01 | Deterministic hash surrogate keys (no identity columns); `MERGE` keyed on business keys rather than blind `INSERT`; deterministic ordering in all dedup windows, with an explicit tiebreaker so ties never resolve randomly |
| IDEM-02 | Auto Loader's RocksDB file-state store — a file already recorded in the checkpoint is never re-read |
| IDEM-03 | Bronze is append-only and immutable; Silver and Gold are functions of Bronze; `_batch_id` allows a bounded replay window |
| IDEM-04 | Delta ACID transactions — a failed write commits nothing. Multi-table publishes are ordered so dimensions land before the facts referencing them |

> **The interview version:** "idempotency is not a property you add at the end. It is three
> decisions made early — no identity columns, merge instead of append, and deterministic ordering
> in every window function. Get those wrong and no amount of retry logic will save you."

**Deterministic ordering, concretely.** `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY updated_at
DESC)` is *not* deterministic when two records share `updated_at` — a real occurrence in CDC
extracts. Every dedup window in this project orders by `(updated_at DESC, op_ts DESC,
_record_hash DESC)`, where the final hash term guarantees a total order.

---

## 3. Reliability and recovery

| ID | Requirement | Target |
|---|---|---|
| NFR-REL-01 | Batch pipeline success rate | ≥ 98% of scheduled runs, retries included |
| NFR-REL-02 | Task retry policy | 2 automatic retries, exponential backoff |
| NFR-REL-03 | Recovery Point Objective (RPO) | 0 for Bronze — no ingested data is ever lost |
| NFR-REL-04 | Recovery Time Objective (RTO) | Full Silver + Gold rebuild from Bronze within 4 h at `medium` volume |
| NFR-REL-05 | Streaming restart | Resumes from checkpoint with no data loss and no duplicates |
| NFR-REL-06 | Delta retention | 30 days time travel on Silver/Gold; `VACUUM` no more aggressive than 7 days |

**Partial failure policy.** A task failing mid-DAG must not leave downstream consumers reading a
half-built model. Gold publishes are atomic per table; the dashboard reads Gold, never Silver.

---

## 4. Data quality thresholds

| ID | Rule class | Severity | Threshold |
|---|---|---|---|
| NFR-DQ-01 | Primary key uniqueness | FATAL | 0 violations tolerated |
| NFR-DQ-02 | Surrogate key uniqueness | FATAL | 0 violations |
| NFR-DQ-03 | Required fields not null | ERROR → quarantine | ≤ 1% of batch; above that, FATAL |
| NFR-DQ-04 | Referential integrity | ERROR → inferred member | ≤ 2% of batch |
| NFR-DQ-05 | Range and cross-field rules | ERROR → quarantine | ≤ 1% |
| NFR-DQ-06 | Domain / enum validity | WARN | ≤ 5% |
| NFR-DQ-07 | Volume anomaly | FATAL | Batch row count outside ±50% of 7-day trailing mean |
| NFR-DQ-08 | Schema drift — new column | WARN | Auto-evolve, log, continue |
| NFR-DQ-09 | Schema drift — removed / retyped column | FATAL | Stop; requires human decision |
| NFR-DQ-10 | Overall batch pass rate | — | < 95% valid → alert; < 80% → FATAL |

**Why "new column = WARN" but "removed column = FATAL":** an added column cannot break existing
logic, so evolving and continuing is safe. A removed or retyped column *will* break downstream
logic, possibly silently by producing nulls. That is a human decision, not an automatic one.

---

## 5. Reconciliation

| ID | Check | Frequency | Action on failure |
|---|---|---|---|
| NFR-REC-01 | Source row count = Bronze row count per batch | Every run | FATAL |
| NFR-REC-02 | Bronze count − quarantined = Silver count | Every run | FATAL |
| NFR-REC-03 | Silver net revenue = Gold `fact_sales` net revenue, to the cent | Every run | FATAL |
| NFR-REC-04 | Silver distinct orders = Gold `fact_order_fulfillment` rows | Every run | FATAL |
| NFR-REC-05 | Streaming daily revenue vs batch daily revenue | Daily | Alert if > 0.5% variance |
| NFR-REC-06 | Source `MAX(updated_at)` ≤ Bronze `MAX(updated_at)` | Every run | ERROR |

Monetary reconciliation uses `DECIMAL`, never `DOUBLE`. Floating-point accumulation across
millions of rows produces cent-level drift that makes exact reconciliation impossible — and
"reconciles to within a few cents" is not reconciliation.

---

## 6. Performance targets

Measured at the `medium` profile (~6 M rows) on Free Edition serverless. Baselines are captured in
Phase 13, and these targets are revised against measurement rather than guessed at now.

| ID | Operation | Initial target |
|---|---|---|
| NFR-PERF-01 | Full daily batch, ingestion → Gold | < 45 min |
| NFR-PERF-02 | Single entity Bronze ingestion | < 3 min |
| NFR-PERF-03 | Gold dashboard query p95 | < 10 s on 2X-Small warehouse |
| NFR-PERF-04 | Streaming micro-batch | < 60 s per trigger |
| NFR-PERF-05 | Average Delta file size after `OPTIMIZE` | 128 MB – 1 GB |
| NFR-PERF-06 | Full Silver + Gold rebuild | < 4 h |

**Explicitly not a target:** raw throughput records/second. On a quota-limited serverless tier that
number measures the tier, not the code.

---

## 7. Scalability — what changes at 10 TB/day

The built artefact targets ~6 M rows. This section exists because "how would this scale?" is
guaranteed to be asked, and the honest answer is that several decisions here would change.

| Component | Today | At 10 TB/day | Why it changes |
|---|---|---|---|
| Auto Loader | Directory listing | **File notification mode** (Pub/Sub) | Listing a directory with millions of files becomes the bottleneck |
| Compute | Serverless small | Dedicated job clusters, Photon, autoscaling | Need control over parallelism and instance types |
| Bronze partitioning | `_ingest_date` | `_ingest_date` + hour, possibly source-hash sub-partitions | Daily partitions become too large to rewrite |
| `fact_sales` layout | Liquid clustering | Liquid clustering + Z-order on the hottest predicate; possible date partition | Cluster maintenance cost grows |
| Dedup | Window function over full batch | Streaming state store with watermark, or Bloom-filter pre-filter | Full-batch windows shuffle everything |
| CDC apply | Single `MERGE` | Partition-scoped `MERGE` with pruning predicates, or `MERGE` on liquid-clustered keys | Whole-table `MERGE` rewrites too many files |
| Orchestration | Lakeflow Jobs, 5 tasks | Airflow or Jobs with wide fan-out, per-entity SLAs | Coordination across systems |
| Small files | Periodic `OPTIMIZE` | Auto-compaction + optimised writes on by default | Manual optimisation cannot keep up |
| Quality checks | Full-table scans | Sampled + incremental checks on new partitions only | Full scans dominate runtime |

The one decision that would **not** change: the medallion layering and the config-driven framework.
Those scale by adding config, not by rewriting.

---

## 8. Observability

| ID | Requirement |
|---|---|
| NFR-OBS-01 | Every task writes to `audit.pipeline_run_audit`: `run_id`, `job_id`, `task_name`, `entity`, `layer`, `status`, `rows_in`, `rows_out`, `rows_rejected`, `start_ts`, `end_ts`, `duration_s`, `error_message` |
| NFR-OBS-02 | Every DQ rule evaluation writes to `audit.data_quality_results` |
| NFR-OBS-03 | Every reconciliation writes to `audit.reconciliation_results` |
| NFR-OBS-04 | `run_id` is generated once per job run and propagated to every task, table and log line |
| NFR-OBS-05 | Structured JSON logging with consistent keys, never bare `print()` |
| NFR-OBS-06 | An operations dashboard shows run status, duration trend, row-count trend, DQ pass rate and freshness |
| NFR-OBS-07 | Job failure and SLA breach trigger email notification |

**`run_id` propagation is the highest-value observability decision in this project.** With it,
"which run produced this bad row?" is one query. Without it, the answer is archaeology.

---

## 9. Maintainability

| ID | Requirement |
|---|---|
| NFR-MAINT-01 | Adding a new source entity requires a config file and DQ rules — **no framework code change** |
| NFR-MAINT-02 | Notebooks orchestrate only; all logic lives in `src/` and is importable and unit-testable |
| NFR-MAINT-03 | Unit test coverage ≥ 70% on `src/`, with transformation logic prioritised over glue |
| NFR-MAINT-04 | Lint and format enforced in CI (`ruff`, `black`); a failing check blocks merge |
| NFR-MAINT-05 | No credential, token, key or bucket-specific secret in the repository, enforced by a secret-scanning CI step |
| NFR-MAINT-06 | Every environment-specific value comes from config, never a literal in code |
| NFR-MAINT-07 | Every module has a docstring stating its purpose and contract |

NFR-MAINT-01 is the acceptance test for whether the config-driven framework actually works. It
will be verified literally: adding a twelfth entity late in the project, touching only config.

---

## 10. Portability

| ID | Requirement |
|---|---|
| NFR-PORT-01 | No hard-coded catalog, schema, bucket or path anywhere in `src/` |
| NFR-PORT-02 | The same code runs against `northpeak_dev`, `_test` and `_prod` via an `--env` parameter |
| NFR-PORT-03 | Unit tests run on local PySpark with no Databricks connection |
| NFR-PORT-04 | Databricks-specific APIs isolated behind a thin adapter so tests can run without them |

NFR-PORT-03 matters more than it looks: tests that need a live workspace do not run in CI, so they
stop being run at all.

---

## 11. Security (summary — full detail in `SECURITY.md`)

| ID | Requirement |
|---|---|
| NFR-SEC-01 | PII columns tagged in Unity Catalog and masked for non-privileged roles |
| NFR-SEC-02 | Least privilege: analysts read Gold only, never Bronze |
| NFR-SEC-03 | Secrets in GCP Secret Manager or Databricks secret scopes, never in code, config or notebook output |
| NFR-SEC-04 | Service principal for CI/CD, never a personal token |
| NFR-SEC-05 | All access auditable through Unity Catalog |

---

## 12. Verification matrix

| NFR group | Verified by | Phase |
|---|---|---|
| Freshness | DQ freshness rules + audit timestamps | 5, 10 |
| Idempotency | Integration test: run twice, assert identical output hash | 10 |
| Reliability | Deliberate mid-run failure, then restart | 10 |
| Data quality | Injected defects must appear in `data_quality_results` | 5, 10 |
| Reconciliation | Injected row-loss defect must be caught | 10 |
| Performance | Benchmark suite, before/after optimisation | 13 |
| Observability | Ops dashboard populated from real runs | 10 |
| Maintainability | Add a 12th entity via config only | 13 |
| Portability | CI runs unit tests with no workspace connection | 12 |
| Security | Query as a restricted principal, assert masking | 9 |
