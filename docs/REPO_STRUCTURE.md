# Repository Structure

**Phase:** 1 — decision and rationale · **Revision:** R2 (batch only, Docker source estate)

Directories are created as their phase begins, not upfront.

---

## 1. Final structure

```
enterprise-lakehouse-on-databricks/
│
├── README.md                          # Phase 15
├── pyproject.toml                     # deps, ruff/black/pytest config — single source
├── .gitignore
├── .pre-commit-config.yaml            # secret scan, lint, notebook-output strip
│
├── docs/                              # ← all documentation lives here
│   ├── PHASE0-FEASIBILITY.md          ✅
│   ├── BUSINESS_REQUIREMENTS.md       ✅
│   ├── ARCHITECTURE.md                ✅  incl. 7 diagrams
│   ├── DATA_MODEL.md                  ✅
│   ├── NON_FUNCTIONAL_REQUIREMENTS.md ✅
│   ├── SECURITY.md                    ✅
│   ├── COST.md                        ✅
│   ├── REPO_STRUCTURE.md              ✅  (this file)
│   ├── INGESTION.md                   # Phase 4
│   ├── CDC.md                         # Phase 5
│   ├── DATA_QUALITY.md                # Phase 5
│   ├── PHASE2-SETUP.md                ✅  Docker + GCP + Databricks + spikes runbook
│   ├── GOVERNANCE.md                  # Phase 9
│   ├── TESTING.md                     # Phase 10
│   ├── OBSERVABILITY.md               # Phase 10
│   ├── TERRAFORM.md                   # Phase 11
│   ├── CI_CD.md                       # Phase 12
│   ├── PERFORMANCE.md                 # Phase 13
│   ├── TROUBLESHOOTING.md             # accumulated throughout
│   ├── INTERVIEW_QA.md                # accumulated throughout
│   └── images/                        # screenshots, exported diagrams
│
├── config/                            # ← the framework's control plane
│   ├── env/
│   │   ├── dev.yaml                   # catalog, paths, schedules, DQ thresholds
│   │   ├── test.yaml
│   │   └── prod.yaml
│   ├── entities/                      # one file per source entity
│   │   ├── customers.yaml
│   │   ├── products.yaml
│   │   ├── orders.yaml
│   │   └── ...
│   ├── quality/                       # DQ rules, one file per entity
│   │   └── <entity>.yaml
│   └── schemas/                       # explicit expected schemas (JSON)
│       └── <entity>.json
│
├── src/northpeak/                     # ← installable package, all logic
│   ├── __init__.py
│   ├── common/
│   │   ├── config.py                  # config loading + validation
│   │   ├── spark.py                   # session + Databricks adapter
│   │   ├── logging.py                 # structured JSON logging, run_id propagation
│   │   ├── audit.py                   # pipeline_run_audit writer
│   │   └── keys.py                    # surrogate key hashing
│   ├── ingestion/
│   │   ├── autoloader.py              # Auto Loader reader factory
│   │   ├── api_extractor.py           # paginated REST → GCS
│   │   ├── bronze_writer.py           # metadata attachment + append
│   │   └── framework.py               # the config-driven engine
│   ├── transformations/
│   │   ├── standardise.py
│   │   ├── deduplicate.py
│   │   ├── cdc.py                     # MERGE apply, delete propagation
│   │   ├── scd.py                     # SCD1 + SCD2
│   │   └── dimensional.py             # fact/dim builders, as-of joins
│   ├── quality/
│   │   ├── rules.py                   # rule primitives
│   │   ├── engine.py                  # evaluation + severity routing
│   │   └── quarantine.py
│   └── reconciliation/
│       └── checks.py
│
├── notebooks/                         # ← orchestration only, thin
│   ├── bronze/  silver/  gold/  ops/
│
├── docker/                            # ← Phase 2, the local source estate
│   ├── docker-compose.yml             # profiles: core · cdc · dev · test
│   ├── .env.example                   # never commit the real .env
│   ├── postgres/
│   │   ├── Dockerfile                 # wal_level=logical for Debezium
│   │   └── init/                      # DDL + seed
│   ├── commerce-api/                  # FastAPI REST source
│   ├── sftp/                          # PIM drop config
│   ├── debezium/
│   │   └── connectors/orders.json     # connector registration payload
│   ├── cdc-sink/                      # Kafka → Parquet → GCS
│   ├── file-gen/                      # WMS feeds + clickstream files
│   └── spark-dev/
│       └── Dockerfile                 # pyspark + delta-spark + JDK 17
│
├── generator/                         # Phase 3 — synthetic data generator
│   ├── generate.py                    # CLI: --profile small|medium|large
│   ├── entities/
│   ├── defects.py                     # deliberate DQ defect injection
│   ├── debezium_emitter.py            # fallback: envelope without Kafka
│   └── control_totals.py              # ground truth for reconciliation tests
│
├── sql/
│   ├── ddl/  views/  dashboards/  analysis/
│
├── tests/
│   ├── unit/  integration/  data_quality/  fixtures/
│
├── terraform/
│   ├── modules/{catalog,schema,grants}/
│   ├── envs/{dev,test,prod}/
│   └── reference-only/                # account-level code, documented not applied
│
├── bundle/                            # Databricks Asset Bundles
│   ├── databricks.yml
│   └── resources/{jobs,pipelines}.yml
│
├── .github/workflows/
│   ├── ci.yml                         # lint → test → validate
│   └── cd.yml                         # deploy bundle + terraform
│
└── scripts/                           # setup, spikes, ops runbook helpers
    ├── setup_gcp.sh
    ├── spike1_gcs_external_location.py
    └── spike2_workspace_api.py
```

---

## 2. Deviations from the proposed layout, and why

| Proposed | Changed to | Reason |
|---|---|---|
| `architecture/` and `docs/` as siblings, with `architecture/diagrams/` | **Single `docs/` tree; Mermaid inline in the documents** | Two documentation roots means readers search both and contributors guess. Mermaid renders natively in GitHub, so a diagram beside the prose it explains is more useful than a diagram in a separate folder — and it cannot drift out of sync. |
| `src/ingestion/`, `src/transformations/`, … at top level | **`src/northpeak/…` — a proper installable package** | `pip install -e .` makes imports work identically in notebooks, tests and CI. Bare top-level directories force `sys.path` manipulation, which is the classic reason "it works in the notebook but not in CI". |
| `schemas/` at repo root | **`config/schemas/`** | Schemas are configuration consumed by the framework, not a separate concern. Keeping the framework's entire control plane under one root makes NFR-MAINT-01 ("add an entity by adding config") literally true. |
| `sample_data/` | **`generator/`** | Committing sample data means committing files that go stale, bloat the repo and get silently edited. A generator with a fixed seed is reproducible, versioned, and produces any volume on demand. Small fixtures live in `tests/fixtures/`. |
| `ci-cd/` | **`.github/workflows/` + `bundle/`** | GitHub Actions requires the conventional path. Bundle definitions are deployment artefacts, distinct from CI configuration. |
| (not proposed) | **`bundle/` added** | Asset Bundles is the Databricks-recommended deployment mechanism for jobs and pipelines (ADR-07), and it needs its own root. |
| (not proposed) | **`docker/` added** | R2 introduces a containerised source estate and a local Spark runtime. It is infrastructure for *producing and testing* data, distinct from `generator/` which produces the data itself and runs inside those containers. |
| (not proposed) | **`streaming/` removed** | Scope is batch only (R2). No empty scaffold for work that will not happen. |
| (not proposed) | **`terraform/reference-only/`** added | Free Edition cannot apply account-level Terraform. Rather than omit it, the code is written and clearly marked as never applied — it demonstrates the knowledge without pretending it ran. |

---

## 3. Structural principles

1. **Logic in `src/`, orchestration in `notebooks/`.** A notebook should read as a table of
   contents: load config, call a function, write audit. Anything longer than roughly 30 lines of
   real logic belongs in the package.
2. **Configuration is data, not code.** No entity name, path, catalog or threshold appears as a
   literal in `src/`.
3. **Every directory earns its existence.** Created when its phase starts. An empty scaffold
   directory is a promise the repo may not keep.
4. **Tests mirror `src/`.** `src/northpeak/transformations/scd.py` → `tests/unit/transformations/test_scd.py`.
5. **Documentation lives with the code it describes**, in one tree, in Markdown, with Mermaid
   inline.

---

## 4. Branching model

```
main            ← prod. Protected. Merge only via PR from release/*.
  └── develop   ← dev catalog. Integration branch.
        └── feature/<phase>-<slug>    e.g. feature/p05-scd2-customer
  └── release/<version>               ← test catalog. Cut from develop.
  └── hotfix/<slug>                   ← branches from main
```

| Branch | Deploys to | Gate |
|---|---|---|
| `feature/*` | nothing | CI: lint + unit tests |
| `develop` | `northpeak_dev` | CI + integration tests |
| `release/*` | `northpeak_test` | CI + integration + DQ suite |
| `main` | `northpeak_prod` | Manual approval |

A trunk-based model would be defensible for a solo project and arguably better practice. GitFlow is
chosen deliberately because it is what the target employers run, and demonstrating a release branch
promoting through environments is the point of the exercise.

---

## 5. Repository location

**Standalone repository:** `github.com/Sohailtanveer1/enterprise-lakehouse-on-databricks`, checked
out at `enterprise-lakehouse-on-databricks/` inside the `Sohail-Data-Engg-Portfolio` working
directory but tracked as its own repo with its own remote.

This is the right arrangement and it is already in place. A recruiter following a link lands on
this project's README, not on a portfolio monorepo they must navigate. The portfolio repo should
carry a link out to it.

> **Housekeeping:** the project folder sits inside the parent portfolio repo's working tree. Add
> `enterprise-lakehouse-on-databricks/` to the **parent** repo's `.gitignore` so the two repos do
> not both try to track the same files.
