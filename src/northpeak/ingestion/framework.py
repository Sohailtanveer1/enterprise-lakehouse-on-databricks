"""The config-driven ingestion framework — one engine, N entities.

This is the highest-signal piece of engineering in the project (ADR-03). There
is no per-entity ingestion code anywhere: `config/entities/*.yaml` is the only
thing that differs between ingesting customers and ingesting order_items.

    python -m northpeak.ingestion.framework --env local
    python -m northpeak.ingestion.framework --env dev --entity orders
    python -m northpeak.ingestion.framework --env dev --system erp_postgres

The acceptance test for whether this framework actually works is NFR-MAINT-01:
adding a twelfth entity must touch config only. It is verified literally, by
adding one late in the project.

Where the framework is deliberately NOT used: an entity whose source needs
genuinely bespoke handling can be given its own module. A framework that
absorbs every exception becomes unreadable, and "when did you not use the
framework?" is a fair interview question with a real answer.
"""
from __future__ import annotations

import argparse
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..common import audit
from ..common.config import EntityConfig, EnvConfig, list_entities, load_entity, load_env
from ..common.logging import get_logger, log_context
from ..common.spark import ensure_namespaces, get_spark
from . import autoloader
from .bronze import add_ingest_metadata, bronze_table_name, validate_bronze

log = get_logger(__name__)


def ingest_entity(env: EnvConfig, entity: EntityConfig, batch_id: str) -> dict:
    """Land one entity into Bronze. The whole per-entity pipeline."""
    target = bronze_table_name(env, entity)

    with log_context(entity=entity.name, layer="bronze", batch_id=batch_id):
        with audit.audited_task(env, "bronze_ingest", entity.name, "bronze", batch_id) as rec:
            raw = autoloader.read_incremental(env, entity)
            enriched = add_ingest_metadata(raw, entity, batch_id)
            rec.rows_out = autoloader.write_bronze(enriched, env, entity, target)

            # Validate only what this batch wrote. Scanning the whole table
            # every run turns a cheap check into the most expensive step in
            # the job by week three.
            spark = get_spark()
            written = spark.table(target).where(f"_batch_id = '{batch_id}'")
            checks = validate_bronze(written, entity)
            rec.rows_in = checks["row_count"]

            if checks["issues"]:
                # An empty ingest is usually legitimate (no new files), so it
                # is logged rather than raised. The freshness DQ rule is what
                # turns "nothing arrived for two days" into a failure.
                log.warning(f"bronze validation notes: {checks['issues']}")

            return {"entity": entity.name, "target": target, "status": "SUCCESS", **checks}


def run(
    env_name: str,
    entities: list[str] | None = None,
    system: str | None = None,
    batch_id: str | None = None,
    parallel: bool | None = None,
) -> list[dict]:
    env = load_env(env_name)
    batch_id = batch_id or uuid.uuid4().hex[:12]

    ensure_namespaces(env)
    audit.ensure_audit_tables(env)

    selected = (
        [load_entity(name) for name in entities] if entities else list_entities()
    )
    if system:
        selected = [e for e in selected if e.source.system == system]
    if not selected:
        raise SystemExit(f"no active entities matched (entities={entities}, system={system})")

    log.info(
        f"batch {batch_id}: ingesting {len(selected)} entities into {env.catalog} "
        f"[{', '.join(e.name for e in selected)}]"
    )

    # Free Edition allows 5 concurrent job tasks, so fan-out is capped at 4
    # (ARCHITECTURE.md §6). Ingestion is the only stage that fans out; Silver
    # CDC on shared dimensions must not race the facts referencing them.
    workers = env.max_parallel_entities if parallel is not False else 1
    results: list[dict] = []

    if workers == 1 or len(selected) == 1:
        for entity in selected:
            results.append(_safe(env, entity, batch_id))
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_safe, env, e, batch_id): e for e in selected}
            for future in as_completed(futures):
                results.append(future.result())

    failed = [r for r in results if r["status"] == "FAILED"]
    ok = len(results) - len(failed)
    log.info(f"batch {batch_id} complete: {ok} succeeded, {len(failed)} failed")
    if failed:
        # Non-zero exit so the orchestrator marks the task failed. Reporting
        # success on partial failure is how silent data loss starts.
        for r in failed:
            log.error(f"  {r['entity']}: {r.get('error')}")
    return results


def _safe(env: EnvConfig, entity: EntityConfig, batch_id: str) -> dict:
    """Isolate one entity's failure from the rest of the batch.

    One broken source feed should not block the other eleven. The batch still
    exits non-zero, so nothing downstream treats a partial load as complete.
    """
    try:
        return ingest_entity(env, entity, batch_id)
    except Exception as exc:  # noqa: BLE001 - deliberate per-entity isolation
        log.error(f"[{entity.name}] ingestion failed: {exc}", exc_info=True)
        return {"entity": entity.name, "status": "FAILED", "error": str(exc)[:500]}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Config-driven Bronze ingestion")
    ap.add_argument("--env", default="local")
    ap.add_argument("--entity", action="append", help="repeatable; default all active")
    ap.add_argument("--system", help="restrict to one source system")
    ap.add_argument("--batch-id")
    ap.add_argument("--sequential", action="store_true", help="disable fan-out")
    a = ap.parse_args(argv)

    results = run(
        a.env, a.entity, a.system, a.batch_id, parallel=not a.sequential
    )
    failed = [r for r in results if r["status"] == "FAILED"]

    print(f"\n{'entity':<16}{'status':<10}{'rows':>10}  target")
    for r in sorted(results, key=lambda x: x["entity"]):
        print(f"{r['entity']:<16}{r['status']:<10}{r.get('row_count', 0):>10,}  "
              f"{r.get('target', '')}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
