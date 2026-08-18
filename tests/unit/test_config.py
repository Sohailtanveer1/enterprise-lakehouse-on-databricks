"""Config layer tests — pure Python, no Spark.

These run in CI with no JDK, which is the point: a config typo caught at merge
is free, the same typo caught at 04:00 in a scheduled run is not.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from northpeak.common.config import (
    EntityConfig,
    EnvConfig,
    LoadType,
    QualityConfig,
    RuleConfig,
    SCDType,
    Severity,
    list_entities,
    load_env,
    load_quality,
    validate_all,
)

BASE_SOURCE = {"system": "test", "format": "json", "path_glob": "t/*.json"}


def entity(**overrides) -> dict:
    return {"name": "t", "source": BASE_SOURCE, "primary_key": ["id"], **overrides}


# --------------------------------------------------------- real repo configs


def test_every_entity_config_loads():
    entities = list_entities()
    assert len(entities) == 12, "expected 12 source entities"
    assert {e.name for e in entities} >= {"orders", "order_items", "customers", "events"}


def test_repo_configs_validate_clean():
    """The whole-repo gate. Runs in CI on every push."""
    assert validate_all() == []


@pytest.mark.parametrize("env_name", ["local", "dev", "test", "prod"])
def test_every_environment_loads(env_name):
    assert load_env(env_name).name == env_name


def test_local_env_degrades_without_unity_catalog():
    local = load_env("local")
    assert local.use_unity_catalog is False
    # No catalog concept locally, so the name must drop to schema.table or
    # every Spark call fails with "catalog not found".
    assert local.table("silver", "orders") == "silver.orders"


def test_managed_env_uses_three_part_names():
    assert load_env("dev").table("gold", "fact_sales") == "northpeak_dev.gold.fact_sales"


def test_thresholds_tighten_toward_prod():
    """dev tolerates messy data; prod does not. If this inverts, a bad batch
    reaches production while dev rejects it — exactly backwards."""
    dev, prod = load_env("dev"), load_env("prod")
    assert dev.dq_fail_threshold < prod.dq_fail_threshold
    assert dev.dq_warn_threshold < prod.dq_warn_threshold


def test_fan_out_stays_within_free_edition_limit():
    """Free Edition allows 5 concurrent job tasks."""
    for env_name in ("local", "dev", "test", "prod"):
        assert load_env(env_name).max_parallel_entities <= 4


# ------------------------------------------------- validators reject bad config
#
# Each of these is a mistake that does NOT crash at runtime. It produces wrong
# data quietly, which is why the validator has to catch it at load.


def test_cdc_without_ordering_is_rejected():
    """Without a deterministic order, dedup picks an arbitrary winner and the
    run stops being idempotent — with no error to notice."""
    with pytest.raises(ValidationError, match="order_by"):
        EntityConfig(**entity(load_type="cdc"))


def test_incremental_without_watermark_is_rejected():
    with pytest.raises(ValidationError, match="watermark_column"):
        EntityConfig(**entity(load_type="incremental"))


def test_scd2_without_tracked_columns_is_rejected():
    """Tracking every column opens a new version whenever _ingest_ts changes,
    which is every load. The dimension would grow by its full size nightly."""
    with pytest.raises(ValidationError, match="scd2_tracked_columns"):
        EntityConfig(**entity(load_type="full", scd_type="scd2"))


def test_empty_primary_key_is_rejected():
    with pytest.raises(ValidationError, match="primary_key"):
        EntityConfig(**entity(load_type="full", primary_key=[]))


def test_partition_and_cluster_together_is_rejected():
    """Liquid clustering on a partitioned table fights the partition layout."""
    with pytest.raises(ValidationError, match="not both"):
        EntityConfig(**entity(load_type="full", partition_by=["a"], cluster_by=["b"]))


def test_inverted_dq_thresholds_are_rejected():
    with pytest.raises(ValidationError, match="below dq_warn_threshold"):
        EnvConfig(
            name="x",
            catalog="c",
            landing_root="/l",
            checkpoint_root="/c",
            schema_root="/s",
            dq_fail_threshold=0.99,
            dq_warn_threshold=0.90,
        )


def test_local_env_without_warehouse_is_rejected():
    with pytest.raises(ValidationError, match="local_warehouse"):
        EnvConfig(
            name="x",
            catalog="c",
            landing_root="/l",
            checkpoint_root="/c",
            schema_root="/s",
            use_unity_catalog=False,
        )


@pytest.mark.parametrize(
    "rule_kwargs,expected",
    [
        ({"type": "domain", "columns": ["a"]}, "allowed_values"),
        ({"type": "regex", "columns": ["a"]}, "pattern"),
        ({"type": "referential", "columns": ["a"]}, "reference_table"),
        ({"type": "expression"}, "expression"),
        ({"type": "freshness", "columns": ["a"]}, "max_age_hours"),
        ({"type": "range", "columns": ["a"]}, "min_value or max_value"),
    ],
)
def test_rule_requires_its_type_specific_fields(rule_kwargs, expected):
    with pytest.raises(ValidationError, match=expected):
        RuleConfig(name="r", **rule_kwargs)


# ---------------------------------------------------------- quality configs


def test_every_entity_has_quality_rules():
    for e in list_entities():
        rules = load_quality(e.name).rules
        assert rules, f"{e.name} has no data quality rules"


def test_every_entity_has_a_fatal_primary_key_rule():
    """A duplicate primary key breaks the MERGE and corrupts the dimension.
    There is no tolerable number of them, so it must be FATAL everywhere."""
    for e in list_entities():
        rules = load_quality(e.name).rules
        pk_rules = [
            r
            for r in rules
            if r.type in ("unique", "not_null") and set(r.columns) == set(e.primary_key)
        ]
        assert pk_rules, f"{e.name}: no rule covers the primary key {e.primary_key}"
        assert any(
            r.severity is Severity.FATAL for r in pk_rules
        ), f"{e.name}: primary key rules exist but none is FATAL"


def test_no_rule_targets_an_excluded_column():
    """A rule on a dropped column never fires, which looks like passing."""
    for e in list_entities():
        for rule in load_quality(e.name).rules:
            overlap = set(rule.columns) & set(e.exclude_columns)
            assert not overlap, f"{e.name}: rule '{rule.name}' targets excluded {overlap}"


def test_events_has_no_not_null_rule_on_customer_id():
    """~35% of sessions are anonymous. A not_null rule here would quarantine a
    third of the funnel and make conversion rate meaningless."""
    rules = load_quality("events").rules
    offenders = [r for r in rules if r.type == "not_null" and "customer_id" in r.columns]
    assert not offenders, "anonymous sessions are legitimate, not a defect"


def test_cdc_entities_order_by_lsn_not_timestamp():
    """Application timestamps suffer clock skew and are shared across bulk
    updates. The LSN is the database's own total order."""
    for e in list_entities():
        if e.load_type is LoadType.CDC:
            assert (
                "source_lsn" in e.order_by[0]
            ), f"{e.name}: CDC must order by source_lsn first, got {e.order_by}"


def test_scd2_entities_do_not_track_metadata_columns():
    for e in list_entities():
        if e.scd_type is SCDType.TYPE_2:
            metadata = [c for c in e.scd2_tracked_columns if c.startswith("_")]
            assert not metadata, f"{e.name}: tracking metadata {metadata} versions every load"


def test_quality_config_absent_returns_empty_not_error():
    """An entity with no rules is valid. Raising here would make adding an
    entity a two-file change and break NFR-MAINT-01."""
    result = load_quality("does_not_exist_anywhere")
    assert isinstance(result, QualityConfig)
    assert result.rules == []
