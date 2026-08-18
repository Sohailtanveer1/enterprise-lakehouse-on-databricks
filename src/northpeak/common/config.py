"""Configuration layer — the control plane of the ingestion framework.

Everything environment-specific lives in YAML, never in code (NFR-PORT-01).
Adding a twelfth source entity must be a config file, not a code change
(NFR-MAINT-01), and this module is what makes that true.

Three config kinds:
    env/<env>.yaml        catalog, landing root, thresholds, schedule
    entities/<name>.yaml  one source entity: where it comes from, how it loads
    quality/<name>.yaml   data quality rules for that entity

Validated with pydantic on load. A typo in a config file should fail loudly at
startup, not silently produce an empty DataFrame three tasks later.
"""

from __future__ import annotations

import os
from enum import Enum
from functools import cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

# Repo root, resolved from this file's location so it works identically in the
# spark-dev container, in CI, and in a Databricks Git folder.
REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_ROOT = REPO_ROOT / "config"


class LoadType(str, Enum):
    """How an entity is loaded from its source.

    FULL        source sends a complete snapshot; target is replaced
    INCREMENTAL source sends rows changed since a watermark; target is appended/merged
    CDC         source sends Debezium change events; target is merged with op semantics
    """

    FULL = "full"
    INCREMENTAL = "incremental"
    CDC = "cdc"


class SCDType(str, Enum):
    NONE = "none"
    TYPE_1 = "scd1"
    TYPE_2 = "scd2"


class Severity(str, Enum):
    """What a failing rule does. See ARCHITECTURE.md §8.

    WARN   record it, let the row through with a flag
    ERROR  quarantine the row, continue the run
    FATAL  stop the task, publish nothing
    """

    WARN = "warn"
    ERROR = "error"
    FATAL = "fatal"


class EnvConfig(BaseModel):
    """Per-environment settings. One file per environment; `--env` selects it."""

    name: str
    catalog: str
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    audit_schema: str = "audit"

    # Landing root is the single switch between the two Spike-1 outcomes:
    #   PASS -> gs://northpeak-landing/landing
    #   FAIL -> /Volumes/<catalog>/landing/files
    # No other code knows or cares which one is in use.
    landing_root: str
    checkpoint_root: str
    schema_root: str

    # Local runs write Delta to the filesystem instead of Unity Catalog.
    use_unity_catalog: bool = True
    local_warehouse: str | None = None

    dq_fail_threshold: float = Field(0.80, ge=0.0, le=1.0)
    dq_warn_threshold: float = Field(0.95, ge=0.0, le=1.0)
    volume_anomaly_sigma: float = Field(3.0, gt=0)

    max_parallel_entities: int = Field(4, ge=1, le=4)

    @model_validator(mode="after")
    def _check_thresholds(self) -> EnvConfig:
        if self.dq_fail_threshold >= self.dq_warn_threshold:
            raise ValueError(
                "dq_fail_threshold must be below dq_warn_threshold "
                f"(got fail={self.dq_fail_threshold}, warn={self.dq_warn_threshold})"
            )
        return self

    @model_validator(mode="after")
    def _local_needs_warehouse(self) -> EnvConfig:
        if not self.use_unity_catalog and not self.local_warehouse:
            raise ValueError("local_warehouse is required when use_unity_catalog is false")
        return self

    def table(self, layer: Literal["bronze", "silver", "gold", "audit"], name: str) -> str:
        """Fully-qualified table name for a layer.

        Local runs have no catalog, so the name degrades to `schema.table`,
        which is what a local Spark session with a filesystem warehouse wants.
        """
        schema = getattr(self, f"{layer}_schema")
        return f"{self.catalog}.{schema}.{name}" if self.use_unity_catalog else f"{schema}.{name}"


class SourceConfig(BaseModel):
    system: str
    format: Literal["json", "csv", "parquet", "avro"]
    path_glob: str
    options: dict[str, str] = Field(default_factory=dict)

    # Debezium sources carry an envelope that Silver unwraps.
    debezium_envelope: bool = False


class EntityConfig(BaseModel):
    """One source entity: the whole contract, in one file."""

    name: str
    active: bool = True
    source: SourceConfig

    load_type: LoadType
    primary_key: list[str]
    watermark_column: str | None = None

    # Ordering key for deduplication. For CDC this is the LSN, which is the
    # database's own total order — see ARCHITECTURE.md §7. For everything else
    # it is the application timestamp, which is a heuristic.
    order_by: list[str] = Field(default_factory=list)

    scd_type: SCDType = SCDType.NONE
    scd2_tracked_columns: list[str] = Field(default_factory=list)

    partition_by: list[str] = Field(default_factory=list)
    cluster_by: list[str] = Field(default_factory=list)

    # Columns dropped before Silver (PII with no analytical use).
    exclude_columns: list[str] = Field(default_factory=list)

    schema_hints: str | None = None
    description: str = ""

    @field_validator("primary_key")
    @classmethod
    def _pk_not_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("primary_key cannot be empty — dedup and MERGE both need it")
        return v

    @model_validator(mode="after")
    def _incremental_needs_watermark(self) -> EntityConfig:
        if self.load_type is LoadType.INCREMENTAL and not self.watermark_column:
            raise ValueError(f"{self.name}: incremental load requires a watermark_column")
        return self

    @model_validator(mode="after")
    def _scd2_needs_tracked_columns(self) -> EntityConfig:
        if self.scd_type is SCDType.TYPE_2 and not self.scd2_tracked_columns:
            raise ValueError(
                f"{self.name}: scd2 requires scd2_tracked_columns. Tracking every column "
                "opens a new version on any change at all, including ones with no "
                "analytical meaning, and the dimension grows without bound."
            )
        return self

    @model_validator(mode="after")
    def _cdc_needs_ordering(self) -> EntityConfig:
        if self.load_type is LoadType.CDC and not self.order_by:
            raise ValueError(
                f"{self.name}: cdc load requires order_by (normally source_lsn). "
                "Without a deterministic order, dedup picks an arbitrary winner "
                "and the run is not idempotent."
            )
        return self

    @model_validator(mode="after")
    def _partition_or_cluster_not_both(self) -> EntityConfig:
        if self.partition_by and self.cluster_by:
            raise ValueError(
                f"{self.name}: choose partition_by or cluster_by, not both. "
                "Liquid clustering on a partitioned table fights the partition layout."
            )
        return self


class RuleConfig(BaseModel):
    """One data quality rule."""

    name: str
    type: Literal[
        "not_null",
        "unique",
        "range",
        "domain",
        "regex",
        "referential",
        "expression",
        "freshness",
        "volume",
        "pii_scan",
    ]
    severity: Severity = Severity.ERROR
    columns: list[str] = Field(default_factory=list)

    # type-specific
    min_value: float | None = None
    max_value: float | None = None
    allowed_values: list[str] = Field(default_factory=list)
    pattern: str | None = None
    reference_table: str | None = None
    reference_column: str | None = None
    expression: str | None = None
    max_age_hours: int | None = None
    description: str = ""

    @model_validator(mode="after")
    def _required_fields_per_type(self) -> RuleConfig:
        need: dict[str, tuple[str, ...]] = {
            "not_null": ("columns",),
            "unique": ("columns",),
            "range": ("columns",),
            "domain": ("columns", "allowed_values"),
            "regex": ("columns", "pattern"),
            "referential": ("columns", "reference_table", "reference_column"),
            "expression": ("expression",),
            "freshness": ("columns", "max_age_hours"),
            "pii_scan": ("columns",),
        }
        for field in need.get(self.type, ()):
            if not getattr(self, field):
                raise ValueError(f"rule '{self.name}' of type '{self.type}' requires '{field}'")
        if self.type == "range" and self.min_value is None and self.max_value is None:
            raise ValueError(f"rule '{self.name}': range needs min_value or max_value")
        return self


class QualityConfig(BaseModel):
    entity: str
    rules: list[RuleConfig] = Field(default_factory=list)


# --------------------------------------------------------------------- loaders


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"config not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"config must be a mapping: {path}")
    return data


@cache
def load_env(env: str | None = None, config_root: str | None = None) -> EnvConfig:
    """Load environment config. Defaults to $NORTHPEAK_ENV, then 'local'."""
    env = env or os.environ.get("NORTHPEAK_ENV", "local")
    root = Path(config_root) if config_root else CONFIG_ROOT
    raw = _read_yaml(root / "env" / f"{env}.yaml")
    raw.setdefault("name", env)
    cfg = EnvConfig(**raw)
    # Allow ${VAR} expansion so a bucket name is not hard-coded per developer.
    cfg.landing_root = os.path.expandvars(cfg.landing_root)
    cfg.checkpoint_root = os.path.expandvars(cfg.checkpoint_root)
    cfg.schema_root = os.path.expandvars(cfg.schema_root)
    return cfg


@cache
def load_entity(name: str, config_root: str | None = None) -> EntityConfig:
    root = Path(config_root) if config_root else CONFIG_ROOT
    raw = _read_yaml(root / "entities" / f"{name}.yaml")
    raw.setdefault("name", name)
    return EntityConfig(**raw)


@cache
def load_quality(name: str, config_root: str | None = None) -> QualityConfig:
    """Load DQ rules. An entity with no rules file is valid — it just has none."""
    root = Path(config_root) if config_root else CONFIG_ROOT
    path = root / "quality" / f"{name}.yaml"
    if not path.exists():
        return QualityConfig(entity=name, rules=[])
    raw = _read_yaml(path)
    raw.setdefault("entity", name)
    return QualityConfig(**raw)


def list_entities(config_root: str | None = None, active_only: bool = True) -> list[EntityConfig]:
    """Every entity the framework knows about, sorted by name for determinism."""
    root = Path(config_root) if config_root else CONFIG_ROOT
    entities = [
        load_entity(p.stem, config_root) for p in sorted((root / "entities").glob("*.yaml"))
    ]
    return [e for e in entities if e.active] if active_only else entities


def validate_all(config_root: str | None = None) -> list[str]:
    """Parse every config file and return the errors found.

    Run in CI. A config typo caught at merge is free; the same typo caught at
    04:00 in a scheduled run is not.
    """
    root = Path(config_root) if config_root else CONFIG_ROOT
    errors: list[str] = []

    for path in sorted((root / "env").glob("*.yaml")):
        try:
            load_env(path.stem, config_root)
        except Exception as exc:
            errors.append(f"env/{path.name}: {exc}")

    for path in sorted((root / "entities").glob("*.yaml")):
        try:
            entity = load_entity(path.stem, config_root)
        except Exception as exc:
            errors.append(f"entities/{path.name}: {exc}")
            continue
        try:
            quality = load_quality(entity.name, config_root)
        except Exception as exc:
            errors.append(f"quality/{entity.name}.yaml: {exc}")
            continue
        # A rule referencing a column the entity excludes will never fire, which
        # looks like passing quality checks. Catch it here.
        for rule in quality.rules:
            for column in rule.columns:
                if column in entity.exclude_columns:
                    errors.append(
                        f"quality/{entity.name}.yaml: rule '{rule.name}' targets "
                        f"'{column}', which entity config excludes"
                    )
    return errors
