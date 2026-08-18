"""Shared pytest fixtures.

Two tiers, deliberately separated (NFR-PORT-03):

  no marker      pure Python. Runs anywhere, including CI with no JDK.
  @pytest.mark.spark        needs a JVM; runs in the spark-dev container.
  @pytest.mark.integration  needs Spark AND writes Delta tables.

Tests that require a live Databricks workspace would never run in CI, so they
stop being run at all. Everything here runs locally or is skipped with a
reason, never silently passed.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# Make `northpeak` and `generator` importable without PYTHONPATH or an editable
# install. PYTHONPATH uses ":" on POSIX and ";" on Windows, so a Makefile or CI
# step that hard-codes one breaks on the other. Doing it here means `pytest`
# works from a clean checkout on any platform.
#
# `pip install -e .` is still the preferred path for day-to-day work; this is
# the belt to that braces.
for path in (REPO_ROOT / "src", REPO_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def _java_available() -> bool:
    return shutil.which("java") is not None


requires_spark = pytest.mark.skipif(
    not _java_available(),
    reason="no JDK on this host; run inside the spark-dev container",
)


@pytest.fixture(scope="session")
def spark():
    """Local Spark session with Delta. Session-scoped: JVM startup is ~10s and
    paying it per test makes the suite unusable."""
    pytest.importorskip("pyspark", reason="pyspark not installed")
    if not _java_available():
        pytest.skip("no JDK on this host")

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tempfile.mkdtemp(prefix="np-test-warehouse-")
    builder = (
        SparkSession.builder.appName("northpeak-tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # 8 not 200: the default produces 200 near-empty files per shuffle and
        # dominates runtime on fixtures of a few dozen rows.
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def tmp_lake(tmp_path: Path) -> Path:
    """Isolated lake root per test, so one test's Delta tables cannot leak
    into another's assertions."""
    root = tmp_path / "lake"
    (root / "landing").mkdir(parents=True)
    (root / "warehouse").mkdir(parents=True)
    return root


@pytest.fixture
def config_root() -> Path:
    return REPO_ROOT / "config"


@pytest.fixture(scope="session")
def small_dataset(tmp_path_factory) -> Path:
    """Generate a tiny dataset once for the whole session.

    Fixed seed, so a failing assertion is reproducible rather than
    intermittent — the difference between a bug report and a shrug.
    """
    import sys

    sys.path.insert(0, str(REPO_ROOT))
    from generator.generate import run as generate

    out = tmp_path_factory.mktemp("landing")
    generate("small", out, seed=42, inject=True, days_inventory=2)
    return out
