"""Shared pytest fixtures (local PySpark)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


def _create_local_spark():
    """Databricks-shipped PySpark often disables classic ``master(\"local\")``; fall back to Connect-local."""
    from pyspark.errors.exceptions.base import PySparkRuntimeError
    from pyspark.sql import SparkSession

    wh = str(_ROOT / ".spark-warehouse")
    try:
        return (
            SparkSession.builder.appName("omop_etl_tests")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.warehouse.dir", wh)
            .getOrCreate()
        )
    except RuntimeError as exc:
        msg = str(exc)
        if "Databricks Connect" in msg or "remote Spark" in msg:
            return (
                SparkSession.builder.appName("omop_etl_tests")
                .config("spark.remote", "local[2]")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.warehouse.dir", wh)
                .getOrCreate()
            )
        raise


@pytest.fixture(scope="session")
def spark():
    from pyspark.errors.exceptions.base import PySparkRuntimeError

    try:
        sess = _create_local_spark()
    except (PermissionError, PySparkRuntimeError) as exc:
        pytest.skip(
            "Local Spark JVM did not start (install a JDK, ensure spark-submit is executable, "
            "or use Apache PySpark classic if Databricks Connect-only build blocks local master). "
            f"Detail: {type(exc).__name__}: {exc}"
        )
    try:
        yield sess
    finally:
        sess.stop()
