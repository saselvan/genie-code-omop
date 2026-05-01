"""Column projection and expectation metadata for OMOP transforms (test-friendly helpers)."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame

from config_loader import ColumnMapping, Expectations

logger = logging.getLogger(__name__)


def build_select_exprs(joined_df: DataFrame, column_mappings: list[ColumnMapping]) -> DataFrame:
    """Project OMOP columns via ``selectExpr`` from YAML ``column_mappings``.

    Args:
        joined_df: Fully joined / enriched source DataFrame.
        column_mappings: Target column specs with SQL expressions.

    Returns:
        DataFrame with only mapped columns.
    """
    if not column_mappings:
        logger.warning("build_select_exprs called with no column_mappings.")
        return joined_df
    exprs = [f"{m.expr} AS `{m.target}`" for m in column_mappings]
    logger.info("Selecting %d mapped column(s): %s", len(exprs), [m.target for m in column_mappings])
    print(f"[column_mapper] build_select_exprs: {len(exprs)} column(s).", flush=True)
    return joined_df.selectExpr(*exprs)


def apply_expectations(df: DataFrame, expectations: Expectations) -> tuple[DataFrame, dict[str, Any]]:
    """Fallback hook for tests: returns the input plus a metadata dict for expectations.

    SDP pipelines should apply ``@dp.expect_all*`` decorators in the pipeline module; this
    function does not mutate rows — it only summarizes rules for logging or unit assertions.

    Args:
        df: Transformed DataFrame (unchanged).
        expectations: Parsed expectation groups.

    Returns:
        The same DataFrame and a dict with ``fail``, ``drop``, and ``warn`` rule lists.
    """
    meta: dict[str, Any] = {
        "fail": [{"name": e.name, "expr": e.expr} for e in expectations.fail],
        "drop": [{"name": e.name, "expr": e.expr} for e in expectations.drop],
        "warn": [{"name": e.name, "expr": e.expr} for e in expectations.warn],
    }
    logger.info(
        "Expectation summary (decorator path in pipeline): fail=%d drop=%d warn=%d",
        len(meta["fail"]),
        len(meta["drop"]),
        len(meta["warn"]),
    )
    print(
        f"[column_mapper] apply_expectations metadata: fail={len(meta['fail'])} "
        f"drop={len(meta['drop'])} warn={len(meta['warn'])} (decorators apply in pipeline)",
        flush=True,
    )
    return df, meta
