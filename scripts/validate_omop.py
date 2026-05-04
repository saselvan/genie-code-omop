#!/usr/bin/env python3
"""Five-layer validation for materialized OMOP CDM tables in Unity Catalog.

Auth is handled by Databricks runtime when invoked from Genie Code Agent.
--profile only applies for local development against ~/.databrickscfg.

This script is the CLI orchestrator. Spec parsing and the 5 layer-check
functions live in ``_omop_validator`` so the notebook validator (Commit 2
of v2.0.4c) consumes the same logic.
"""

from __future__ import annotations

import argparse
from functools import partial
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue, NotFound
from databricks.sdk.service.sql import StatementState

from _omop_validator import (
    SqlFn,
    parse_omop_spec_md,
    run_layer_1,
    run_layer_2,
    run_layer_3,
    run_layer_4,
    run_layer_5,
)
from _warehouse import resolve_warehouse_id


def _sql(
    w: WorkspaceClient,
    *,
    warehouse_id: str,
    statement: str,
    catalog: str | None,
    schema: str | None,
) -> list[list[Any]]:
    kwargs: dict[str, Any] = {
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "50s",
    }
    if catalog:
        kwargs["catalog"] = catalog
    if schema:
        kwargs["schema"] = schema
    try:
        resp = w.statement_execution.execute_statement(**kwargs)
    except (InvalidParameterValue, NotFound) as e:
        # Wrap warehouse-related SDK errors in a clean SystemExit. The SDK
        # raises InvalidParameterValue for malformed/empty IDs (message
        # contains "endpoint id") and NotFound for valid-format-but-missing
        # IDs (message contains "warehouse"). Other Invalid/NotFound errors
        # (e.g. table-not-found during a Layer 1 query) propagate normally
        # so genuine bugs keep their tracebacks. Match _warehouse.py error
        # style: one-line, names the resolution paths, no traceback.
        msg = str(e).strip()
        low = msg.lower()
        if "endpoint" in low or "warehouse" in low:
            raise SystemExit(
                f"Invalid SQL warehouse: {msg} "
                "Verify with `databricks warehouses list`, pass --warehouse-id, "
                "or set DATABRICKS_WAREHOUSE_ID."
            ) from e
        raise
    if resp.status.state != StatementState.SUCCEEDED:
        err = getattr(resp.status, "error", None)
        msg = getattr(err, "message", None) or str(resp.status.state)
        raise RuntimeError(f"Statement failed: {msg}")
    res = getattr(resp, "result", None)
    if res is None:
        return []
    rows = getattr(res, "data_array", None)
    if rows is None and hasattr(res, "data"):
        rows = res.data
    return rows or []


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate an OMOP table with five layers.")
    parser.add_argument("--table", required=True, help="FQN catalog.schema.table")
    parser.add_argument(
        "--catalog",
        default=None,
        help="UC catalog (default: first segment of --table FQN)",
    )
    parser.add_argument(
        "--schema",
        default=None,
        help="OMOP core schema (default: second segment of --table FQN)",
    )
    parser.add_argument("--ref-schema", default="reference", help="Reference vocab schema")
    parser.add_argument(
        "--omop-table-spec",
        default=None,
        help="Path to omop_cdm_v54_spec.md (default: alongside script ../references/...)",
    )
    parser.add_argument("--warehouse-id", default=None, help="SQL warehouse ID (auto-discovers if omitted)")
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile (local dev only; ignored on serverless executeCode)",
    )
    args = parser.parse_args()

    fqn = args.table.strip().strip("`")
    parts = fqn.replace("`", "").split(".")
    if len(parts) != 3:
        raise SystemExit("--table must be catalog.schema.table")
    cat, sch, tbl = parts
    if args.catalog:
        cat = args.catalog
    if args.schema:
        sch = args.schema

    spec_path = Path(
        args.omop_table_spec
        or Path(__file__).resolve().parent.parent / "references" / "omop_cdm_v54_spec.md"
    )
    spec_map = parse_omop_spec_md(spec_path.read_text(encoding="utf-8"))
    cols = spec_map.get(tbl.lower())
    if not cols:
        raise SystemExit(
            f"No spec section found for table '{tbl}' in {spec_path}. Add a ## {tbl} section."
        )

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    wh = resolve_warehouse_id(explicit=args.warehouse_id, client=w)
    fq = f"`{cat}`.`{sch}`.`{tbl}`"
    concept = f"`{cat}`.`{args.ref_schema}`.concept"

    sql_fn: SqlFn = partial(_sql, w, warehouse_id=wh, catalog=cat, schema=sch)

    r1 = run_layer_1(cols, cat, sch, tbl, sql_fn)
    missing_cols = r1.missing_cols
    failures = r1.failure_count
    failures += run_layer_2(cols, fq, missing_cols, sql_fn).failure_count
    failures += run_layer_3(cols, fq, concept, missing_cols, sql_fn).failure_count
    failures += run_layer_4(cols, fq, concept, missing_cols, sql_fn).failure_count
    failures += run_layer_5(cols, fq, missing_cols, sql_fn).failure_count

    print(f"\nSummary: {'FAILED' if failures else 'OK'} ({failures} layer(s) failed)")
    if failures:
        print(
            "\nSee docs/omop-runbook.md Section 8 'Validation Failures "
            "(Post-Pipeline)' for common fixes per layer."
        )
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
