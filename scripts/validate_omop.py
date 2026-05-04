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
    ColSpec,
    LayerResult,
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


def _layer1_line(cols: list[ColSpec], r: LayerResult) -> str:
    if any(f.check == "schema:table_missing" for f in r.findings):
        return "table missing from catalog"
    spec_count = len(cols)
    missing = sum(1 for f in r.findings if f.check.startswith("schema:missing:"))
    type_mm = sum(1 for f in r.findings if f.check.startswith("schema:type:"))
    extras = sum(1 for f in r.findings if f.check.startswith("schema:extra:"))
    matched = spec_count - missing - type_mm
    parts = [f"{spec_count} cols expected", f"{matched} aligned"]
    if missing:
        parts.append(f"{missing} missing")
    if type_mm:
        parts.append(f"{type_mm} type mismatch{'es' if type_mm != 1 else ''}")
    if extras:
        parts.append(f"{extras} extra (warn)")
    return ", ".join(parts)


def _layer2_line(cols: list[ColSpec], r: LayerResult) -> str:
    pks = [c for c in cols if c.pk]
    if not pks:
        return "no PK in spec (skipped)"
    if any(f.status == "SKIP" for f in r.findings):
        return f"{len(pks)} PK col(s) skipped (missing per Layer 1)"
    if r.failure_count:
        return f"{len(pks)} PK col(s) checked, duplicates found"
    return f"{len(pks)} PK col(s) checked, 0 duplicates"


def _layer3_line(cols: list[ColSpec], r: LayerResult) -> str:
    concept_cols = [c for c in cols if c.name.endswith("_concept_id")]
    n = len(concept_cols)
    if n == 0:
        return "no concept_id columns in spec"
    skipped_per_col = sum(
        1
        for f in r.findings
        if f.status == "SKIP" and f.check.startswith("concept_fk:")
    )
    failed_per_col = sum(
        1
        for f in r.findings
        if f.status == "FAIL" and f.check.startswith("concept_fk:")
    )
    checked = n - skipped_per_col
    if r.failure_count:
        return (
            f"{checked} of {n} concept_id col(s) checked, "
            f"{failed_per_col} with unresolved values"
        )
    if skipped_per_col:
        return (
            f"{checked} of {n} concept_id col(s) checked "
            f"({skipped_per_col} skipped per Layer 1), all resolve"
        )
    return f"{n} concept_id col(s) checked, all resolve"


def _layer4_line(cols: list[ColSpec], r: LayerResult) -> str:
    domain_cols = [c for c in cols if c.domain and c.name.endswith("_concept_id")]
    n = len(domain_cols)
    if n == 0:
        return "no domain-annotated columns in spec"
    skipped_per_col = sum(
        1
        for f in r.findings
        if f.status == "SKIP" and f.check.startswith("domain:") and f.check != "domain"
    )
    failed_per_col = sum(
        1
        for f in r.findings
        if f.status == "FAIL" and f.check.startswith("domain:")
    )
    checked = n - skipped_per_col
    if r.failure_count:
        return (
            f"{checked} of {n} domain-annotated col(s) checked, "
            f"{failed_per_col} with domain mismatches"
        )
    if skipped_per_col:
        return (
            f"{checked} of {n} domain-annotated col(s) checked "
            f"({skipped_per_col} skipped per Layer 1), all conform"
        )
    return f"{n} domain-annotated col(s) checked, all conform"


def _layer5_line(cols: list[ColSpec], r: LayerResult) -> str:
    notnull_cols = [c for c in cols if not c.nullable]
    n = len(notnull_cols)
    if n == 0:
        return "no NOT NULL columns in spec"
    if any(f.status == "SKIP" and f.check == "not_null" for f in r.findings):
        return "table missing (skipped)"
    skipped_per_col = sum(
        1
        for f in r.findings
        if f.status == "SKIP"
        and f.check.startswith("not_null:")
        and f.check != "not_null"
    )
    failed_per_col = sum(
        1
        for f in r.findings
        if f.status == "FAIL" and f.check.startswith("not_null:")
    )
    checked = n - skipped_per_col
    if r.failure_count:
        return (
            f"{checked} of {n} NOT NULL col(s) checked, "
            f"{failed_per_col} with NULL violations"
        )
    if skipped_per_col:
        return (
            f"{checked} of {n} NOT NULL col(s) checked "
            f"({skipped_per_col} skipped per Layer 1), 0 NULL violations"
        )
    return f"{n} NOT NULL col(s) checked, 0 NULL violations"


def _format_summary(cols: list[ColSpec], results: list[LayerResult]) -> str:
    """Compose the multi-line Summary block printed at the end of a run.

    Surfaces per-layer counts (columns expected/checked/skipped/failed)
    and Layer 5's row-count denominator. On FAIL, names which layers
    failed by number and appends the runbook trailer pointing customers
    at Section 8 'Validation Failures (Post-Pipeline)'. The block opens
    with ``Summary: OK`` or ``Summary: FAILED`` so existing scripts that
    grep those anchors continue to work.

    Counts derive from data the layer functions already collect:
      * column universes from ``cols`` (spec)
      * skipped/failed per-layer from ``LayerResult.findings`` check
        identifiers (e.g. ``concept_fk:gender_concept_id``)
      * total rows from ``LayerResult.total_rows`` (Layer 5 only)

    Standard-resolved rate (Layer 3) is intentionally not surfaced —
    Layer 3 currently checks existence in reference.concept but not the
    standard_concept flag; reporting a rate would require new SQL and
    is bookmarked as a v2.0.7 candidate.
    """
    r1, r2, r3, r4, r5 = results
    failed_layers = [i + 1 for i, r in enumerate(results) if r.failure_count]
    green = 5 - len(failed_layers)

    if failed_layers:
        failed_names = ", ".join(f"layer-{i}" for i in failed_layers)
        header = f"Summary: FAILED ({green}/5 layers green; failed: {failed_names})"
    else:
        header = "Summary: OK (5/5 layers green)"

    lines = ["", header]
    lines.append(f"  - Layer 1 (schema):      {_layer1_line(cols, r1)}")
    lines.append(f"  - Layer 2 (PK):          {_layer2_line(cols, r2)}")
    lines.append(f"  - Layer 3 (FK concepts): {_layer3_line(cols, r3)}")
    lines.append(f"  - Layer 4 (domain):      {_layer4_line(cols, r4)}")
    lines.append(f"  - Layer 5 (NOT NULL):    {_layer5_line(cols, r5)}")
    if r5.total_rows is not None:
        lines.append(f"Total rows in table: {r5.total_rows}")
    if failed_layers:
        lines.append(
            "\nSee docs/omop-runbook.md Section 8 'Validation Failures "
            "(Post-Pipeline)' for common fixes per layer."
        )
    return "\n".join(lines)


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
            f"Table '{tbl}' isn't in the OMOP CDM v5.4 spec at {spec_path}. "
            f"The validator only runs against the 20 spec-covered tables — "
            f"check the spelling against the spec, or omit if '{tbl}' isn't "
            f"a standard OMOP CDM table."
        )

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    wh = resolve_warehouse_id(explicit=args.warehouse_id, client=w)
    fq = f"`{cat}`.`{sch}`.`{tbl}`"
    concept = f"`{cat}`.`{args.ref_schema}`.concept"

    sql_fn: SqlFn = partial(_sql, w, warehouse_id=wh, catalog=cat, schema=sch)

    r1 = run_layer_1(cols, cat, sch, tbl, sql_fn)
    missing_cols = r1.missing_cols
    r2 = run_layer_2(cols, fq, missing_cols, sql_fn)
    r3 = run_layer_3(cols, fq, concept, missing_cols, sql_fn)
    r4 = run_layer_4(cols, fq, concept, missing_cols, sql_fn)
    r5 = run_layer_5(cols, fq, missing_cols, sql_fn)

    results = [r1, r2, r3, r4, r5]
    failures = sum(r.failure_count for r in results)

    print(_format_summary(cols, results))
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
