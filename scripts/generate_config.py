#!/usr/bin/env python3
"""Generate a stub OMOP transform YAML from a bronze table DESCRIBE (Databricks SQL).

Pure pass-through scaffolder: for every bronze column, emits
  {target: snake_case(col), expr: f"src.{col}"}

No domain heuristics. The agent rewrites column_mappings based on the OMOP
target columns, the resolution decision tree (MANDATORY rule 3 in SKILL.md),
and the canonical condition_occurrence example. Structural patterns
(resolution strategies, two-lookup rule, hash keys, domain_id) come from
the skill, not from this script.

Auth is handled by Databricks runtime when invoked from Genie Code Agent.
--profile only applies for local development against ~/.databrickscfg.
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def _resolve_warehouse_id(explicit: str | None, w: WorkspaceClient | None = None) -> str:
    """Resolve a SQL warehouse ID via explicit arg, env var, or auto-discovery.

    1. Explicit --warehouse-id wins.
    2. DATABRICKS_WAREHOUSE_ID env var.
    3. First running serverless warehouse via SDK list.
    4. Raise with a clear message if none available.
    """
    if explicit:
        return explicit
    env = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if env:
        return env
    client = w or WorkspaceClient()
    for wh in client.warehouses.list():
        state = getattr(wh, "state", None)
        wtype = getattr(wh, "warehouse_type", None)
        is_running = str(state).upper().endswith("RUNNING")
        is_serverless = "SERVERLESS" in str(wtype).upper() or bool(
            getattr(wh, "enable_serverless_compute", False)
        )
        if is_running and is_serverless and wh.id:
            return wh.id
    raise SystemExit(
        "No running serverless warehouse found. Pass --warehouse-id, "
        "set DATABRICKS_WAREHOUSE_ID, or start a warehouse in the workspace."
    )


def _execute_sql(
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
    resp = w.statement_execution.execute_statement(**kwargs)
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


def _pascal_to_snake(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _passthrough_column_mappings(col_names: list[str]) -> list[dict[str, str]]:
    """Pure pass-through: one mapping per bronze column, snake_case target.

    The agent rewrites these based on the canonical example and resolution
    decision tree. No CASE statements, no date explosion, no concept_id stubs.
    """
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for col in col_names:
        c = col.strip()
        if not c:
            continue
        target = _pascal_to_snake(c)
        if target in seen:
            continue
        seen.add(target)
        out.append({"target": target, "expr": f"src.{c}"})
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DESCRIBE bronze UC table and emit a pure pass-through OMOP YAML stub."
    )
    parser.add_argument("--bronze-table", required=True, help="FQN catalog.schema.table")
    parser.add_argument("--omop-table", required=True, help="OMOP target table name")
    parser.add_argument(
        "--output",
        default=None,
        help="Output YAML path (default ./configs/{omop-table}.yaml). Caller-supplied.",
    )
    parser.add_argument("--catalog", default=None, help="Default UC catalog for SQL session")
    parser.add_argument(
        "--bronze-schema",
        default=None,
        help="Bronze schema name for placeholders (default: second segment of --bronze-table FQN)",
    )
    parser.add_argument("--warehouse-id", default=None, help="SQL warehouse ID (auto-discovers if omitted)")
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile (local dev only; ignored on serverless executeCode)",
    )
    args = parser.parse_args()

    fqn = args.bronze_table.strip()
    parts = fqn.split(".")
    if len(parts) != 3:
        raise SystemExit("--bronze-table must be catalog.schema.table")

    cat, schema, table = parts
    bronze_schema = args.bronze_schema or schema

    out_path = Path(
        args.output or Path("configs") / f"{args.omop_table.lower()}.yaml"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    wh = _resolve_warehouse_id(args.warehouse_id, w=w)

    rows = _execute_sql(
        w,
        warehouse_id=wh,
        statement=f"DESCRIBE TABLE {fqn}",
        catalog=args.catalog or cat,
        schema=schema,
    )

    # Exclude internal Spark / Auto Loader columns that are not OMOP targets.
    _EXCLUDE_COLS = {"_rescued_data", "_metadata", "_commit_version", "_commit_timestamp"}

    col_names: list[str] = []
    for r in rows:
        if not r:
            continue
        name = str(r[0])
        if name.lower() in _EXCLUDE_COLS:
            continue
        col_names.append(name)

    column_mappings = _passthrough_column_mappings(col_names)

    doc: dict[str, Any] = {
        "table_name": args.omop_table.lower(),
        "target_schema": "core_omop",
        "description": f"OMOP CDM v5.4 {args.omop_table} (stub — review all sections)",
        "sources": [
            {
                "alias": "src",
                "table": f"{{catalog}}.{bronze_schema}.{table}",
            }
        ],
        "joins": [],
        "vocabulary_lookups": [],
        "column_mappings": column_mappings,
        "expectations": {"fail": [], "drop": [], "warn": []},
    }

    header = f"""# Generated stub for OMOP table '{args.omop_table}' from bronze `{fqn}`.
# This is a pure pass-through scaffold: one column_mappings entry per bronze
# column, snake_case target. The agent rewrites column_mappings based on the
# OMOP target columns and the canonical example in SKILL.md.
#
# TODO(vocabulary_lookups): Add per the resolution decision tree (MANDATORY rule 3).
# TODO(expectations): Add fail/drop/warn rules (e.g. PK NOT NULL, allowed concept sets).
# TODO(joins): Add join blocks if multiple sources are required.
"""
    body = yaml.dump(
        doc,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        width=120,
    )
    out_path.write_text(header + "\n" + body, encoding="utf-8")

    print(f"Wrote: {out_path.resolve()}")
    print(
        "Next steps: rewrite column_mappings per the canonical example in SKILL.md, "
        "add vocabulary_lookups, validate with scripts/validate_yaml_schema.py."
    )


if __name__ == "__main__":
    main()
