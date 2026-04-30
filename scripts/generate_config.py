#!/usr/bin/env python3
"""Generate a stub OMOP transform YAML from a bronze table DESCRIBE (Databricks SQL)."""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Any

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def _warehouse_id(explicit: str | None) -> str:
    wid = explicit or os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not wid:
        raise SystemExit(
            "SQL warehouse ID required: pass --warehouse-id or set DATABRICKS_WAREHOUSE_ID"
        )
    return wid


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


def _guess_column_mappings(
    col_names: list[str], omop_table: str
) -> list[dict[str, str]]:
    """Heuristic column_mappings; caller must review."""
    mappings: list[dict[str, str]] = []
    lower_omop = omop_table.lower()

    for col in col_names:
        c = col.strip()
        if not c:
            continue
        cl = c.lower()
        snake = _pascal_to_snake(c)

        # Primary keys / person
        if cl == "patientid" and lower_omop == "person":
            mappings.append({"target": "person_id", "expr": f"src.{c}"})
            continue
        if cl.endswith("id") and cl not in ("patientid",):
            if lower_omop == "visit_occurrence" and cl == "encounterid":
                mappings.append({"target": "visit_occurrence_id", "expr": f"src.{c}"})
                continue
            guess_target = f"{snake}" if snake.endswith("_id") else f"{snake}_id"
            mappings.append({"target": guess_target, "expr": f"src.{c}"})
            continue

        if cl == "birthdate" and lower_omop == "person":
            mappings.extend(
                [
                    {"target": "year_of_birth", "expr": f"YEAR(src.{c})"},
                    {"target": "month_of_birth", "expr": f"MONTH(src.{c})"},
                    {"target": "day_of_birth", "expr": f"DAY(src.{c})"},
                    {"target": "birth_datetime", "expr": f"src.{c}"},
                ]
            )
            continue

        if cl.endswith("datetime") or cl.endswith("timestamp"):
            mappings.append({"target": snake, "expr": f"src.{c}"})
            continue

        if cl.endswith("date"):
            mappings.append({"target": snake, "expr": f"src.{c}"})
            continue

        if "gender" in cl and lower_omop == "person":
            mappings.append(
                {
                    "target": "gender_concept_id",
                    "expr": (
                        f"CASE WHEN src.{c} = 'M' THEN 8507 "
                        f"WHEN src.{c} = 'F' THEN 8532 ELSE 0 END"
                    ),
                }
            )
            continue

        if cl in ("racecode", "ethnicitycode"):
            suffix = "race" if "race" in cl else "ethnicity"
            mappings.append(
                {
                    "target": f"{suffix}_concept_id",
                    "expr": f"/* TODO: vocab lookup or source_to_concept_map for src.{c} */ CAST(NULL AS INT)",
                }
            )
            continue

        # default: pass through as snake_case target
        mappings.append({"target": snake, "expr": f"src.{c}"})

    # de-duplicate targets preserving first
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for m in mappings:
        t = m["target"]
        if t in seen:
            continue
        seen.add(t)
        out.append(m)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DESCRIBE bronze UC table and emit stub OMOP YAML config."
    )
    parser.add_argument("--bronze-table", required=True, help="FQN catalog.schema.table")
    parser.add_argument("--omop-table", required=True, help="OMOP target table name")
    parser.add_argument(
        "--output",
        default=None,
        help="Output YAML path (default ./configs/{omop-table}.yaml)",
    )
    parser.add_argument("--catalog", default=None, help="Default UC catalog for SQL session")
    parser.add_argument(
        "--bronze-schema",
        default=None,
        help="Bronze schema name for placeholders (e.g. bronze_clinical)",
    )
    parser.add_argument("--warehouse-id", default=None, help="SQL warehouse ID")
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile for WorkspaceClient",
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
    wh = _warehouse_id(args.warehouse_id)

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

    column_mappings = _guess_column_mappings(col_names, args.omop_table.lower())

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
# TODO(vocabulary_lookups): Add rows per CONTEXT_ATOM — reference.concept and/or source_to_concept_map.
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
        "Next steps: Review the generated config, fill in TODO sections, "
        "then run scripts/validate_omop.py after materializing the table."
    )


if __name__ == "__main__":
    main()
