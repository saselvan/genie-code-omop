#!/usr/bin/env python3
"""Build source_to_concept_map CSV rows by resolving distinct source codes to concept_ids."""

from __future__ import annotations

import argparse
import csv
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


STCM_FIELDS = [
    "source_code",
    "source_concept_id",
    "source_vocabulary_id",
    "source_code_description",
    "target_concept_id",
    "target_vocabulary_id",
    "valid_start_date",
    "valid_end_date",
    "invalid_reason",
]


def _warehouse_id(explicit: str | None) -> str:
    wid = explicit or os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not wid:
        raise SystemExit(
            "SQL warehouse ID required: pass --warehouse-id or set DATABRICKS_WAREHOUSE_ID"
        )
    return wid


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


def _chunks(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve distinct source codes to concept_ids and write source_to_concept_map CSV."
    )
    parser.add_argument("--source-vocabulary-id", required=True, help="e.g. ICD10CM")
    parser.add_argument("--source-table", required=True, help="FQN catalog.schema.table")
    parser.add_argument("--source-code-column", required=True)
    parser.add_argument("--catalog", required=True, help="Catalog for concept table")
    parser.add_argument("--ref-schema", default="reference")
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV (default seed_data/source_to_concept_map_{vocab}.csv)",
    )
    parser.add_argument("--warehouse-id", default=None)
    parser.add_argument("--profile", default=None)
    args = parser.parse_args()

    src_fqn = args.source_table.strip()
    sp = src_fqn.split(".")
    if len(sp) != 3:
        raise SystemExit("--source-table must be catalog.schema.table")

    out = Path(
        args.output
        or Path("seed_data") / f"source_to_concept_map_{args.source_vocabulary_id}.csv"
    )
    out.parent.mkdir(parents=True, exist_ok=True)

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    wh = _warehouse_id(args.warehouse_id)

    distinct_sql = (
        f"SELECT DISTINCT `{args.source_code_column}` AS c "
        f"FROM {src_fqn} WHERE `{args.source_code_column}` IS NOT NULL"
    )
    drows = _sql(
        w,
        warehouse_id=wh,
        statement=distinct_sql,
        catalog=sp[0],
        schema=sp[1],
    )
    codes = sorted({str(r[0]) for r in drows if r and r[0] is not None})

    concept_fqn = f"`{args.catalog}`.`{args.ref_schema}`.concept"
    resolved: dict[str, tuple[str, str]] = {}

    for batch in _chunks(codes, 200):
        literals = ", ".join("'" + c.replace("'", "''") + "'" for c in batch)
        q = f"""
SELECT concept_code, CAST(concept_id AS STRING), vocabulary_id
FROM {concept_fqn}
WHERE vocabulary_id = '{args.source_vocabulary_id.replace("'", "''")}'
  AND concept_code IN ({literals})
"""
        crows = _sql(
            w,
            warehouse_id=wh,
            statement=q,
            catalog=args.catalog,
            schema=args.ref_schema,
        )
        for row in crows:
            if not row or row[0] is None:
                continue
            code = str(row[0])
            cid = str(row[1])
            vocab = str(row[2]) if len(row) > 2 else args.source_vocabulary_id
            resolved[code] = (cid, vocab)

    unresolved = [c for c in codes if c not in resolved]

    with out.open("w", newline="", encoding="utf-8") as fh:
        wtr = csv.DictWriter(fh, fieldnames=STCM_FIELDS)
        wtr.writeheader()
        for code in codes:
            if code in resolved:
                cid, tv = resolved[code]
                wtr.writerow(
                    {
                        "source_code": code,
                        "source_concept_id": "0",
                        "source_vocabulary_id": args.source_vocabulary_id,
                        "source_code_description": "",
                        "target_concept_id": cid,
                        "target_vocabulary_id": tv,
                        "valid_start_date": "19700101",
                        "valid_end_date": "20991231",
                        "invalid_reason": "",
                    }
                )
            else:
                wtr.writerow(
                    {
                        "source_code": code,
                        "source_concept_id": "0",
                        "source_vocabulary_id": args.source_vocabulary_id,
                        "source_code_description": "UNRESOLVED — manual mapping required",
                        "target_concept_id": "0",
                        "target_vocabulary_id": "",
                        "valid_start_date": "19700101",
                        "valid_end_date": "20991231",
                        "invalid_reason": "",
                    }
                )

    print(f"Wrote: {out.resolve()}")
    print(f"Resolved: {len(resolved)} codes; unresolved: {len(unresolved)} (need manual mapping)")
    if unresolved[:20]:
        print("Sample unresolved:", ", ".join(unresolved[:20]))


if __name__ == "__main__":
    main()
