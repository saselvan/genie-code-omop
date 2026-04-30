#!/usr/bin/env python3
"""Five-layer validation for materialized OMOP CDM tables in Unity Catalog."""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


@dataclass
class ColSpec:
    name: str
    sql_type: str
    nullable: bool
    pk: bool
    fk: str | None
    domain: str | None


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


def _norm_type(t: str) -> str:
    return re.sub(r"\s+", "", t.upper().replace("NOTNULL", ""))


def parse_omop_spec_md(text: str) -> dict[str, list[ColSpec]]:
    """Parse your organization reference markdown: ## table_name followed by a pipe table."""
    sections = re.split(r"(?m)^##\s+(.+)\s*$", text)
    out: dict[str, list[ColSpec]] = {}
    if len(sections) < 3:
        return out

    for i in range(1, len(sections), 2):
        title = sections[i].strip()
        body = sections[i + 1] if i + 1 < len(sections) else ""
        key = title.lower().split()[0] if title else ""
        if not key or key in ("omop", "tables", "reference"):
            continue

        rows: list[ColSpec] = []
        in_table = False
        header: list[str] | None = None
        for line in body.splitlines():
            line = line.strip()
            if not line.startswith("|"):
                if in_table and rows:
                    break
                continue
            parts = [p.strip() for p in line.strip("|").split("|")]
            if not parts or set(parts) <= {"", "-"}:
                continue
            if re.match(r"^[-:]+$", parts[0]):
                continue
            if not in_table:
                header = [h.lower() for h in parts]
                in_table = True
                continue
            assert header is not None
            rec = {header[j]: parts[j] if j < len(parts) else "" for j in range(len(header))}
            col = rec.get("column") or rec.get("col_name")
            if not col:
                continue
            typ = rec.get("type", "STRING")
            null_mark = (rec.get("nullable") or rec.get("nn") or "Y").upper()
            nullable = null_mark in ("Y", "YES", "NULLABLE", "TRUE")
            pk_mark = (rec.get("pk") or "N").upper()
            pk = pk_mark in ("Y", "YES", "TRUE")
            fk = rec.get("fk") or None
            dom = rec.get("domain") or None
            if fk == "":
                fk = None
            if dom == "":
                dom = None
            rows.append(
                ColSpec(
                    name=col,
                    sql_type=typ,
                    nullable=nullable,
                    pk=pk,
                    fk=fk,
                    domain=dom,
                )
            )
        if rows:
            out[key] = rows
    return out


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
        help="Silver schema (default: second segment of --table FQN)",
    )
    parser.add_argument("--ref-schema", default="reference", help="Reference vocab schema")
    parser.add_argument(
        "--omop-table-spec",
        default=None,
        help="Path to omop_cdm_v54_spec.md (default: alongside script ../references/...)",
    )
    parser.add_argument("--warehouse-id", default=None)
    parser.add_argument("--profile", default=None)
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
    wh = _warehouse_id(args.warehouse_id)
    fq = f"`{cat}`.`{sch}`.`{tbl}`"
    concept = f"`{cat}`.`{args.ref_schema}`.concept"

    failures = 0

    # --- Layer 1: schema ---
    print("== Layer 1: schema (columns + loose types) ==")
    info_sql = f"""
SELECT LOWER(column_name) AS c, data_type, full_data_type
FROM `{cat}`.information_schema.columns
WHERE table_catalog = '{cat}' AND table_schema = '{sch}' AND table_name = '{tbl}'
"""
    irows = _sql(w, warehouse_id=wh, statement=info_sql, catalog=cat, schema=sch)
    actual = {str(r[0]).lower(): (str(r[1]), str(r[2]) if len(r) > 2 else "") for r in irows}
    spec_names = {c.name.lower() for c in cols}
    missing = sorted(spec_names - set(actual.keys()))
    extra = sorted(set(actual.keys()) - spec_names)
    type_mismatch: list[str] = []
    for c in cols:
        a = actual.get(c.name.lower())
        if not a:
            continue
        dt, fdt = a
        blob = _norm_type(fdt or dt)
        exp = _norm_type(c.sql_type)
        if exp in ("INT", "INTEGER") and ("INT" in blob or "DECIMAL" in blob):
            continue
        if exp == "BIGINT" and ("BIGINT" in blob or "LONG" in blob):
            continue
        if exp in ("STRING", "VARCHAR", "CHAR") and (
            "STRING" in blob or "VARCHAR" in blob or "CHAR" in blob
        ):
            continue
        if exp in ("DATE",) and "DATE" in blob:
            continue
        if exp in ("TIMESTAMP",) and ("TIMESTAMP" in blob or "TIMESTAMPTZ" in blob):
            continue
        if exp in ("FLOAT", "DOUBLE", "REAL") and (
            "FLOAT" in blob or "DOUBLE" in blob or "REAL" in blob
        ):
            continue
        type_mismatch.append(f"{c.name}: expected ~{c.sql_type}, got {fdt or dt}")

    if missing or extra or type_mismatch:
        failures += 1
        if missing:
            print(f"FAIL: missing columns: {missing}")
        if extra:
            print(f"WARN: extra columns not in spec: {extra}")
        if type_mismatch:
            print("FAIL: type mismatches:")
            for m in type_mismatch:
                print("  -", m)
    else:
        print("PASS: column names and coarse types align with spec.")

    # --- Layer 2: PK uniqueness ---
    print("== Layer 2: primary key uniqueness ==")
    pks = [c.name for c in cols if c.pk]
    if not pks:
        print("WARN: no PK marked in spec; skipping uniqueness check.")
    else:
        pk_list = ", ".join(pks)
        dup_sql = f"""
SELECT {pk_list}, COUNT(*) AS c FROM {fq} GROUP BY {pk_list} HAVING COUNT(*) > 1 LIMIT 20
"""
        dups = _sql(w, warehouse_id=wh, statement=dup_sql, catalog=cat, schema=sch)
        if dups:
            failures += 1
            print(f"FAIL: duplicate PK groups (showing up to 20): {dups}")
        else:
            print("PASS: no duplicate primary keys.")

    # --- Layer 3: referential integrity to concept ---
    print("== Layer 3: referential integrity to concept ==")
    concept_cols = [
        c.name
        for c in cols
        if c.name.endswith("_concept_id")
        or c.name in ("provider_id", "care_site_id", "location_id")
    ]
    # provider/location/care_site are not concept — filter
    concept_cols = [c for c in concept_cols if c.endswith("_concept_id")]
    bad_rows: list[list[Any]] = []
    for col in concept_cols:
        q = f"""
SELECT COUNT(*) FROM {fq} s
LEFT JOIN {concept} c ON c.concept_id = s.`{col}`
WHERE s.`{col}` IS NOT NULL AND s.`{col}` <> 0 AND c.concept_id IS NULL
"""
        cnt = _sql(w, warehouse_id=wh, statement=q, catalog=cat, schema=sch)
        n = int(cnt[0][0]) if cnt and cnt[0] else 0
        if n:
            failures += 1
            bad_rows.append([col, n])
    if bad_rows:
        print("FAIL: concept_id values not found in reference.concept (excluding 0):")
        for col, n in bad_rows:
            print(f"  - {col}: {n} rows")
    else:
        print("PASS: all non-null non-zero concept_ids resolve to reference.concept.")

    # --- Layer 4: domain conformance ---
    print("== Layer 4: domain conformance (where Domain is documented) ==")
    dom_fails: list[str] = []
    for c in cols:
        if not c.domain or not c.name.endswith("_concept_id"):
            continue
        dom = c.domain.replace("'", "''")
        col = c.name.replace("`", "")
        q = f"""
SELECT COUNT(*) FROM {fq} s
JOIN {concept} c ON c.concept_id = s.`{col}`
WHERE s.`{col}` IS NOT NULL AND s.`{col}` <> 0
  AND c.domain_id <> '{dom}'
"""
        cnt = _sql(w, warehouse_id=wh, statement=q, catalog=cat, schema=sch)
        n = int(cnt[0][0]) if cnt and cnt[0] else 0
        if n:
            failures += 1
            dom_fails.append(f"{col}: {n} rows with domain_id <> {c.domain}")
    if dom_fails:
        print("FAIL: domain mismatches:")
        for line in dom_fails:
            print("  -", line)
    else:
        print("PASS: domain checks for annotated concept columns.")

    # --- Layer 5: completeness / null-rate ---
    print("== Layer 5: completeness (NOT NULL columns must have zero NULLs) ==")
    null_fails: list[str] = []
    total_rows = _sql(
        w,
        warehouse_id=wh,
        statement=f"SELECT COUNT(*) FROM {fq}",
        catalog=cat,
        schema=sch,
    )
    total = int(total_rows[0][0]) if total_rows and total_rows[0] else 0
    for c in cols:
        if c.nullable:
            continue
        col = c.name.replace("`", "")
        q = f"SELECT COUNT(*) FROM {fq} WHERE `{col}` IS NULL"
        cnt = _sql(w, warehouse_id=wh, statement=q, catalog=cat, schema=sch)
        n = int(cnt[0][0]) if cnt and cnt[0] else 0
        if n:
            failures += 1
            rate = (n / total) if total else 0.0
            null_fails.append(f"{col}: {n} NULL rows ({rate:.2%} of {total})")
    if null_fails:
        print("FAIL: unexpected NULLs in spec-required columns:")
        for line in null_fails:
            print("  -", line)
    else:
        print("PASS: required (non-nullable) columns have no NULLs.")

    print(f"\nSummary: {'FAILED' if failures else 'OK'} ({failures} layer(s) failed)")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
