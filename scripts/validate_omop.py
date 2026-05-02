#!/usr/bin/env python3
"""Five-layer validation for materialized OMOP CDM tables in Unity Catalog.

Auth is handled by Databricks runtime when invoked from Genie Code Agent.
--profile only applies for local development against ~/.databrickscfg.
"""

from __future__ import annotations

import argparse
import re
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue, NotFound
from databricks.sdk.service.sql import StatementState

from _warehouse import resolve_warehouse_id


@dataclass
class ColSpec:
    name: str
    sql_type: str
    nullable: bool
    pk: bool
    fk: str | None
    domain: str | None


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


def _norm_type(t: str) -> str:
    return re.sub(r"\s+", "", t.upper().replace("NOTNULL", ""))


def _should_skip_check(column_name: str, missing_cols: set[str]) -> bool:
    """Return True if this column was reported missing from the actual table.

    Layers 3, 4, 5 use this to skip per-column SQL operations on columns that
    don't exist in the actual table. Without the skip, those operations raise
    UNRESOLVED_COLUMN at SQL parse time, producing an uncaught Spark traceback.
    Layer 1 already reports the column as missing; re-querying it adds a
    traceback without adding information.

    The lookup is case-insensitive against ``missing_cols``, which is
    conventionally lowercased by ``run_layer_1``.
    """
    return column_name.lower() in missing_cols


def parse_omop_spec_md(text: str) -> dict[str, list[ColSpec]]:
    """Parse the OMOP CDM v5.4 spec markdown: ## table_name followed by a pipe table."""
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


SqlFn = Callable[..., list[list[Any]]]


def run_layer_1(
    cols: list[ColSpec],
    cat: str,
    sch: str,
    tbl: str,
    sql_fn: SqlFn,
) -> tuple[int, set[str]]:
    """Layer 1: schema (columns + loose types).

    Returns ``(layer_failures, missing_cols_lowercased)``. The
    ``missing_cols`` set is consumed by Layers 3, 4, 5 to skip per-column
    SQL on columns that don't exist in the actual table — see
    ``_should_skip_check`` for the rationale.
    """
    print("== Layer 1: schema (columns + loose types) ==")
    info_sql = f"""
SELECT LOWER(column_name) AS c, data_type, full_data_type
FROM `{cat}`.information_schema.columns
WHERE table_catalog = '{cat}' AND table_schema = '{sch}' AND table_name = '{tbl}'
"""
    irows = sql_fn(statement=info_sql)
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

    failures = 0
    if missing or extra or type_mismatch:
        failures = 1
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
    return failures, set(missing)


def run_layer_2(
    cols: list[ColSpec],
    fq: str,
    sql_fn: SqlFn,
) -> int:
    """Layer 2: PK uniqueness.

    NOTE: Layer 2 has the same spec/actual coupling pattern as Layers 3, 4, 5
    — if a spec PK column is missing from the actual table, the GROUP BY
    will UNRESOLVED_COLUMN. v2.0.4b deferred this fix per BACKLOG: Phase 4
    did not surface a Layer 2 symptom, and the cross-row uniqueness failure
    mode shape doesn't obviously map to the spec/actual mismatch case.
    Revisit during a focused review against v2.0.4c-merged main.
    """
    print("== Layer 2: primary key uniqueness ==")
    pks = [c.name for c in cols if c.pk]
    if not pks:
        print("WARN: no PK marked in spec; skipping uniqueness check.")
        return 0
    pk_list = ", ".join(pks)
    dup_sql = f"""
SELECT {pk_list}, COUNT(*) AS c FROM {fq} GROUP BY {pk_list} HAVING COUNT(*) > 1 LIMIT 20
"""
    dups = sql_fn(statement=dup_sql)
    if dups:
        print(f"FAIL: duplicate PK groups (showing up to 20): {dups}")
        return 1
    print("PASS: no duplicate primary keys.")
    return 0


def run_layer_3(
    cols: list[ColSpec],
    fq: str,
    concept: str,
    missing_cols: set[str],
    sql_fn: SqlFn,
) -> int:
    """Layer 3: referential integrity to concept.

    Skips ``*_concept_id`` columns reported missing by Layer 1 — see
    ``_should_skip_check``. Skipped columns emit a SKIP line and do not
    increment failures (Layer 1 already counted them).
    """
    print("== Layer 3: referential integrity to concept ==")
    concept_cols = [c for c in cols if c.name.endswith("_concept_id")]
    skipped: list[str] = []
    bad_rows: list[tuple[str, int]] = []
    for c in concept_cols:
        if _should_skip_check(c.name, missing_cols):
            skipped.append(c.name)
            continue
        q = f"""
SELECT COUNT(*) FROM {fq} s
LEFT JOIN {concept} c ON c.concept_id = s.`{c.name}`
WHERE s.`{c.name}` IS NOT NULL AND s.`{c.name}` <> 0 AND c.concept_id IS NULL
"""
        cnt = sql_fn(statement=q)
        n = int(cnt[0][0]) if cnt and cnt[0] else 0
        if n:
            bad_rows.append((c.name, n))
    for s in skipped:
        print(f"SKIP: {s}: column not in actual table (Layer 1)")
    if bad_rows:
        print("FAIL: concept_id values not found in reference.concept (excluding 0):")
        for col, n in bad_rows:
            print(f"  - {col}: {n} rows")
        return 1
    suffix = " (some skipped per Layer 1)" if skipped else ""
    print(f"PASS: all non-null non-zero concept_ids resolve to reference.concept{suffix}.")
    return 0


def run_layer_4(
    cols: list[ColSpec],
    fq: str,
    concept: str,
    missing_cols: set[str],
    sql_fn: SqlFn,
) -> int:
    """Layer 4: domain conformance (where Domain is documented).

    Skips columns reported missing by Layer 1 — see ``_should_skip_check``.
    """
    print("== Layer 4: domain conformance (where Domain is documented) ==")
    skipped: list[str] = []
    dom_fails: list[str] = []
    for c in cols:
        if not c.domain or not c.name.endswith("_concept_id"):
            continue
        if _should_skip_check(c.name, missing_cols):
            skipped.append(c.name)
            continue
        dom = c.domain.replace("'", "''")
        col = c.name.replace("`", "")
        q = f"""
SELECT COUNT(*) FROM {fq} s
JOIN {concept} c ON c.concept_id = s.`{col}`
WHERE s.`{col}` IS NOT NULL AND s.`{col}` <> 0
  AND c.domain_id <> '{dom}'
"""
        cnt = sql_fn(statement=q)
        n = int(cnt[0][0]) if cnt and cnt[0] else 0
        if n:
            dom_fails.append(f"{col}: {n} rows with domain_id <> {c.domain}")
    for s in skipped:
        print(f"SKIP: {s}: column not in actual table (Layer 1)")
    if dom_fails:
        print("FAIL: domain mismatches:")
        for line in dom_fails:
            print("  -", line)
        return 1
    suffix = " (some skipped per Layer 1)" if skipped else ""
    print(f"PASS: domain checks for annotated concept columns{suffix}.")
    return 0


def run_layer_5(
    cols: list[ColSpec],
    fq: str,
    missing_cols: set[str],
    sql_fn: SqlFn,
) -> int:
    """Layer 5: completeness (NOT NULL columns must have zero NULLs).

    Skips columns reported missing by Layer 1 — see ``_should_skip_check``.
    """
    print("== Layer 5: completeness (NOT NULL columns must have zero NULLs) ==")
    skipped: list[str] = []
    null_fails: list[str] = []
    total_rows = sql_fn(statement=f"SELECT COUNT(*) FROM {fq}")
    total = int(total_rows[0][0]) if total_rows and total_rows[0] else 0
    for c in cols:
        if c.nullable:
            continue
        if _should_skip_check(c.name, missing_cols):
            skipped.append(c.name)
            continue
        col = c.name.replace("`", "")
        q = f"SELECT COUNT(*) FROM {fq} WHERE `{col}` IS NULL"
        cnt = sql_fn(statement=q)
        n = int(cnt[0][0]) if cnt and cnt[0] else 0
        if n:
            rate = (n / total) if total else 0.0
            null_fails.append(f"{col}: {n} NULL rows ({rate:.2%} of {total})")
    for s in skipped:
        print(f"SKIP: {s}: column not in actual table (Layer 1)")
    if null_fails:
        print("FAIL: unexpected NULLs in spec-required columns:")
        for line in null_fails:
            print("  -", line)
        return 1
    suffix = " (some skipped per Layer 1)" if skipped else ""
    print(f"PASS: required (non-nullable) columns have no NULLs{suffix}.")
    return 0


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

    failures = 0
    l1_failures, missing_cols = run_layer_1(cols, cat, sch, tbl, sql_fn)
    failures += l1_failures
    failures += run_layer_2(cols, fq, sql_fn)
    failures += run_layer_3(cols, fq, concept, missing_cols, sql_fn)
    failures += run_layer_4(cols, fq, concept, missing_cols, sql_fn)
    failures += run_layer_5(cols, fq, missing_cols, sql_fn)

    print(f"\nSummary: {'FAILED' if failures else 'OK'} ({failures} layer(s) failed)")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
