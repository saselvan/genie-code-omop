"""Shared OMOP CDM validation logic — spec parser + five layer-check functions.

Consumed by ``scripts/validate_omop.py`` (the CLI) and
``templates/project_scaffold/src/99_validate_omop_output.py``
(the notebook). Both surfaces invoke the same parser and layer functions
against the same OMOP CDM v5.4 spec markdown, so a customer's result on
the notebook matches the CLI's result on the same table.

Layer functions print findings to stdout (for the CLI's text-log
surface) and return a ``LayerResult`` carrying:
  * ``failure_count`` — 0 = pass, 1 = the layer reported failures
  * ``findings`` — structured ``Finding`` records, one per emit point
    (PASS / FAIL / WARN / SKIP), parallel to the printed lines
  * ``missing_cols`` — Layer 1 only: lowercased column names absent
    from the actual table; consumed by Layers 3, 4, 5 to skip
    UNRESOLVED_COLUMN-prone per-column SQL

Structured ``findings`` exist so the notebook can populate a Spark
DataFrame without parsing the CLI's stdout. The CLI ignores
``findings`` and consumes only ``failure_count``; the CLI's print
output is byte-stable across minor versions (Layer 5's behavior
change in a recent release is the documented exception — see CHANGELOG).

Layer functions take a ``sql_fn: SqlFn`` callable and never construct
or use a ``WorkspaceClient`` directly — the CLI orchestrator builds
the bound ``sql_fn`` (with warehouse_id / catalog / schema pre-applied)
and the notebook orchestrator does the same with its own SDK-aware
wrapper. SDK exception wrapping (clean ``SystemExit`` for invalid
warehouse IDs) is a CLI concern and lives in ``validate_omop.py``.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ColSpec:
    name: str
    sql_type: str
    nullable: bool
    pk: bool
    fk: str | None
    domain: str | None


@dataclass(frozen=True)
class Finding:
    """One structured validation result emitted by a layer function.

    Parallel to one printed line: every ``print(...)`` in a layer's
    PASS/FAIL/WARN/SKIP path has a matching ``Finding`` appended in
    the same execution step, so the structured surface and the text
    surface cannot drift mid-run.

    Fields:
      * ``layer`` — 1..5
      * ``status`` — ``"PASS"`` | ``"FAIL"`` | ``"WARN"`` | ``"SKIP"``
      * ``check`` — stable identifier (e.g. ``"schema"``,
        ``"concept_fk:gender_concept_id"``, ``"not_null:person_id"``)
      * ``message`` — human-readable detail; matches the printed
        line minus the ``"PASS:"`` / ``"FAIL:"`` / ... prefix
    """

    layer: int
    status: str
    check: str
    message: str


@dataclass
class LayerResult:
    """Return type for ``run_layer_1`` through ``run_layer_5``.

    ``failure_count`` is the field the CLI's pass/fail accumulator
    consumes; ``findings`` is the structured surface for the notebook
    (and any future non-CLI consumer); ``missing_cols`` is Layer 1's
    hand-off to Layers 3, 4, 5 (empty for layers 2-5); ``total_rows``
    is Layer 5's exposed denominator from its ``SELECT COUNT(*) FROM
    {fq}`` query (None for layers 1-4 since they don't count rows;
    None for Layer 5 when the table-missing short-circuit fires
    before the row count is computed). Surfaced in the CLI's enriched
    Summary block; the notebook validator consumes findings only and
    ignores total_rows.
    """

    failure_count: int
    findings: list[Finding] = field(default_factory=list)
    missing_cols: set[str] = field(default_factory=set)
    total_rows: int | None = None


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
) -> LayerResult:
    """Layer 1: schema (columns + loose types).

    Returns a ``LayerResult`` whose ``missing_cols`` is consumed by
    Layers 3, 4, 5 to skip per-column SQL on columns that don't exist
    in the actual table — see ``_should_skip_check`` for the rationale.
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
    type_mismatch: list[tuple[str, str]] = []
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
        type_mismatch.append((c.name, f"{c.name}: expected ~{c.sql_type}, got {fdt or dt}"))

    failures = 0
    findings: list[Finding] = []
    table_missing = not actual
    if table_missing:
        msg = (
            f"table `{cat}`.`{sch}`.`{tbl}` does not exist in catalog. "
            f"If this is a validation-only table (visit_detail, "
            f"device_exposure, note, note_nlp, specimen, dose_era), see "
            f"docs/omop-runbook.md Section 7.5 'BYO-ETL: validation-only "
            f"tables' for the BYO-ETL pattern (per AD-001). Otherwise, the "
            f"build pipeline may not have run yet, or '{tbl}' may be "
            f"misspelled. Subsequent layers will skip checks for this table."
        )
        print(f"FAIL: {msg}")
        findings.append(
            Finding(
                layer=1,
                status="FAIL",
                check="schema:table_missing",
                message=msg,
            )
        )
        failures = 1
    if missing or extra or type_mismatch:
        failures = 1
        if missing:
            print(f"FAIL: missing columns: {missing}")
            for col_name in missing:
                findings.append(
                    Finding(
                        layer=1,
                        status="FAIL",
                        check=f"schema:missing:{col_name}",
                        message=f"missing column: {col_name}",
                    )
                )
        if extra:
            print(f"WARN: extra columns not in spec: {extra}")
            for col_name in extra:
                findings.append(
                    Finding(
                        layer=1,
                        status="WARN",
                        check=f"schema:extra:{col_name}",
                        message=f"extra column not in spec: {col_name}",
                    )
                )
        if type_mismatch:
            print("FAIL: type mismatches:")
            for col_name, m in type_mismatch:
                print("  -", m)
                findings.append(
                    Finding(
                        layer=1,
                        status="FAIL",
                        check=f"schema:type:{col_name}",
                        message=m,
                    )
                )
    elif not table_missing:
        print("PASS: column names and coarse types align with spec.")
        findings.append(
            Finding(
                layer=1,
                status="PASS",
                check="schema",
                message="column names and coarse types align with spec",
            )
        )
    return LayerResult(failure_count=failures, findings=findings, missing_cols=set(missing))


def run_layer_2(
    cols: list[ColSpec],
    fq: str,
    missing_cols: set[str],
    sql_fn: SqlFn,
) -> LayerResult:
    """Layer 2: PK uniqueness.

    Short-circuits with a SKIP finding (no SQL fired) when every PK column
    is in ``missing_cols`` — the table-missing case (every spec column
    absent from actual) and the all-PKs-drifted case (rare) both match.
    Without the short-circuit, the GROUP BY query would raise
    TABLE_OR_VIEW_NOT_FOUND or UNRESOLVED_COLUMN respectively, surfacing
    as a Spark traceback rather than a customer-actionable finding. The
    partial-PK-drift case (some PK cols present, some not) still fires
    the query — that combination doesn't fit Layer 2's cross-row shape
    and is a known followup (rare in OMOP, where most tables have
    single-column PKs).

    """
    print("== Layer 2: primary key uniqueness ==")
    findings: list[Finding] = []
    pks = [c.name for c in cols if c.pk]
    if not pks:
        print("WARN: no PK marked in spec; skipping uniqueness check.")
        findings.append(
            Finding(
                layer=2,
                status="WARN",
                check="pk",
                message="no PK marked in spec; skipping uniqueness check",
            )
        )
        return LayerResult(failure_count=0, findings=findings)
    pk_list = ", ".join(pks)
    if all(_should_skip_check(p, missing_cols) for p in pks):
        msg = "PK columns missing from actual table (Layer 1)"
        print(f"SKIP: pk_uniqueness:{pk_list}: {msg}")
        findings.append(
            Finding(
                layer=2,
                status="SKIP",
                check=f"pk_uniqueness:{pk_list}",
                message=msg,
            )
        )
        return LayerResult(failure_count=0, findings=findings)
    dup_sql = f"""
SELECT {pk_list}, COUNT(*) AS c FROM {fq} GROUP BY {pk_list} HAVING COUNT(*) > 1 LIMIT 20
"""
    dups = sql_fn(statement=dup_sql)
    if dups:
        print(f"FAIL: duplicate PK groups (showing up to 20): {dups}")
        findings.append(
            Finding(
                layer=2,
                status="FAIL",
                check=f"pk_uniqueness:{pk_list}",
                message=f"duplicate PK groups (showing up to 20): {dups}",
            )
        )
        return LayerResult(failure_count=1, findings=findings)
    print("PASS: no duplicate primary keys.")
    findings.append(
        Finding(
            layer=2,
            status="PASS",
            check=f"pk_uniqueness:{pk_list}",
            message="no duplicate primary keys",
        )
    )
    return LayerResult(failure_count=0, findings=findings)


def run_layer_3(
    cols: list[ColSpec],
    fq: str,
    concept: str,
    missing_cols: set[str],
    sql_fn: SqlFn,
) -> LayerResult:
    """Layer 3: referential integrity to concept.

    Skips ``*_concept_id`` columns reported missing by Layer 1 — see
    ``_should_skip_check``. Skipped columns emit a SKIP line and do not
    increment failures (Layer 1 already counted them).
    """
    print("== Layer 3: referential integrity to concept ==")
    findings: list[Finding] = []
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
        findings.append(
            Finding(
                layer=3,
                status="SKIP",
                check=f"concept_fk:{s}",
                message="column not in actual table (Layer 1)",
            )
        )
    if bad_rows:
        print("FAIL: concept_id values not found in reference.concept (excluding 0):")
        for col, n in bad_rows:
            print(f"  - {col}: {n} rows")
            findings.append(
                Finding(
                    layer=3,
                    status="FAIL",
                    check=f"concept_fk:{col}",
                    message=f"{col}: {n} rows with concept_id not in reference.concept",
                )
            )
        return LayerResult(failure_count=1, findings=findings)
    if concept_cols and len(skipped) == len(concept_cols):
        msg = "all concept_id checks skipped (every concept column missing per Layer 1)"
        print(f"SKIP: concept_fk: {msg}")
        findings.append(
            Finding(
                layer=3,
                status="SKIP",
                check="concept_fk",
                message=msg,
            )
        )
        return LayerResult(failure_count=0, findings=findings)
    suffix = " (some skipped per Layer 1)" if skipped else ""
    print(f"PASS: all non-null non-zero concept_ids resolve to reference.concept{suffix}.")
    findings.append(
        Finding(
            layer=3,
            status="PASS",
            check="concept_fk",
            message=f"all non-null non-zero concept_ids resolve to reference.concept{suffix}",
        )
    )
    return LayerResult(failure_count=0, findings=findings)


def run_layer_4(
    cols: list[ColSpec],
    fq: str,
    concept: str,
    missing_cols: set[str],
    sql_fn: SqlFn,
) -> LayerResult:
    """Layer 4: domain conformance (where Domain is documented).

    Skips columns reported missing by Layer 1 — see ``_should_skip_check``.
    """
    print("== Layer 4: domain conformance (where Domain is documented) ==")
    findings: list[Finding] = []
    skipped: list[str] = []
    dom_fails: list[tuple[str, str]] = []
    domain_cols = [c for c in cols if c.domain and c.name.endswith("_concept_id")]
    for c in domain_cols:
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
            dom_fails.append((col, f"{col}: {n} rows with domain_id <> {c.domain}"))
    for s in skipped:
        print(f"SKIP: {s}: column not in actual table (Layer 1)")
        findings.append(
            Finding(
                layer=4,
                status="SKIP",
                check=f"domain:{s}",
                message="column not in actual table (Layer 1)",
            )
        )
    if dom_fails:
        print("FAIL: domain mismatches:")
        for col_name, line in dom_fails:
            print("  -", line)
            findings.append(
                Finding(
                    layer=4,
                    status="FAIL",
                    check=f"domain:{col_name}",
                    message=line,
                )
            )
        return LayerResult(failure_count=1, findings=findings)
    if domain_cols and len(skipped) == len(domain_cols):
        msg = "all domain checks skipped (every annotated concept column missing per Layer 1)"
        print(f"SKIP: domain: {msg}")
        findings.append(
            Finding(
                layer=4,
                status="SKIP",
                check="domain",
                message=msg,
            )
        )
        return LayerResult(failure_count=0, findings=findings)
    suffix = " (some skipped per Layer 1)" if skipped else ""
    print(f"PASS: domain checks for annotated concept columns{suffix}.")
    findings.append(
        Finding(
            layer=4,
            status="PASS",
            check="domain",
            message=f"domain checks for annotated concept columns{suffix}",
        )
    )
    return LayerResult(failure_count=0, findings=findings)


def run_layer_5(
    cols: list[ColSpec],
    fq: str,
    missing_cols: set[str],
    sql_fn: SqlFn,
) -> LayerResult:
    """Layer 5: completeness (NOT NULL columns must have zero NULLs).

    Skips columns reported missing by Layer 1 — see ``_should_skip_check``.
    Short-circuits entirely (no SQL fired) if every spec column is in
    ``missing_cols`` (table-missing case): the pre-loop ``SELECT
    COUNT(*) FROM {fq}`` denominator query would TABLE_OR_VIEW_NOT_FOUND
    and surface as a Spark traceback rather than a customer-actionable
    finding.
    """
    print("== Layer 5: completeness (NOT NULL columns must have zero NULLs) ==")
    findings: list[Finding] = []
    if cols and all(_should_skip_check(c.name, missing_cols) for c in cols):
        msg = "table missing from actual catalog (Layer 1)"
        print(f"SKIP: not_null: {msg}")
        findings.append(
            Finding(
                layer=5,
                status="SKIP",
                check="not_null",
                message=msg,
            )
        )
        return LayerResult(failure_count=0, findings=findings)
    skipped: list[str] = []
    null_fails: list[tuple[str, str]] = []
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
            null_fails.append((col, f"{col}: {n} NULL rows ({rate:.2%} of {total})"))
    for s in skipped:
        print(f"SKIP: {s}: column not in actual table (Layer 1)")
        findings.append(
            Finding(
                layer=5,
                status="SKIP",
                check=f"not_null:{s}",
                message="column not in actual table (Layer 1)",
            )
        )
    if null_fails:
        print("FAIL: unexpected NULLs in spec-required columns:")
        for col_name, line in null_fails:
            print("  -", line)
            findings.append(
                Finding(
                    layer=5,
                    status="FAIL",
                    check=f"not_null:{col_name}",
                    message=line,
                )
            )
        return LayerResult(failure_count=1, findings=findings, total_rows=total)
    suffix = " (some skipped per Layer 1)" if skipped else ""
    print(f"PASS: required (non-nullable) columns have no NULLs{suffix}.")
    findings.append(
        Finding(
            layer=5,
            status="PASS",
            check="not_null",
            message=f"required (non-nullable) columns have no NULLs{suffix}",
        )
    )
    return LayerResult(failure_count=0, findings=findings, total_rows=total)
