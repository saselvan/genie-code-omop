"""Five-layer OMOP CDM validation against the shared spec.

This notebook delegates all validation logic to
``_omop_validator.py`` — the shared module the CLI (``validate_omop.py``)
also uses — so notebook findings on a given workspace table match CLI
findings on the same table. Both the shared module and the OMOP CDM v5.4
spec markdown are scaffolded next to this notebook in the customer's
``<target>/src/`` directory (see ``_copy_shared_module`` and
``_copy_shared_spec`` in ``scripts/scaffold_omop_project.py``).

Set widgets (``catalog``, ``core_schema``, ``ref_schema``), then run.
Validates every table the OMOP CDM v5.4 spec covers — currently all 20
spec-covered tables, iterated via ``sorted(spec_map)``.
Tables present in the spec but missing from the customer's catalog
produce a clean ``schema:table_missing`` finding from Layer 1 plus
per-layer SKIPs; they do not raise tracebacks.

Runs as a Databricks Python task or as a notebook (re-add a ``# Databricks
notebook source`` header on line 1 for notebook-task semantics).
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Customer-project layout (canonical runtime): _omop_validator.py and
# omop_cdm_v54_spec.md are siblings of this file (scaffolded by Commits 1.6
# and 1.7). Skill-repo layout (development/testing): the validator lives at
# <SKILL_ROOT>/scripts/_omop_validator.py and the spec at
# <SKILL_ROOT>/references/omop_cdm_v54_spec.md. We try the sibling layout
# first and fall back to skill-repo paths so the same notebook source works
# in both contexts without manual configuration.

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

_SKILL_ROOT_FALLBACK = _HERE.parents[2] if len(_HERE.parents) >= 3 else None
if _SKILL_ROOT_FALLBACK is not None:
    _fallback_scripts = _SKILL_ROOT_FALLBACK / "scripts"
    if _fallback_scripts.is_dir() and str(_fallback_scripts) not in sys.path:
        sys.path.insert(0, str(_fallback_scripts))

from _omop_validator import (  # noqa: E402  (sys.path setup precedes import)
    parse_omop_spec_md,
    run_layer_1,
    run_layer_2,
    run_layer_3,
    run_layer_4,
    run_layer_5,
)

_SPEC_CANDIDATES = [_HERE / "omop_cdm_v54_spec.md"]
if _SKILL_ROOT_FALLBACK is not None:
    _SPEC_CANDIDATES.append(
        _SKILL_ROOT_FALLBACK / "references" / "omop_cdm_v54_spec.md"
    )
SPEC_PATH = next((p for p in _SPEC_CANDIDATES if p.exists()), None)
if SPEC_PATH is None:
    raise FileNotFoundError(
        f"omop_cdm_v54_spec.md not found in any of: {_SPEC_CANDIDATES}. "
        "In a scaffolded customer project the scaffolder copies the spec "
        "into <project>/src/ alongside this notebook. In the skill repo "
        "it lives at <SKILL_ROOT>/references/."
    )

try:
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("core_schema", "core_omop")
    dbutils.widgets.text("ref_schema", "reference")
except NameError:
    pass

spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.appName("validate_omop_output").getOrCreate()

catalog = dbutils.widgets.get("catalog")
core_schema = dbutils.widgets.get("core_schema")
ref_schema = dbutils.widgets.get("ref_schema")


def notebook_sql_fn(*, statement: str) -> list[list]:
    """Adapter passed into the shared module's layer functions.

    Layer functions in ``_omop_validator`` invoke their SQL callable as
    ``sql_fn(statement=...)`` and consume a list of list-of-cells. The CLI
    binds an SDK ``execute_statement`` callable; the notebook binds this
    ``spark.sql`` wrapper. Same call shape, same return shape, equivalent
    findings against the same workspace tables.
    """
    return [list(row) for row in spark.sql(statement).collect()]


spec_map = parse_omop_spec_md(SPEC_PATH.read_text(encoding="utf-8"))

results: list[dict] = []
for table in sorted(spec_map):
    cols = spec_map[table]
    fq = f"`{catalog}`.`{core_schema}`.`{table}`"
    concept = f"`{catalog}`.`{ref_schema}`.concept"

    print(f"=== Validating {fq} ===")
    r1 = run_layer_1(cols, catalog, core_schema, table, notebook_sql_fn)
    r2 = run_layer_2(cols, fq, r1.missing_cols, notebook_sql_fn)
    r3 = run_layer_3(cols, fq, concept, r1.missing_cols, notebook_sql_fn)
    r4 = run_layer_4(cols, fq, concept, r1.missing_cols, notebook_sql_fn)
    r5 = run_layer_5(cols, fq, r1.missing_cols, notebook_sql_fn)

    for layer_result in (r1, r2, r3, r4, r5):
        for f in layer_result.findings:
            results.append(
                {
                    "table": table,
                    "layer": f.layer,
                    "status": f.status,
                    "check": f.check,
                    "message": f.message,
                }
            )

summary_df = spark.createDataFrame(results)
summary_df.orderBy("table", "layer", "check").show(200, truncate=False)
print("--- validation summary (status counts) ---")
summary_df.groupBy("status").count().show()
