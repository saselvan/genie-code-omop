#!/usr/bin/env python3
"""Structured OMOP CDM v5.4 DAG dependencies.

Single source of truth for the build DAG. The markdown reference
(`references/omop_dag_dependencies.md`) is human documentation; this
module is what helpers import. Stays in lockstep with the scaffolder's
`templates/project_scaffold/resources/jobs.yml` task list — drift is
caught by `tests/test_omop_dag.py::TestDAGJobsYmlLockstep`.

The build DAG covers 14 of the 20 tables in
`references/omop_cdm_v54_spec.md`. The other 6 (`visit_detail`,
`device_exposure`, `note`, `note_nlp`, `specimen`, `dose_era`) are
validation-only per AD-001 — customers bring their own ETL for these
tables, and `validate_omop` checks them against the spec on
customer-built data. Callers asking the DAG about these tables
(via `direct_predecessors`, `topological_sort`, or
`transitive_predecessors`) get `BuildScopeError` carrying a
user-actionable AD-001 message. `BuildScopeError` inherits from
`KeyError`, so existing ``except KeyError`` callers continue to
catch it transparently; new callers can ``except BuildScopeError``
to distinguish "not buildable per AD-001" from other KeyErrors.

Auth boilerplate not applicable (pure-Python module, no SDK calls).

Adding a new OMOP table:
1. Add the table key to the DAG dict below with its direct dependencies.
2. Add a corresponding `task_key` block to the scaffolder's `jobs.yml`
   (same key, matching `depends_on`).
3. The drift test (`TestDAGJobsYmlLockstep`) catches mismatches; the
   `topological_sort` cycle check catches accidental dep loops.

Era table YAML shape is a known followup; pin down when first era table
is built.
"""

from __future__ import annotations


class BuildScopeError(KeyError):
    """Raised when a table is in the OMOP CDM v5.4 spec but not in
    the build DAG.

    Per AD-001, the skill validates 20 tables and auto-builds 14;
    the other 6 (``visit_detail``, ``device_exposure``, ``note``,
    ``note_nlp``, ``specimen``, ``dose_era``) are validation-only
    via the BYO-ETL pattern. Inherits from ``KeyError`` so existing
    ``except KeyError`` callers continue to catch it transparently.
    """


def _raise_buildscope_error(table: str) -> None:
    """Raise a ``BuildScopeError`` with the canonical AD-001 message.

    Single source of truth for the error text raised by every
    DAG-lookup helper in this module. Keeps the customer-facing
    message identical regardless of which entry point the caller hit.
    """
    raise BuildScopeError(
        f"{table!r} is in the OMOP CDM v5.4 spec but not in the build "
        f"DAG. Per AD-001, this table is validation-only — bring your "
        f"own ETL for it. See docs/omop-runbook.md Section 7.5 "
        f"'BYO-ETL: validation-only tables' for the BYO-ETL pattern."
    )


# Direct dependencies per OMOP CDM v5.4 build DAG.
# Round 1 tables (no deps) appear with empty lists.
DAG: dict[str, list[str]] = {
    # Round 1 — Dimension tables (no dependencies)
    "person": [],
    "care_site": [],
    "provider": [],
    "location": [],
    # Round 2 — Visit infrastructure
    "visit_occurrence": ["person"],
    "observation_period": ["person", "visit_occurrence"],
    # Round 3 — Clinical fact tables
    "condition_occurrence": ["person", "visit_occurrence"],
    "procedure_occurrence": ["person", "visit_occurrence"],
    "drug_exposure": ["person", "visit_occurrence"],
    "measurement": ["person", "visit_occurrence"],
    "observation": ["person", "visit_occurrence"],
    "death": ["person"],
    # Round 4 — Era roll-ups
    "condition_era": ["condition_occurrence"],
    "drug_era": ["drug_exposure"],
}


def direct_predecessors(table: str) -> list[str]:
    """Direct dependencies of a single table.

    Returns a fresh list (caller-mutation safe).

    Raises:
        BuildScopeError: if `table` is not in the build DAG (per
            AD-001, the table is validation-only). Inherits from
            `KeyError` for backward compatibility with
            ``except KeyError`` callers.
    """
    if table not in DAG:
        _raise_buildscope_error(table)
    return list(DAG[table])


def topological_sort(tables: set[str]) -> list[str]:
    """Return tables in valid build order.

    Tables not in the requested subset are ignored as dependencies, so
    `topological_sort({"visit_occurrence"})` returns `["visit_occurrence"]`
    even though it depends on `person` (which isn't in the subset). Output
    is deterministic via lexicographic tie-breaking when multiple tables
    are simultaneously ready.

    Raises:
        BuildScopeError: if any table in the input is not in the
            build DAG (per AD-001, the table is validation-only).
            Inherits from `KeyError` for backward compatibility.
        ValueError: if a cycle is detected. Cannot happen given DAG is
            fixed and acyclic, but defensive against future DAG edits.
    """
    if not tables:
        return []

    for t in tables:
        if t not in DAG:
            _raise_buildscope_error(t)

    in_degree: dict[str, int] = {t: 0 for t in tables}
    for t in tables:
        for dep in DAG[t]:
            if dep in tables:
                in_degree[t] += 1

    result: list[str] = []
    while in_degree:
        ready = sorted(t for t, d in in_degree.items() if d == 0)
        if not ready:
            raise ValueError(
                f"Cycle detected in DAG subset: {sorted(in_degree)}"
            )
        next_table = ready[0]
        result.append(next_table)
        del in_degree[next_table]
        for t in list(in_degree):
            if next_table in DAG[t]:
                in_degree[t] -= 1
    return result


def transitive_predecessors(table: str) -> set[str]:
    """All tables that must exist before `table` can be built.

    Returns:
        The full transitive closure of `table`'s dependencies (does NOT
        include `table` itself). Returns `set()` for round-1 tables.

    Raises:
        BuildScopeError: if `table` is not in the build DAG (per
            AD-001, the table is validation-only). Inherits from
            `KeyError` for backward compatibility.
    """
    if table not in DAG:
        _raise_buildscope_error(table)

    result: set[str] = set()
    stack = list(DAG[table])
    while stack:
        dep = stack.pop()
        if dep in result:
            continue
        result.add(dep)
        if dep in DAG:
            stack.extend(DAG[dep])
    return result
