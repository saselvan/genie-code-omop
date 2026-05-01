#!/usr/bin/env python3
"""Structured OMOP CDM v5.4 DAG dependencies.

Single source of truth for the build DAG. The markdown reference
(`references/omop_dag_dependencies.md`) is human documentation; this
module is what helpers import. Stays in lockstep with the scaffolder's
`templates/project_scaffold/resources/jobs.yml` task list — drift is
caught by `tests/test_omop_dag.py::TestDAGJobsYmlLockstep`.

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
        KeyError: if `table` is not in the DAG.
    """
    return list(DAG[table])


def topological_sort(tables: set[str]) -> list[str]:
    """Return tables in valid build order.

    Tables not in the requested subset are ignored as dependencies, so
    `topological_sort({"visit_occurrence"})` returns `["visit_occurrence"]`
    even though it depends on `person` (which isn't in the subset). Output
    is deterministic via lexicographic tie-breaking when multiple tables
    are simultaneously ready.

    Raises:
        KeyError: if any table in the input is not in the DAG.
        ValueError: if a cycle is detected. Cannot happen given DAG is
            fixed and acyclic, but defensive against future DAG edits.
    """
    if not tables:
        return []

    for t in tables:
        _ = DAG[t]

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
        KeyError: if `table` is not in the DAG.
    """
    if table not in DAG:
        raise KeyError(table)

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
