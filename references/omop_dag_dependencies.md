# OMOP DAG Dependency Ordering

This is the dependency chart for the OMOP CDM v5.4 build DAG used in `resources/jobs.yml`. Tables in the same round can run in parallel; tables in later rounds wait on the listed predecessors.

## Dependency rounds

```
Round 1 (parallel, no dependencies):
  person          care_site       provider        location

Round 2:
  visit_occurrence       depends on: person
  observation_period     depends on: person, visit_occurrence

Round 3 (parallel, depends on Round 1 + 2):
  condition_occurrence   depends on: person, visit_occurrence
  procedure_occurrence   depends on: person, visit_occurrence
  drug_exposure          depends on: person, visit_occurrence
  measurement            depends on: person, visit_occurrence

Round 4 (era roll-ups):
  condition_era          depends on: condition_occurrence
  drug_era               depends on: drug_exposure
```

## Why these dependencies

- **Round 1** tables are pure dimensions sourced directly from EHR source. No cross-OMOP-table joins; safe to run in parallel.
- **`visit_occurrence`** carries `person_id` as a foreign key. The visit pipeline doesn't query `person` directly, but the dependency exists so a failed `person` build doesn't produce orphan visits downstream.
- **`observation_period`** is a derived table — typically computed as the earliest/latest visit dates per person, so it needs both upstream tables materialized.
- **Round 3 fact tables** all carry `person_id` and `visit_occurrence_id` foreign keys. Listing both predecessors explicitly (rather than relying on the transitive `visit_occurrence → person` edge) makes each task self-documenting.
- **Round 4 era tables** are pure SQL roll-ups of their Round 3 predecessor (`condition_era` from `condition_occurrence`, `drug_era` from `drug_exposure`). No cross-fact joins.

## Explicit-deps convention

In `resources/jobs.yml`, Round 3 tasks list both `person` and `visit_occurrence` in their `depends_on` even though `person` is transitive via `visit_occurrence`. This is intentional:

- Cost: zero (Jobs API accepts redundant dependencies; the DAG is unchanged)
- Benefit: any reader can read a single task block and see its semantic dependencies without tracing the DAG mentally

When you wire a new OMOP table into the DAG, list every table whose data the new pipeline reads — direct or transitive.

## Out of scope (Phase 5+ tables)

The following OMOP CDM v5.4 tables are intentionally NOT in this DAG. Add them in a future round if your organization's research scope expands:

- `cohort`, `cohort_definition` — populated by OHDSI Atlas, not by ETL
- `note`, `note_nlp` — require NLP pipeline (separate track)
- `specimen`, `fact_relationship` — research-tier, defer until needed
- `episode`, `episode_event` — research-tier
- `dose_era` — third era table; add alongside `condition_era` / `drug_era` if needed

## Reference

Canonical example: [`resources/jobs.yml`](../../../resources/jobs.yml) — the active OMOP DAG with Round 1 dimensions, Round 2 visit_occurrence, and 10 placeholder tasks (commented out) showing the full dependency shape for Rounds 3 and 4.
