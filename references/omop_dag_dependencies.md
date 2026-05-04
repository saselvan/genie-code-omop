# OMOP DAG Dependency Ordering

This is the dependency chart for the OMOP CDM v5.4 build DAG used in `resources/jobs.yml`. The DAG covers 14 of the 20 tables in [`omop_cdm_v54_spec.md`](./omop_cdm_v54_spec.md). The other 6 (`visit_detail`, `device_exposure`, `note`, `note_nlp`, `specimen`, `dose_era`) are validation-only per architectural decision AD-001 — customers bring their own ETL ("BYO-ETL") for these tables, and the validator checks them against the spec on whatever data the customer builds; see the "Validation-only (BYO-ETL)" section below for per-table notes.

Tables in the same round can run in parallel; tables in later rounds wait on the listed predecessors.

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
  observation            depends on: person, visit_occurrence
  death                  depends on: person

Round 4 (era roll-ups):
  condition_era          depends on: condition_occurrence
  drug_era               depends on: drug_exposure
```

## Why these dependencies

- **Round 1** tables are pure dimensions sourced directly from EHR source. No cross-OMOP-table joins; safe to run in parallel.
- **`visit_occurrence`** carries `person_id` as a foreign key. The visit pipeline doesn't query `person` directly, but the dependency exists so a failed `person` build doesn't produce orphan visits downstream.
- **`observation_period`** is a derived table — typically computed as the earliest/latest visit dates per person, so it needs both upstream tables materialized.
- **Round 3 clinical tables** all carry `person_id`. The five visit-anchored tables (`condition_occurrence`, `procedure_occurrence`, `drug_exposure`, `measurement`, `observation`) also carry `visit_occurrence_id`; both predecessors are listed explicitly (rather than relying on the transitive `visit_occurrence → person` edge) so each task is self-documenting. **`death`** is the exception — it has no `visit_occurrence_id` (death events are not modeled as visits in OMOP v5.4) and lists only `person` as a predecessor.
- **Round 4 era tables** are pure SQL roll-ups of their Round 3 predecessor (`condition_era` from `condition_occurrence`, `drug_era` from `drug_exposure`). No cross-fact joins.

## Explicit-deps convention

In `resources/jobs.yml`, Round 3 tasks list both `person` and `visit_occurrence` in their `depends_on` even though `person` is transitive via `visit_occurrence`. This is intentional:

- Cost: zero (Jobs API accepts redundant dependencies; the DAG is unchanged)
- Benefit: any reader can read a single task block and see its semantic dependencies without tracing the DAG mentally

When you wire a new OMOP table into the DAG, list every table whose data the new pipeline reads — direct or transitive.

## Validation-only (BYO-ETL)

The following 6 OMOP CDM v5.4 tables are in [`omop_cdm_v54_spec.md`](./omop_cdm_v54_spec.md) but NOT in this build DAG. The validator (`scripts/validate_omop.py` and the in-project notebook `templates/project_scaffold/src/99_validate_omop_output.py`) checks them against the spec on whatever data the customer builds; the build DAG does not produce them. Customers bring their own ETL ("BYO-ETL") for these tables — Lakeflow Connect, a custom Spark job, an existing OMOP build the team already runs, or any other path that lands the data in the customer's chosen target schema.

- `visit_detail` — finer-grained visit segmentation; sourced from EHR encounter-detail records
- `device_exposure` — implant / device usage; sourced from EHR device records
- `note` — clinical note text; the build path needs an upstream extract from the EHR notes store
- `note_nlp` — NLP-derived structured terms from `note`; requires a separate NLP pipeline (e.g., cTAKES, Spark NLP, an LLM extractor)
- `specimen` — specimen / sample collection; sourced from lab and pathology system extracts
- `dose_era` — third era table; populated by SQL roll-up of `drug_exposure` analogous to `condition_era` / `drug_era`, but no canonical SQL ships in this skill yet (see DC-008 in `BACKLOG.md` for the era-table YAML-shape followup)

The validator's coverage of these BYO-ETL tables is the same as for the 14 build-scope tables: schema (Layer 1), PK uniqueness (Layer 2), concept FKs (Layer 3), domain conformance (Layer 4), and NOT NULL checks (Layer 5) per [`omop_cdm_v54_spec.md`](./omop_cdm_v54_spec.md).

This is the AD-001 architectural decision: the spec is the conformance contract for all 20 tables; the build DAG is the production path for the 14 tables a typical from-EHR-bronze pipeline builds end-to-end. v2.0.4a expanded spec coverage from 14 to 20 tables to make the validator-side coverage uniform; the build path stayed at 14 because expanding it without a customer-driven need would impose a build pattern that doesn't match the partial-source-coverage case.

## Out of scope (no validator coverage)

The following OMOP CDM v5.4 tables are NOT in [`omop_cdm_v54_spec.md`](./omop_cdm_v54_spec.md) and therefore have no validator coverage in this skill. They sit outside both validation and build scope:

- `cohort`, `cohort_definition` — populated by OHDSI Atlas, not by ETL
- `fact_relationship` — research-tier, defer until needed
- `episode`, `episode_event` — research-tier

If your research scope adds these, the spec must grow first (a v2.0.4a-shaped fidelity expansion) before any build or validator work makes sense.

## Reference

Canonical example: [`resources/jobs.yml`](../../../../resources/jobs.yml) — the active OMOP DAG with Round 1 dimensions, Round 2 visit_occurrence, and 13 placeholder tasks (commented out) showing the full dependency shape for Rounds 3 and 4. (This relative link only resolves inside the OMOP repo clone — when viewing the skill from `/Workspace/.assistant/skills/`, open `resources/jobs.yml` in your repo directly.)
