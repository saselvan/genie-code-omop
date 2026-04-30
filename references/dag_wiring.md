# DAG wiring — adding an OMOP table to the orchestrated build job

Once the pipeline runs green and `validate_omop.py` passes 5/5 layers, add the table as a task in `resources/jobs.yml` so it runs as part of the orchestrated `omop_full_build` job.

## MANDATORY rules — read first

These rules are why this lives in the skill and not just in generic Databricks Jobs documentation. The `databricks-jobs` skill knows DAB syntax; this file knows OMOP-specific constraints.

1. **`full_refresh: true` on every OMOP task.** OMOP rebuilds are batch snapshots — incremental refresh would silently double-count rows when the source is re-landed. The hand-written DAG sets this on every active and placeholder task. Non-negotiable.
2. **Explicit `depends_on` for every upstream table the pipeline reads.** List both `person` AND `visit_occurrence` for Round 3 facts even though `person` is transitive via `visit_occurrence`. Zero cost, big readability win, future-proofs against accidentally dropping the transitive edge.
3. **`pipeline_id: ${resources.pipelines.omop_<table>.id}`.** This is a DAB resource reference — the pipeline must already exist in `resources/pipeline_generic.yml`. If it doesn't, add it before editing `jobs.yml`.
4. **Validate before deploying.** Run `databricks bundle validate -t <target>` after editing — this catches typos in `pipeline_id`, malformed `depends_on` lists, and YAML syntax errors. Do NOT run `databricks bundle deploy` until validation is clean.

## Workflow

**Step 1 — Read the OMOP dependency chart:** [`omop_dag_dependencies.md`](omop_dag_dependencies.md). Find your table's round, list every predecessor it depends on (including transitive ones — explicit deps are self-documenting).

**Step 2 — Read the canonical DAG:** [`resources/jobs.yml`](../../../../resources/jobs.yml). Each placeholder task already shows the exact YAML shape you need. (Relative link only resolves inside the OMOP repo clone — when viewing the skill from `/Workspace/.assistant/skills/`, open `resources/jobs.yml` in your repo directly.)

**Step 3 — Edit `resources/jobs.yml`:** uncomment the placeholder for your table (or add a new task block in the right Round section). The required shape:

```yaml
- task_key: condition_occurrence
  depends_on:
    - task_key: person
    - task_key: visit_occurrence
  pipeline_task:
    pipeline_id: ${resources.pipelines.omop_condition_occurrence.id}
    full_refresh: true
```

**Step 4 — Validate the bundle:**

```bash
databricks bundle validate -t prod
```

If validation fails, fix the error (usually a missing pipeline resource or a typo in a `task_key` reference) and re-run. Only then `databricks bundle deploy`.

## Common errors

- **`unknown resource omop_<table>`:** the pipeline definition is missing from `resources/pipeline_generic.yml`. Add it before referencing the pipeline_id in `jobs.yml`.
- **`task <name> not found in depends_on`:** the upstream `task_key` doesn't exist in the same job. Check the round-by-round build order — Round 3 tasks can depend on Round 1 + Round 2 task_keys, never on tasks that haven't been added yet.
- **`unknown field full_refresh`:** validate your DAB CLI version supports `pipeline_task.full_refresh` (Databricks CLI ≥ 0.230). Older versions silently drop the flag and your refresh will be incremental.
