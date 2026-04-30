---
name: omop-pipeline-builder
description: "Use when generating, validating, or running EHR-to-OMOP-CDM-v5.4 transformations. Triggers on Person, Visit_Occurrence, Condition_Occurrence, or other OMOP CDM v5.4 tables; EHR source tables (PATIENT, PAT_ENC, PAT_ENC_DX); YAML config authoring for omop-pipeline-builder; or vocabulary concept_id resolution."
license: Proprietary
compatibility: Designed for Databricks Genie Code Agent mode. Requires databricks-sdk, pyyaml, pydantic. Run pipeline triggering uses Pipelines Editor native run when available, scripts/run_pipeline.py from notebooks.
metadata:
  author: Samuel Selvan (Databricks SA)
  version: "1.0"
  built_for_session: "2026-04-29 OMOP transform framework hands-on"
---

# OMOP Pipeline Builder

Skill for authoring YAML-driven SDP (Spark Declarative Pipelines) transforms from EHR source bronze tables into OMOP CDM v5.4 tables in Unity Catalog, validating silver output, and triggering pipeline updates. Works with the shared config schema (`configs/_schema.yaml`) and vocabulary seed data (`seed_data/source_to_concept_map_custom.csv`).

## MANDATORY — Read before every task

You MUST follow these three rules on every config generation. Do not skip any of them.

1. **Follow the Canonical YAML example in this skill before generating.** Read the [Canonical YAML example](#canonical-yaml-example) section below. Every generated config must match that structure exactly — `vocabulary_lookups` with `resolution` + `source_alias` + `fallback`, `expectations` as `{name, expr}` objects, `{catalog}.{bronze_schema}` placeholders in sources.

2. **Validate before presenting.** After writing the YAML, run `config_loader.load_config("path/to/your.yaml")` and confirm 0 Pydantic errors. If errors exist, fix and re-validate. Do NOT present the config or any summary to the user until validation passes with 0 errors.

3. **Choose the right resolution strategy — ALWAYS query the reference schema, even if an existing config exists.** An existing config may use an outdated resolution strategy. Do NOT copy resolution strategies from old configs without verifying them. Query the reference schema EVERY TIME: `SELECT COUNT(*) FROM {catalog}.{ref_schema}.concept WHERE vocabulary_id = '<vocab>' AND concept_code = '<sample_code>'`. Then apply this decision tree:
   - **Local/institution-specific codes** (race, ethnicity, visit type) that do NOT exist in the reference schema → `resolution: source_to_concept_map`
   - **Standard vocabularies that ARE the standard** for their domain (LOINC for Measurement) → `resolution: concept_table`
   - **Standard vocabularies that need crosswalk** to the domain's standard (ICD10CM→SNOMED, CPT4→SNOMED, NDC→RxNorm, ICD10PCS→SNOMED) → `resolution: concept_table_mapped` with `domain_id` set to the target OMOP table's domain. **ICD-10 codes are NOT standard in OMOP — SNOMED is. Never use `concept_table` for `condition_concept_id` when the source is ICD-10.**
   - **`*_source_concept_id` columns** (traceability — stores the non-standard source concept) → `resolution: concept_table` with `standard_only: false`
   - For `concept_table_mapped`: set `domain_id` (required — filters one-to-many to the correct domain), `relationship_id` (default "Maps to", override for "Maps to unit" or "Maps to value"), `standard_only` (default true)
   - **One-to-many fan-out:** `concept_table_mapped` may produce multiple output rows per source row (OHDSI convention). Include the resolved concept_id in surrogate key expressions.

## When to use this skill

Use this skill when the user wants to:

- Scaffold a new OMOP table transform config from a bronze EHR source table (or joined bronze sources)
- Generate `source_to_concept_map` seed rows from distinct source codes
- Validate a materialized OMOP table in `core_omop` (schema, keys, concept FKs, domains, null rates)
- Start and monitor a Spark Declarative Pipeline (SDP) update for a specific `table_name` parameter
- Understand EHR-to-OMOP column semantics, vocabulary domains, or the `visit_type_concept_id` provenance rule

Pair with the **snake-case-column-renamer** skill when bronze still uses PascalCase EHR source names; this skill assumes you either keep PascalCase in YAML expressions or have already landed snake_case consistently.

## What this skill does

1. Documents the end-to-end workflow from bronze inspection through YAML editing, validation, and pipeline run.
2. Provides `scripts/generate_config.py` to `DESCRIBE` a bronze table via SQL and emit a stub YAML (sources, joins, `column_mappings`, TODO blocks for `vocabulary_lookups` and `expectations`).
3. Provides `scripts/generate_source_mappings.py` to resolve distinct source codes against `{catalog}.{ref_schema}.concept` and emit OHDSI-shaped `source_to_concept_map` CSV.
4. Provides `scripts/validate_omop.py` to run five validation layers against a target table.
5. Provides `scripts/run_pipeline.py` to call `WorkspaceClient.pipelines.start_update` with optional `table_name` parameters and poll to completion.
6. Ships reference docs for CDM columns, EHR source mappings, and vocabulary domains.

## Step-by-step workflow

### Step 1 — Confirm the target OMOP table

Agree on the OMOP CDM v5.4 target (for example `person`, `visit_occurrence`, `condition_occurrence`). Confirm the Unity Catalog names: catalog (e.g. `samuels_fevm_catalog` for dev, `your_catalog` for your organization), `core_omop` for silver, `bronze_clinical` for bronze, `reference` for vocab. Use **three-part** names only: `{catalog}.{schema}.{table}`.

### Step 2 — Inspect the bronze source

Run `DESCRIBE TABLE` (or `information_schema.columns`) on the bronze table(s). Note PascalCase EHR source columns, key columns (`PatientID`, `EncounterID`, etc.), and code columns that will need vocabulary resolution. If the table was renamed to snake_case on landing, align YAML expressions with the actual column names.

### Step 3 — Generate config via `scripts/generate_config.py`

**Before generating, read `configs/_schema.yaml` for the required YAML structure, then read `configs/person.yaml` as a working example.** All generated configs MUST follow the same structural shape — especially `vocabulary_lookups` (requires `resolution`, `source_alias`, `fallback`) and `expectations` (requires `{name, expr}` objects). See the [Canonical YAML example](#canonical-yaml-example) section below.

Run the generator with the bronze table FQN, target OMOP table name, and optional catalog/schema overrides:

```bash
python scripts/generate_config.py \
  --bronze-table {catalog}.bronze_clinical.patient \
  --omop-table person \
  --catalog samuels_fevm_catalog \
  --bronze-schema bronze_clinical \
  --output ./configs/person.yaml
```

Requirements: `databricks-sdk`, `pyyaml`, a SQL warehouse ID (pass `--warehouse-id` or set `DATABRICKS_WAREHOUSE_ID`). The script writes a YAML stub matching the shared config schema (see `configs/_schema.yaml`) and prints the output path plus next steps.

**Note:** The generator may include internal Spark columns like `_rescued_data` or `_metadata` in the output — remove these, they are not OMOP columns.

### Step 4 — Review, edit, and validate YAML

Fill in:

- `joins` when multiple `sources` are listed
- `vocabulary_lookups` (and/or `source_to_concept_map` seeds for non-trivial codes)
- `expectations` (`fail` / `drop` / `warn`) appropriate to the table
- Any `column_mappings` the heuristics got wrong

Replace any hardcoded catalog/schema names with `{catalog}` and `{bronze_schema}` placeholders in `sources[].table`. The pipeline's `config_loader.py` substitutes these at runtime from Spark conf. See `configs/person.yaml` for the pattern.

**BEFORE presenting the config to the user, validate it against the Pydantic schema:**

```python
from config_loader import load_config
cfg = load_config("configs/your_table.yaml")
print(f"OK: {cfg.table_name}, {len(cfg.column_mappings)} columns, {len(cfg.vocabulary_lookups)} lookups")
```

**If validation fails:** fix the YAML and re-validate. Repeat until it passes. Do not present to the user or produce any summary until 0 errors. Common fixes:
- `vocabulary_lookups` must use `resolution: source_to_concept_map` or `resolution: concept_table` — not custom strategies like `case_map`. If a code set is small (< 10 values), use a CASE expression in `column_mappings` instead of a vocabulary lookup.
- `expectations` items must be `{name: "stable_id", expr: "SQL boolean"}` objects — not plain strings. The `name` is required for SDP telemetry dashboards.
- `source_alias` is required on every vocabulary lookup — must match an alias from `sources`.
- `sources[].table` must use `{catalog}` and `{bronze_schema}` placeholders — never hardcode catalog names.

**Only after validation passes:** present the config using this completion format:

```
Config validated: {table_name} — 0 errors

  {n_columns} columns mapped | {n_lookups} vocabulary lookups | {n_expectations} expectations
  Resolution: {brief strategy summary, e.g. "1x concept_table_mapped (ICD10CM→SNOMED), 1x concept_table"}

What's next — pick one, or ask me anything:

  1. Walk me through deploying this (explains deploy, pipeline run, and validation step by step)
  2. Review vocabulary choices (why each resolution strategy was picked)
  3. Deploy and run (just the commands — deploy, run pipeline, validate output)
```

**How to respond to each option:**

**If user picks 1 (walk me through):** Give one step at a time with brief explanations. Start with: "Save this YAML to `configs/{table_name}.yaml` in your repo. Then run `CATALOG=your_catalog ./deploy.sh production` — this syncs the config to the UC Volume where the pipeline reads it." After the user confirms, give the pipeline run command. Then the validation step. Link to `docs/omop-runbook.md` Section 7 for reference.

**If user picks 2 (review vocab choices):** For each vocabulary lookup, explain in one sentence why that resolution strategy was chosen. Example: "DiagnosisCode uses concept_table_mapped because ICD-10 codes are non-standard in OMOP — they need the Maps to crosswalk to get standard SNOMED concept_ids. Race uses source_to_concept_map because your race codes are institution-specific and don't exist in OHDSI Athena."

**If user picks 3 (just commands):** Emit three commands, no explanation:
```
CATALOG=your_catalog ./deploy.sh production
databricks bundle run omop_full_build -t production
# After pipeline completes: open src/99_validate_omop_output.py, set table={table_name}, Run All
```
If the pipeline resource and job task don't exist yet for this table, say so and offer: "Ask me to wire {table_name} into the DAG — I'll generate the pipeline resource and job task YAML."

### Step 5 — Run the pipeline (dual mechanism)

With the YAML validated (Step 4), trigger the pipeline so it materializes the OMOP table.

- **Lakeflow Pipelines Editor:** open the pipeline in the Databricks UI and click **Run**. Set pipeline parameters (for example `table_name`) in the UI if your bundle passes them via Spark conf.
- **Notebook or local automation:** run `scripts/run_pipeline.py` with the pipeline ID or name, target OMOP table name for `parameters`, optional `--full-refresh`, and `--profile` if not using the default credentials chain:

```bash
python scripts/run_pipeline.py \
  --pipeline-id "<uuid>" \
  --table person \
  --full-refresh \
  --profile fe-vm-serverless-stable-udlnh4
```

If you only know the pipeline name, use `--pipeline-name` (the script resolves the ID via `list_pipelines`). The script polls every 10 seconds for up to 30 minutes and prints update state and events.

### Step 6 — Validate via `scripts/validate_omop.py`

Once the pipeline run is `COMPLETED` and the OMOP table is materialized, validate it:

```bash
python scripts/validate_omop.py \
  --table {catalog}.core_omop.person \
  --ref-schema reference
```

`--catalog` and `--schema` are optional overrides; by default the FQN segments after `--table` are used.

The script reports pass/fail for five layers (schema, primary-key uniqueness, concept referential integrity, domain conformance where defined, completeness / null-rate). A non-zero exit code means at least one layer failed — fix the config or the upstream data and re-run Step 5 before proceeding to Step 7.

### Step 7 — Wire the table into the OMOP job DAG

Once the pipeline runs green and `validate_omop.py` passes 5/5 layers, add the table as a task in `resources/jobs.yml` so it runs as part of the orchestrated `omop_full_build` job.

**Read the OMOP dependency chart first:** [`references/omop_dag_dependencies.md`](references/omop_dag_dependencies.md). Find your table's round, list every predecessor it depends on (including transitive ones — explicit deps are self-documenting).

**Read the canonical DAG:** [`resources/jobs.yml`](../../../resources/jobs.yml). Each placeholder task already shows the exact YAML shape you need.

**Edit `resources/jobs.yml`:** uncomment the placeholder for your table (or add a new task block in the right Round section). The required shape:

```yaml
- task_key: condition_occurrence
  depends_on:
    - task_key: person
    - task_key: visit_occurrence
  pipeline_task:
    pipeline_id: ${resources.pipelines.omop_condition_occurrence.id}
    full_refresh: true
```

**MANDATORY rules:**

1. **`full_refresh: true` on every OMOP task.** OMOP rebuilds are batch snapshots — incremental refresh would silently double-count rows when the source is re-landed. The hand-written DAG sets this on every active and placeholder task.
2. **Explicit `depends_on` for every upstream table the pipeline reads.** List both `person` AND `visit_occurrence` for Round 3 facts even though `person` is transitive via `visit_occurrence`. Zero cost, big readability win.
3. **`pipeline_id: ${resources.pipelines.omop_<table>.id}`.** This is a DAB resource reference — the pipeline must already exist in `resources/pipeline_generic.yml`. If it doesn't, add it before editing `jobs.yml`.
4. **Validate before deploying.** Run `databricks bundle validate -t <target>` after editing — this catches typos in `pipeline_id`, malformed `depends_on` lists, and YAML syntax errors. Do NOT run `databricks bundle deploy` until validation is clean.

```bash
databricks bundle validate -t samuel-fevm
```

If validation fails, fix the error (usually a missing pipeline resource or a typo in a task_key reference) and re-run. Only then deploy.

## Canonical YAML example

This is the exact shape that the pipeline and Pydantic schema expect. Use this as the template when generating or editing configs:

```yaml
table_name: person
target_schema: core_omop
description: "OMOP CDM v5.4 Person from EHR source patient and identity_id."

sources:
  - alias: pat
    table: "{catalog}.{bronze_schema}.patient"
  - alias: id
    table: "{catalog}.{bronze_schema}.identity_id"

joins:
  - left: pat
    right: id
    type: left
    condition: "pat.PatientID = id.PatientID"

vocabulary_lookups:
  - source_alias: pat
    source_column: RaceCode
    target_column: race_concept_id
    resolution: source_to_concept_map
    source_vocabulary_id: Race
    fallback: 0
  - source_alias: pat
    source_column: EthnicityCode
    target_column: ethnicity_concept_id
    resolution: source_to_concept_map
    source_vocabulary_id: Ethnicity
    fallback: 0

column_mappings:
  - target: person_id
    expr: "CAST(pat.PatientID AS BIGINT)"
  - target: gender_concept_id
    expr: "CASE WHEN pat.GenderCode = 'M' THEN 8507 WHEN pat.GenderCode = 'F' THEN 8532 ELSE 0 END"
  - target: year_of_birth
    expr: "YEAR(pat.BirthDate)"
  - target: person_source_value
    expr: "CAST(id.MRN AS STRING)"

expectations:
  fail:
    - name: valid_person_id
      expr: "person_id IS NOT NULL"
  drop:
    - name: valid_gender
      expr: "gender_concept_id IN (8507, 8532, 0)"
  warn:
    - name: known_race_concept
      expr: "race_concept_id != 0"
```

**Key rules:**
- `vocabulary_lookups[].resolution` must be `source_to_concept_map` or `concept_table` — no custom strategies
- `vocabulary_lookups[].source_alias` is **required** — must match an alias from `sources`
- `expectations.*[]` items are **objects** with `name` (stable ID for SDP telemetry) and `expr` (SQL boolean) — not plain strings
- `sources[].table` uses `{catalog}` and `{bronze_schema}` placeholders — never hardcode catalog names

## Edge cases and known limitations

- **One-to-many vocabulary mappings (critical for `concept_table_mapped`).** One ICD-10 code can map to multiple SNOMED concepts. Per OHDSI convention, the ETL creates multiple output rows — one per target concept in the matching domain. This means one source diagnosis row may produce 2-4 `condition_occurrence` rows. **Surrogate key expressions must account for this fan-out.** Use `condition_concept_id` in your key hash or ROW_NUMBER ordering:
  ```yaml
  # WRONG — produces duplicate keys on fan-out:
  - target: condition_occurrence_id
    expr: “ROW_NUMBER() OVER (ORDER BY dx.EncounterID, dx.DiagnosisCode)”
  # RIGHT — unique across fan-out rows:
  - target: condition_occurrence_id
    expr: “ROW_NUMBER() OVER (ORDER BY dx.EncounterID, dx.DiagnosisCode, condition_concept_id)”
  ```
  Cross-domain targets (e.g., Observation-domain targets from an ICD-10 code) are filtered out by `domain_id` and should be picked up when building the `observation` table.
- **`*_source_concept_id` columns.** Use `resolution: concept_table` with `standard_only: false` to store the non-standard source concept. Use `resolution: concept_table_mapped` with `standard_only: true` (default) for the standard `*_concept_id` column. Both lookups use the same source column but produce different concept_ids.
- **`relationship_id` for measurement.** The default “Maps to” works for condition/procedure/drug. For measurement, also consider `relationship_id: “Maps to unit”` (resolves LOINC → UCUM unit) and `relationship_id: “Maps to value”` (resolves LOINC → categorical value concepts).
- **`visit_type_concept_id` is not visit kind.** In OMOP it records **provenance** (how the visit row was captured). For EHR source/EHR encounter rows, use concept **32817** (“EHR encounter record”). Clinical visit type belongs in `visit_concept_id`.
- **Vocabulary lookups for non-trivial codes:** codes that do not exist as `concept_code` in the right `vocabulary_id` need `source_to_concept_map`, cross-vocabulary relationships (`Maps to`), or manual OHDSI workflow—not a bare string match on `concept` alone.
- **`generate_config.py` heuristics** are guesses from column names; joins, vocab, and expectations always need human review.
- **`validate_omop.py` domain checks** cover common `*_concept_id` columns listed in the reference spec; exotic columns may need extending the spec or script.
- **CPT4** is often missing until Athena CPT4 license steps complete; validation against procedure concepts may show gaps until vocab is complete.
- **Statement execution** requires a running SQL warehouse; large `DISTINCT` scans in `generate_source_mappings.py` can be expensive on wide bronze tables—filter early if needed.
- **Pipeline parameters** must match how your SDP code reads `spark.conf` (for example `table_name`); mismatches cause the wrong flow or missing config path.

## Healthcare / OMOP tokens

**OMOP table names this skill recognizes (clinical scope):**

`person`, `observation_period`, `visit_occurrence`, `condition_occurrence`, `procedure_occurrence`, `drug_exposure`, `measurement`, `observation`, `death`, `location`, `care_site`, `provider`, `condition_era`, `drug_era`

**Concept domain names (and typical `*_concept_id` columns):**

`Gender`, `Race`, `Ethnicity`, `Visit`, `Type Concept`, `Condition`, `Procedure`, `Drug`, `Measurement`, `Observation`, `Unit`, `Route`, `Place of Service`, `Relationship` (non-exhaustive; see `references/vocabulary_domains.md`)

**Note:** OHDSI Athena uses `Type Concept` as the domain_id for provenance columns (`visit_type_concept_id`, `condition_type_concept_id`, etc.) — not `Visit Type` or `Condition Type`.

## Extending the skill

- **New OMOP tables:** add an abridged section to `references/omop_cdm_v54_spec.md` and extend the embedded spec in `scripts/validate_omop.py` if markdown parsing does not yet cover that table.
- **New EHR sources:** document mappings in `references/ehr_to_omop_mappings.md` and add row patterns to `configs/_schema.yaml` and `seed_data/` if they are org-wide.
- **New vocabulary or domains:** update `references/vocabulary_domains.md` and optionally add default `vocabulary_lookups` templates to `generate_config.py` heuristics.
- **institution-specific tokens:** keep org conventions in repo-level `configs/_schema.yaml` and extend seed CSVs under `seed_data/`; regenerate configs with the scripts after changing bronze layout.

## References

- [`references/omop_cdm_v54_spec.md`](references/omop_cdm_v54_spec.md) — required columns, keys, FKs, concept domains (clinical scope)
- [`references/ehr_to_omop_mappings.md`](references/ehr_to_omop_mappings.md) — EHR source → OMOP table and column mapping notes
- [`references/vocabulary_domains.md`](references/vocabulary_domains.md) — domain ↔ vocabulary patterns and join strategies
- [`references/omop_dag_dependencies.md`](references/omop_dag_dependencies.md) — Round 1–4 dependency chart for `resources/jobs.yml` (Step 7)
- [`scripts/generate_config.py`](scripts/generate_config.py) — bronze `DESCRIBE` → YAML stub
- [`scripts/generate_source_mappings.py`](scripts/generate_source_mappings.py) — distinct codes → `source_to_concept_map` CSV
- [`scripts/validate_omop.py`](scripts/validate_omop.py) — five-layer UC table validation
- [`scripts/run_pipeline.py`](scripts/run_pipeline.py) — start and poll pipeline updates

OHDSI CDM 5.4 canonical reference: [https://ohdsi.github.io/CommonDataModel/cdm54.html](https://ohdsi.github.io/CommonDataModel/cdm54.html)
