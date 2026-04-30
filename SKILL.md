---
name: omop-pipeline-builder
description: "Use when scaffolding YAML transform configs from EHR bronze tables into OMOP CDM v5.4 silver, adding rows to the source_to_concept_map UC table (via direct SQL or a git-tracked bootstrap CSV) to map institution-specific codes to standard concepts, validating materialized OMOP silver tables (5-layer schema/PK/RI/domain/completeness checks), or wiring new OMOP tables into the resources/jobs.yml DAG. Triggers on YAML config authoring, vocabulary concept_id resolution strategy decisions (source_to_concept_map vs concept_table vs concept_table_mapped), or starting Spark Declarative Pipeline updates for specific OMOP tables."
license: MIT
compatibility: Designed for Databricks Genie Code Agent mode launched from a notebook with a Python-capable cluster (serverless or classic). Genie Code launched from the catalog browser, a Genie Space, or the SQL editor is backed by a SQL warehouse and CANNOT run this skill's Step 4 Pydantic validator — see "Compute requirements" below. Requires databricks-sdk, pyyaml, pydantic. Run pipeline triggering uses Pipelines Editor native run when available, scripts/run_pipeline.py from notebooks.
metadata:
  author: Samuel Selvan (Databricks SA)
  version: "1.4"
  built_for_session: "2026-04-29 OMOP transform framework hands-on"
  v1_1_notes: "Vendor-neutralized; standalone YAML validator (no host-repo cd); workspace-scope deploy; canonical example flipped to fact-table shape."
  v1_2_notes: "STCM guidance reframed: source_to_concept_map UC table is the runtime source of truth; CSV seed at seed_data/source_to_concept_map_custom.csv is one of two write paths (alongside direct SQL/MERGE), not the only path. Added 'Adding source_to_concept_map mappings' section."
  v1_3_notes: "Compute requirements made explicit: skill requires a Python-capable notebook kernel; catalog browser / Genie Space / SQL editor surfaces (SQL warehouse compute) cannot run the Pydantic validator. Added 'Compute requirements' section. MANDATORY rule 2 now short-circuits with a verbatim user message instead of retrying when Python is unavailable."
  v1_4_notes: "Path A canonical example switched from INSERT to MERGE INTO with composite key (source_code, source_vocabulary_id) — matches src/01_load_vocabulary.py's verb so framework is internally consistent on STCM writes; idempotent on re-runs. INSERT demoted to 'first-bootstrap-only' aside."
---

# OMOP Pipeline Builder

Skill for authoring YAML-driven SDP (Spark Declarative Pipelines) transforms from EHR source bronze tables into OMOP CDM v5.4 tables in Unity Catalog, validating silver output, and triggering pipeline updates. Works with the shared config schema (`configs/_schema.yaml`) and the `source_to_concept_map` reference table in UC, which can be populated by direct SQL/MERGE or by the git-tracked bootstrap seed CSV at `seed_data/source_to_concept_map_custom.csv` (see [Adding source_to_concept_map mappings](#adding-source_to_concept_map-mappings)).

## Compute requirements (read before launching)

This skill **must be launched from a notebook** with a Python-capable cluster attached (serverless or classic). The Step 4 validator (`scripts/validate_yaml_schema.py`) is Pydantic — it requires a Python interpreter. The skill cannot run on a SQL warehouse.

Genie Code can be invoked from several surfaces. Only the notebook surface works for config generation:

| Launch surface | Backing compute | Skill works? |
|---|---|---|
| Notebook (any cluster, any DBR, serverless or classic) | Notebook kernel | **Yes — use this surface.** |
| Catalog browser, Genie Space, SQL editor | SQL warehouse | No — Pydantic validation cannot execute. The agent will not be able to satisfy MANDATORY rule 2 below. |
| Workflow / Job context | Job kernel | Yes (less common entry point). |

If you are reading this skill from a SQL-warehouse-backed surface, stop and reopen the same prompt from a notebook. Do not try SQL-only workarounds — there is no SQL equivalent of the Pydantic schema. SQL-only Q&A about OMOP (e.g., "what columns does condition_occurrence need?") is fine on any surface; YAML generation is not.

## MANDATORY — Read before every task

You MUST follow these three rules on every config generation. Do not skip any of them.

1. **Follow the Canonical YAML example in this skill before generating.** Read the [Canonical YAML example](#canonical-yaml-example) section below. Every generated config must match that structure exactly — `vocabulary_lookups` with `resolution` + `source_alias` + `fallback`, `expectations` as `{name, expr}` objects, `{catalog}.{bronze_schema}` placeholders in sources.

2. **Validate before presenting.** After writing the YAML, import the standalone validator (`from validate_yaml_schema import validate`) and call `validate("/Workspace/Users/<your_user>/configs/your.yaml")`. Confirm 0 Pydantic errors. See [Step 4](#step-4--review-edit-and-validate-yaml) for the full 3-step Python pattern (CLI form is the always-safe fallback). If errors exist, fix the YAML and re-validate. Do NOT present the config or any summary to the user until validation passes with 0 errors. **If `executeCode` cannot run Python — e.g., it errors that the runtime is a SQL warehouse, or import/`%pip` operations fail with "Python is not supported" — STOP immediately. Do not retry, do not attempt SQL-only workarounds, do not generate the YAML without validation. Tell the user verbatim: "This skill needs a Python-capable notebook kernel. Open Genie Code from a notebook (any cluster — serverless works) and rerun the same prompt. The catalog browser, Genie Space, and SQL editor surfaces back the agent with a SQL warehouse and cannot execute the Pydantic validator." See [Compute requirements](#compute-requirements-read-before-launching).**

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
- Add rows to the `source_to_concept_map` UC table (the runtime source of truth) — either via direct SQL/MERGE for ongoing ops, or via a git-tracked bootstrap CSV for repo-shipped mappings (see [Adding source_to_concept_map mappings](#adding-source_to_concept_map-mappings))
- Validate a materialized OMOP table in `core_omop` (schema, keys, concept FKs, domains, null rates)
- Start and monitor a Spark Declarative Pipeline (SDP) update for a specific `table_name` parameter
- Understand EHR-to-OMOP column semantics, vocabulary domains, or the `visit_type_concept_id` provenance rule

Pair with the **snake-case-column-renamer** skill when bronze still uses PascalCase EHR source names; this skill assumes you either keep PascalCase in YAML expressions or have already landed snake_case consistently.

## What this skill does

1. Documents the end-to-end workflow from bronze inspection through YAML editing, validation, and pipeline run.
2. Provides `scripts/generate_config.py` to `DESCRIBE` a bronze table via SQL and emit a stub YAML (sources, joins, `column_mappings`, TODO blocks for `vocabulary_lookups` and `expectations`).
3. Provides `scripts/generate_source_mappings.py` to resolve distinct source codes against `{catalog}.{ref_schema}.concept` and emit an OHDSI-shaped CSV that can be merged into the `source_to_concept_map` UC table (the runtime source of truth — see [Adding source_to_concept_map mappings](#adding-source_to_concept_map-mappings)).
4. Provides `scripts/validate_omop.py` to run five validation layers against a target table.
5. Provides `scripts/run_pipeline.py` to call `WorkspaceClient.pipelines.start_update` with optional `table_name` parameters and poll to completion.
6. Ships reference docs for CDM columns, EHR source mappings, and vocabulary domains.

## Step-by-step workflow

### Step 0 — Discover source schema

There is **no setup file to drop before using this skill**. Installing the skill is the only setup. The agent discovers the build context lazily — asks once, verifies against UC, and (optionally) persists what it learned at the end of Step 4 so the next session is a fast path.

**Cold-start workflow (first session, no `discovery.yaml` exists yet):**

1. Ask the user once: "What catalog and bronze schema are we working in?"
2. Confirm the schema actually exists:

   ```sql
   SHOW SCHEMAS IN <catalog>;
   SHOW TABLES IN <catalog>.<bronze_schema>;
   ```

3. Ask the user which bronze table maps to the OMOP target you're building (offer a guess from the `SHOW TABLES` output if it's obvious — e.g., `patient` for OMOP `person`, but always confirm).
4. Proceed to Step 1.

**Fast-path workflow (`discovery.yaml` exists in the user workspace):**

If `/Workspace/Users/<your_user>/discovery.yaml` already exists from a prior session, the agent should still **verify against UC** before using it — schemas and tables drift, and a stale `discovery.yaml` is worse than no `discovery.yaml`. If a referenced schema or table no longer exists, treat it as a cold start and re-ask the user. The standalone shape:

```yaml
catalog: <your_catalog>
bronze_schema: <your_bronze_schema>
ref_schema: reference
table_mappings:
  person: <your_patient_table>
  visit_occurrence: <your_encounter_table>
  condition_occurrence: <your_encounter_diagnosis_table>
```

This file is an **artifact the agent writes at the end of Step 4 with user consent** (see Step 4's "Persist context" sub-step). It's never a precondition. Users do not edit it manually unless they want to.

**Build order (OMOP DAG, see [`references/omop_dag_dependencies.md`](references/omop_dag_dependencies.md)):**

Round 1: `person`, `care_site`, `provider`, `location` (no dependencies)
Round 2: `visit_occurrence`, `observation_period` (depend on `person`)
Round 3: `condition_occurrence`, `procedure_occurrence`, `drug_exposure`, `measurement` (depend on `person` + `visit_occurrence`)
Round 4: `condition_era`, `drug_era` (depend on Round 3)

Build in dependency order — the validator's L3 referential integrity layer needs upstream tables to exist.

**Key output from Step 0:**
- The actual catalog and bronze schema (verified against UC)
- The actual bronze table for the OMOP target you're about to build
- Pass these forward as explicit `--catalog`, `--bronze-schema`, `--bronze-table` to Step 3, OR (fast-path) via `--discovery-file` if an up-to-date `discovery.yaml` exists.

### Step 1 — Confirm the target OMOP table

Agree on the OMOP CDM v5.4 target (for example `person`, `visit_occurrence`, `condition_occurrence`). Confirm the Unity Catalog names: `<your_catalog>`, `core_omop` for silver, `<your_bronze_schema>` for bronze, `reference` for vocab. Use **three-part** names only: `{catalog}.{schema}.{table}`.

### Step 2 — Inspect the bronze source

Run `DESCRIBE TABLE` (or `information_schema.columns`) on the bronze table(s). Note column names exactly as they appear in UC (PascalCase or snake_case), key columns (the patient identifier, the encounter identifier, etc.), and code columns that will need vocabulary resolution. YAML expressions must reference the actual column names present in bronze.

### Step 3 — Generate config via `scripts/generate_config.py`

**Before generating, read `configs/_schema.yaml` for the required YAML structure, then read `configs/person.yaml` as a working example of overall file shape.** All generated configs MUST follow the same structural shape — especially `vocabulary_lookups` (requires `resolution`, `source_alias`, `fallback`) and `expectations` (requires `{name, expr}` objects). For **fact tables** (`condition_occurrence`, `procedure_occurrence`, `drug_exposure`), the structural rules — two-lookup pattern, `domain_id` on both lookups, hash-with-resolved-concept-id surrogate keys — are demonstrated by the [Canonical YAML example](#canonical-yaml-example) below. `configs/person.yaml` is the file-shape template; the inline canonical is the fact-table-rules template. They are not competing canonical poles.

The generator supports two invocation modes. Use **explicit mode** by default — it requires no setup and is what cold-start sessions use. Use **lookup mode** as an opportunistic fast path if an up-to-date `discovery.yaml` already exists from a prior session (and remember: always verify against UC before trusting it).

**Explicit mode (default — no setup file required):**

```bash
python scripts/generate_config.py \
  --bronze-table <your_catalog>.<your_bronze_schema>.<your_patient_table> \
  --omop-table person \
  --catalog <your_catalog> \
  --bronze-schema <your_bronze_schema> \
  --output ./configs/person.yaml
```

**Lookup mode (fast path — only if `discovery.yaml` exists and is fresh):**

```bash
python scripts/generate_config.py \
  --discovery-file /Workspace/Users/<your_user>/discovery.yaml \
  --omop-table person \
  --output ./configs/person.yaml
```

`--catalog`, `--bronze-schema`, and the bronze table FQN are resolved from `discovery.yaml` by `--omop-table`. The agent should still confirm the resolved values against `SHOW SCHEMAS` / `SHOW TABLES` before invoking — a stale `discovery.yaml` will silently send the script at the wrong table.

Requirements: `databricks-sdk`, `pyyaml`. SQL warehouse ID auto-discovers (first running serverless warehouse) — override with `--warehouse-id` or `DATABRICKS_WAREHOUSE_ID`. The script writes a pure pass-through YAML stub matching the shared config schema (see `configs/_schema.yaml`): one `column_mappings` entry per bronze column with `target: snake_case(col), expr: "src.<Col>"`. The agent then rewrites most of `column_mappings` based on the OMOP target columns, the resolution decision tree (MANDATORY rule 3), and the canonical `condition_occurrence` example below. The generator is honest about not knowing your domain semantics — it doesn't pretend.

### Step 4 — Review, edit, and validate YAML

Starting from the pure pass-through scaffold emitted by `scripts/generate_config.py`, rewrite or fill in:

- `joins` when multiple `sources` are listed (the scaffold emits a single `src` source — replace with the right alias and add joins as needed)
- `vocabulary_lookups` — the scaffold emits an empty list. If a lookup uses `resolution: source_to_concept_map`, also plan how the required rows will land in the UC `source_to_concept_map` table — see [Adding source_to_concept_map mappings](#adding-source_to_concept_map-mappings).
- `expectations` (`fail` / `drop` / `warn`) appropriate to the table — the scaffold emits empty fail/drop/warn lists
- `column_mappings` — the scaffold emits one pass-through entry per bronze column (`target: snake_case(col), expr: src.<Col>`); rewrite each entry to the correct OMOP target column, drop columns that don't belong, add CASE expressions or vocabulary-resolved references as needed

Replace any hardcoded catalog/schema names with `{catalog}` and `{bronze_schema}` placeholders in `sources[].table`. The pipeline's `config_loader.py` substitutes these at runtime from Spark conf. See `configs/person.yaml` for the pattern.

**BEFORE presenting the config to the user, validate it against the Pydantic schema using `scripts/validate_yaml_schema.py` — the standalone validator that ships with this skill (no host-repo `cd` required):**

The validator has two interchangeable surfaces. Use whichever fits your `executeCode` runtime.

**Python (preferred — kernel survives across executeCode calls within a session):**

```python
import sys
sys.path.insert(0, "/Workspace/.assistant/skills/omop-pipeline-builder/scripts")
from validate_yaml_schema import validate
cfg = validate("/Workspace/Users/<your_user>/configs/your_table.yaml")
print(f"OK: {cfg.table_name}, {len(cfg.column_mappings)} columns, {len(cfg.vocabulary_lookups)} lookups")
```

The `sys.path.insert` literal `/Workspace/.assistant/skills/omop-pipeline-builder/scripts` is fixed thanks to workspace-scope deploy — the same path works for every user invoking the skill. The kernel persists across `executeCode` calls within a session, so `from validate_yaml_schema import validate` only pays the import cost once.

**CLI (always-safe fallback):**

```bash
python /Workspace/.assistant/skills/omop-pipeline-builder/scripts/validate_yaml_schema.py \
  /Workspace/Users/<your_user>/configs/your_table.yaml
```

Exit code 0 = valid; exit code 1 = invalid (errors printed to stderr).

**If validation fails:** fix the YAML and re-validate. Repeat until it passes. Do not present to the user or produce any summary until 0 errors. Common fixes:
- `vocabulary_lookups` must use `resolution: source_to_concept_map` or `resolution: concept_table` — not custom strategies like `case_map`. If a code set is small (< 10 values), use a CASE expression in `column_mappings` instead of a vocabulary lookup.
- `expectations` items must be `{name: "stable_id", expr: "SQL boolean"}` objects — not plain strings. The `name` is required for SDP telemetry dashboards.
- `source_alias` is required on every vocabulary lookup — must match an alias from `sources`.
- `sources[].table` must use `{catalog}` and `{bronze_schema}` placeholders — never hardcode catalog names.

**Only after validation passes:** present the config. The following is an example completion format — adapt to the surface (notebook, IDE, chat):

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

**If user picks 2 (review vocab choices):** For each vocabulary lookup, explain in one sentence why that resolution strategy was chosen. Example: "The diagnosis-code column uses concept_table_mapped because ICD-10 codes are non-standard in OMOP — they need the Maps to crosswalk to get standard SNOMED concept_ids. Race uses source_to_concept_map because your race codes are institution-specific and don't exist in OHDSI Athena."

**If user picks 3 (just commands):** Emit three commands, no explanation:
```
CATALOG=your_catalog ./deploy.sh production
databricks bundle run omop_full_build -t production
# After pipeline completes: open src/99_validate_omop_output.py, set table={table_name}, Run All
```
If the pipeline resource and job task don't exist yet for this table, say so and offer: "Ask me to wire {table_name} into the DAG — I'll generate the pipeline resource and job task YAML."

#### Persist context (offer once per session)

After validation passes, if `/Workspace/Users/<your_user>/discovery.yaml` does NOT already exist, offer:

> "I learned your catalog (`<catalog>`), bronze schema (`<bronze_schema>`), and that OMOP `<table_name>` reads from `<bronze_table>`. Want me to save these to `discovery.yaml` so I don't have to ask next time?"

If yes, write a YAML file to the user workspace path with the discovered values:

```python
import yaml
discovery = {
    "catalog": "<catalog>",
    "bronze_schema": "<bronze_schema>",
    "ref_schema": "reference",
    "table_mappings": {"<table_name>": "<bronze_table>"},
}
path = "/Workspace/Users/<your_user>/discovery.yaml"
with open(path, "w") as f:
    yaml.safe_dump(discovery, f, default_flow_style=False, sort_keys=False)
print(f"Saved: {path}")
```

If the file ALREADY exists (because you read it in fast-path mode at the start of this session), append the new table mapping instead of overwriting:

```python
import yaml
path = "/Workspace/Users/<your_user>/discovery.yaml"
with open(path) as f:
    doc = yaml.safe_load(f) or {}
doc.setdefault("table_mappings", {})["<table_name>"] = "<bronze_table>"
with open(path, "w") as f:
    yaml.safe_dump(doc, f, default_flow_style=False, sort_keys=False)
print(f"Updated: {path}")
```

The offer is a courtesy, not a requirement — if the user declines, never ask again in the same session. If the user accepts and you've already added the OMOP→bronze mapping for the table you just generated, you do NOT need to re-ask on the next OMOP target in the same session — read the file, append the new mapping, save.

### Step 5 — Run the pipeline (dual mechanism)

With the YAML validated (Step 4), trigger the pipeline so it materializes the OMOP table.

- **Lakeflow Pipelines Editor:** open the pipeline in the Databricks UI and click **Run**. Set pipeline parameters (for example `table_name`) in the UI if your bundle passes them via Spark conf.
- **Notebook or local automation:** run `scripts/run_pipeline.py` with the pipeline ID or name, target OMOP table name for `parameters`, and optional `--full-refresh`:

```bash
python scripts/run_pipeline.py \
  --pipeline-id "<uuid>" \
  --table person \
  --full-refresh
```

(Auth is handled by the Databricks runtime when invoked from Genie Code Agent. `--profile` only applies for local development against `~/.databrickscfg`.)

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

Once the pipeline runs green and `validate_omop.py` passes 5/5 layers, add the table to `resources/jobs.yml`. The OMOP-specific rules (`full_refresh: true` mandatory, explicit `depends_on`, `bundle validate` gate) and step-by-step workflow live in [`references/dag_wiring.md`](references/dag_wiring.md). The dependency chart for build order lives in [`references/omop_dag_dependencies.md`](references/omop_dag_dependencies.md).

## Canonical YAML example

This is the inline canonical example — the fact-table shape (`condition_occurrence`) that demonstrates the highest-failure structural rules: two-lookup pattern (standard + source concept), `domain_id` requirement on both, hash-based surrogate keys with the resolved `*_concept_id` in the hash. Match this shape exactly when generating fact-table configs (`condition_occurrence`, `procedure_occurrence`, `drug_exposure`).

For `person` (dimension-table shape) and `measurement` (specialized "Maps to + Maps to unit" pattern), see [`references/canonical_examples.md`](references/canonical_examples.md). Read order: simplest (`person`) → canonical fact table (this section) → specialized (`measurement`).

**Every fact table needs TWO vocabulary lookups per coded column** — one for the standard concept, one for the source concept:

```yaml
# Source column names are illustrative — substitute the columns from your bronze table.
# The structural patterns (resolution strategies, two-lookup rule, hash keys, domain_id) apply regardless.
table_name: condition_occurrence
target_schema: core_omop
description: "OMOP CDM v5.4 condition_occurrence from an encounter_diagnosis-shaped table."

sources:
  - alias: dx
    table: "{catalog}.{bronze_schema}.<your_encounter_diagnosis_table>"
  - alias: enc
    table: "{catalog}.{bronze_schema}.<your_encounter_table>"

joins:
  - left: dx
    right: enc
    type: left
    condition: "dx.encounter_id = enc.encounter_id"

vocabulary_lookups:
  # Standard concept: ICD-10 → SNOMED via Maps to crosswalk
  - source_alias: dx
    source_column: diagnosis_code
    target_column: condition_concept_id
    resolution: concept_table_mapped
    vocabulary_id: ICD10CM
    domain_id: Condition
    fallback: 0
  # Source concept: ICD-10 concept directly (non-standard, for traceability)
  - source_alias: dx
    source_column: diagnosis_code
    target_column: condition_source_concept_id
    resolution: concept_table
    vocabulary_id: ICD10CM
    domain_id: Condition
    standard_only: false
    fallback: 0

column_mappings:
  # Include condition_concept_id in key for one-to-many fan-out safety
  - target: condition_occurrence_id
    expr: "xxhash64(CONCAT_WS('|', CAST(dx.encounter_id AS STRING), dx.diagnosis_code, CAST(condition_concept_id AS STRING)))"
  - target: person_id
    expr: "CAST(enc.patient_id AS BIGINT)"
  - target: condition_start_date
    expr: "DATE(dx.diagnosis_datetime)"
  - target: condition_type_concept_id
    expr: "32817"
  - target: condition_source_value
    expr: "dx.diagnosis_code"

expectations:
  fail:
    - name: valid_pk
      expr: "condition_occurrence_id IS NOT NULL"
    - name: valid_person
      expr: "person_id IS NOT NULL"
  warn:
    - name: known_condition
      expr: "condition_concept_id != 0"
```

**Why this shape:**
- `concept_table_mapped` for the standard concept (ICD-10 is non-standard in OMOP; SNOMED is standard)
- `concept_table` with `standard_only: false` and `domain_id` for the source concept (traceability)
- `domain_id` is required on both lookups — filters one-to-many fan-out to the correct domain and satisfies Pydantic validation
- Surrogate key includes `condition_concept_id` in the hash for one-to-many fan-out safety
- `vocabulary_lookups` are evaluated before `column_mappings`, so the surrogate-key expression can reference `condition_concept_id` by name
- `expectations.*[]` items are `{name, expr}` objects (not bare strings); `sources[].table` uses `{catalog}` and `{bronze_schema}` placeholders (never hardcoded catalog names)

## Edge cases and known limitations

The canonical example above documents the structural rules for `concept_table_mapped` (two-lookup pattern, `domain_id` requirement, hash-with-concept-id). This section covers cases the canonical doesn't show:

- **One-to-many fan-out scope.** When `concept_table_mapped` produces multiple target concepts for one source code, cross-domain targets (e.g., Observation-domain targets from an ICD-10 code) are filtered out by `domain_id` and should be picked up when building the `observation` table.
- **`relationship_id` overrides for measurement.** Default "Maps to" works for condition/procedure/drug. For measurement, also consider `relationship_id: "Maps to unit"` (resolves LOINC → UCUM unit) and `relationship_id: "Maps to value"` (resolves LOINC → categorical value concepts). See [`references/canonical_examples.md`](references/canonical_examples.md) for the measurement pattern.
- **`visit_type_concept_id` is not visit kind.** In OMOP it records **provenance** (how the visit row was captured). For encounter rows from an EHR, use concept **32817** ("EHR"). Clinical visit type belongs in `visit_concept_id`.
- **Vocabulary lookups for non-trivial codes:** codes that do not exist as `concept_code` in the right `vocabulary_id` need `source_to_concept_map`, cross-vocabulary relationships, or manual OHDSI workflow — not a bare string match on `concept` alone.
- **`generate_config.py` is a pass-through scaffolder.** It emits `{target: snake_case(col), expr: "src.<Col>"}` for every bronze column. The agent rewrites column_mappings, joins, vocabulary_lookups, and expectations using the canonical example and resolution decision tree. The script does not guess domain semantics.
- **`validate_omop.py` domain checks** cover common `*_concept_id` columns listed in the reference spec; exotic columns may need extending the spec or script.
- **CPT4** is often missing until Athena CPT4 license steps complete; validation against procedure concepts may show gaps until vocab is complete.
- **SQL warehouse cost.** Large `DISTINCT` scans in `generate_source_mappings.py` can be expensive on wide bronze tables — filter early if needed. The standalone schema validator (`scripts/validate_yaml_schema.py`) does NOT need a warehouse — it's pure Pydantic, fast, runs anywhere.
- **Pipeline parameters** must match how your SDP code reads `spark.conf` (for example `table_name`); mismatches cause the wrong flow or missing config path.
- **Surrogate key stability under full_refresh.** OMOP rebuilds are batch snapshots. `ROW_NUMBER() OVER (ORDER BY ...)` produces unstable keys across runs because row ordering is non-deterministic when source data changes. `xxhash64(CONCAT_WS('|', ...))` is deterministic and idempotent — same input, same key, regardless of rebuild timing. `ROW_NUMBER` also requires a single-reducer global sort that does not scale on large fact tables. **Always prefer `xxhash64`.**

## Adding `source_to_concept_map` mappings

**The runtime source of truth is the Delta table `{catalog}.{ref_schema}.source_to_concept_map` in UC.** SDP pipelines join to this table at run time (see `src/vocab_resolver.py`); the resolver does not read CSVs. A lookup with `resolution: source_to_concept_map` will silently return `0` (the fallback) for any code that does not have a row in this table.

There are two supported paths for adding rows. Pick based on who owns the mapping and how often it changes.

**Path A — Direct table writes (recommended for ongoing ops, integrations, large/changing mappings):**

Write to the table via SQL. Works for one-off rows, scheduled MERGEs from upstream reference systems, and Lakeflow Connect ingestion. Mappings are governed by UC ACLs and audited like any other Delta table; updates take effect immediately without redeploying the pipeline.

**Default to `MERGE INTO`, not `INSERT INTO`.** The same rows MERGE'd twice are a no-op; the same rows INSERT'd twice create duplicates. `src/01_load_vocabulary.py` (Path B) already uses MERGE on this table — keeping Path A on MERGE means the framework has one consistent verb for STCM writes, so muscle memory transfers between the two paths and between ad-hoc additions and scheduled jobs.

```sql
MERGE INTO `{catalog}`.`{ref_schema}`.`source_to_concept_map` AS t
USING (
  VALUES
    ('Heart Rate',              0, 'Flowsheet', 'Heart rate (LOINC 8867-4)',     3027018, 'LOINC', DATE'1970-01-01', DATE'2099-12-31', NULL),
    ('Blood Pressure Systolic', 0, 'Flowsheet', 'Systolic BP (LOINC 8480-6)',    3004249, 'LOINC', DATE'1970-01-01', DATE'2099-12-31', NULL)
) AS s (
  source_code, source_concept_id, source_vocabulary_id,
  source_code_description, target_concept_id, target_vocabulary_id,
  valid_start_date, valid_end_date, invalid_reason
)
ON  t.source_code           = s.source_code
AND t.source_vocabulary_id  = s.source_vocabulary_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

The composite key `(source_code, source_vocabulary_id)` is required: `source_code` alone is not unique across vocabularies (e.g., `'01'` may legitimately exist in both `Race` and `Ethnicity` vocabularies). Matching on `source_code` alone would silently overwrite cross-vocab rows. This is the same composite key `src/01_load_vocabulary.py` uses.

`INSERT INTO ... VALUES (...)` is acceptable for the very first bootstrap of a brand-new vocabulary where no existing rows could collide — but the moment a re-run is conceivable (notebook re-execution, partial-failure recovery, scheduled top-up), use MERGE.

**Path B — Git-tracked bootstrap CSV (recommended for small, stable, repo-shipped mappings):**

`src/01_load_vocabulary.py` (one-time setup notebook) MERGEs `seed_data/source_to_concept_map_custom.csv` into the table on each run. Use this path for mappings that should travel with the codebase: foundational race / ethnicity / gender / visit-type / language code crosswalks. Edit the CSV, commit, redeploy, re-run the load notebook.

`scripts/generate_source_mappings.py` produces a CSV in this exact OHDSI shape from a distinct-codes scan of bronze. Treat its output as a starting point for Path B (or as INSERT-statement source material for Path A) — it cannot resolve codes that are not already in `concept` for the given `source_vocabulary_id`, so unresolved rows are written with `target_concept_id = 0` and need manual mapping.

**Picking a path:**

| Mapping property | Path A (table writes) | Path B (CSV seed) |
|---|---|---|
| Owner | Data engineering / stewards | Repo / framework code |
| Update cadence | Anytime (no redeploy) | At deploy time |
| Volume | Any | Small (typically < a few hundred rows) |
| Source of truth | The table itself | The CSV in git, MERGEd into the table |
| Best fit | Org-wide reference systems, Lakeflow Connect, ad-hoc additions | Foundational code crosswalks shipped with the repo |

Both paths converge on the same physical table — the pipeline does not care which path put the row there. Mixing both is fine: bootstrap mappings live in the CSV; ongoing additions go through SQL.

## Healthcare / OMOP tokens

**OMOP table names this skill recognizes (clinical scope):**

`person`, `observation_period`, `visit_occurrence`, `condition_occurrence`, `procedure_occurrence`, `drug_exposure`, `measurement`, `observation`, `death`, `location`, `care_site`, `provider`, `condition_era`, `drug_era`

**Concept domain names (and typical `*_concept_id` columns):**

`Gender`, `Race`, `Ethnicity`, `Visit`, `Type Concept`, `Condition`, `Procedure`, `Drug`, `Measurement`, `Observation`, `Unit`, `Route`, `Place of Service`, `Relationship` (non-exhaustive; see `references/vocabulary_domains.md`)

**Note:** OHDSI Athena uses `Type Concept` as the domain_id for provenance columns (`visit_type_concept_id`, `condition_type_concept_id`, etc.) — not `Visit Type` or `Condition Type`.

## Not for

This skill does NOT handle:
- **Phase 5+ tables** (note, note_nlp, specimen, episode, fact_relationship) — see `references/omop_dag_dependencies.md` for the out-of-scope list
- **SCD2 / slowly changing dimensions** — use SDP's `create_auto_cdc_flow` directly (see the production path in the runbook)
- **OHDSI Atlas / Achilles / White Rabbit** — separate OHDSI tools for cohort building, data quality dashboards, and source profiling
- **Cross-network federated research** — OMOP network study participation requires infrastructure beyond this ETL framework

If you need any of the above, this skill is the wrong tool.

## Quality contract — read before modifying

Before modifying SKILL.md, reference files, or scripts, read `tests/llm_regression/SKILL_INVENTORY.md` for the documented behavioral contract — which strategies are tested, which fixtures exist, and what the LLM is expected to produce. Then run the regression harness AND the schema drift test:

```bash
rm -rf tests/llm_regression/.harness_cache/
pytest tests/llm_regression/test_run_fixtures.py tests/test_validate_yaml_schema.py -v
```

If any fixtures fail after your change, the change broke the skill's contract. Fix the change, not the test. The drift test (`tests/test_validate_yaml_schema.py`) specifically catches divergence between the embedded Pydantic schema in `scripts/validate_yaml_schema.py` and the host schema in `src/config_loader.py` — if you change one, you MUST update the other.

## References

- [`references/canonical_examples.md`](references/canonical_examples.md) — `person` (dimension) and `measurement` (Maps to + Maps to unit) canonical YAML examples
- [`references/dag_wiring.md`](references/dag_wiring.md) — Step 7 walkthrough for adding tasks to `resources/jobs.yml`
- [`references/omop_cdm_v54_spec.md`](references/omop_cdm_v54_spec.md) — required columns, keys, FKs, concept domains (clinical scope)
- [`references/ehr_to_omop_mappings.md`](references/ehr_to_omop_mappings.md) — bronze → OMOP table and column mapping notes
- [`references/vocabulary_domains.md`](references/vocabulary_domains.md) — domain ↔ vocabulary patterns and join strategies
- [`references/omop_dag_dependencies.md`](references/omop_dag_dependencies.md) — Round 1–4 dependency chart for `resources/jobs.yml`
- [`templates/discovery.yaml`](templates/discovery.yaml) — file-shape reference for the optional `discovery.yaml` artifact (NOT a setup precondition)
- [`scripts/generate_config.py`](scripts/generate_config.py) — bronze `DESCRIBE` → pure pass-through YAML stub
- [`scripts/generate_source_mappings.py`](scripts/generate_source_mappings.py) — distinct codes → CSV in OHDSI `source_to_concept_map` shape (input for either bootstrap-CSV or direct-SQL paths; see [Adding source_to_concept_map mappings](#adding-source_to_concept_map-mappings))
- [`scripts/validate_yaml_schema.py`](scripts/validate_yaml_schema.py) — standalone Pydantic config validator (CLI + `validate(path)`)
- [`scripts/validate_omop.py`](scripts/validate_omop.py) — five-layer UC table validation
- [`scripts/run_pipeline.py`](scripts/run_pipeline.py) — start and poll pipeline updates

OHDSI CDM 5.4 canonical reference: [https://ohdsi.github.io/CommonDataModel/cdm54.html](https://ohdsi.github.io/CommonDataModel/cdm54.html)
