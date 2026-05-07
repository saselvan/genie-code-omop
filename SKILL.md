---
name: omop-pipeline-builder
description: "Use when scaffolding YAML transform configs from EHR bronze tables into OMOP CDM v5.4 silver, adding rows to the source_to_concept_map UC table (via direct SQL or a git-tracked bootstrap CSV) to map institution-specific codes to standard concepts, validating materialized OMOP silver tables (5-layer schema/PK/RI/domain/completeness checks), or wiring new OMOP tables into the resources/jobs.yml DAG. Triggers on YAML config authoring, vocabulary concept_id resolution strategy decisions (source_to_concept_map vs concept_table vs concept_table_mapped), or starting Spark Declarative Pipeline updates for specific OMOP tables."
license: MIT
compatibility: Designed for Databricks Genie Code Agent mode launched from a notebook with a Python-capable cluster (serverless or classic). Genie Code launched from the catalog browser, a Genie Space, or the SQL editor is backed by a SQL warehouse and CANNOT run this skill's Step 6 Pydantic validator — see "Compute requirements" below. Requires databricks-sdk, pyyaml, pydantic. Run pipeline triggering uses Pipelines Editor native run when available, scripts/run_pipeline.py from notebooks.
metadata:
  author: Samuel Selvan
  version: "2.0.7.3"
  built_for_session: "2026-04-29 OMOP transform framework hands-on"
---

# OMOP Pipeline Builder

Skill for authoring YAML-driven SDP (Spark Declarative Pipelines) transforms from EHR source bronze tables into OMOP CDM v5.4 tables in Unity Catalog, validating silver output, and triggering pipeline updates. Works with the shared config schema (`configs/_schema.yaml`) and the `source_to_concept_map` reference table in UC, which can be populated by direct SQL/MERGE or by the git-tracked bootstrap seed CSV at `seed_data/source_to_concept_map_custom.csv` (see [Adding source_to_concept_map mappings](#adding-source_to_concept_map-mappings)).

**Framing.** This skill maintains a living, governed bundle in a UC Volume. Each invocation is a delta against the current state — generation, update, or replacement, with reviewer ratification at each step. The agent re-reads bundle state before every per-table flow, so it knows whether a config already exists and whether the table is already materialized in `core_target`.

## Compute requirements (read before launching)

This skill **must be launched from a notebook** with a Python-capable cluster attached (serverless or classic). The Step 6 validator (`scripts/validate_yaml_schema.py`) is Pydantic — it requires a Python interpreter. The skill cannot run on a SQL warehouse.

Genie Code can be invoked from several surfaces. Only the notebook surface works for config generation:

| Launch surface | Backing compute | Skill works? |
|---|---|---|
| Notebook (any cluster, any DBR, serverless or classic) | Notebook kernel | **Yes — use this surface.** |
| Catalog browser, Genie Space, SQL editor | SQL warehouse | No — Pydantic validation cannot execute. The agent will not be able to satisfy MANDATORY rule 2 below. |
| Workflow / Job context | Job kernel | Yes (less common entry point). |

If you are reading this skill from a SQL-warehouse-backed surface, stop and reopen the same prompt from a notebook. Do not try SQL-only workarounds — there is no SQL equivalent of the Pydantic schema. SQL-only Q&A about OMOP (e.g., "what columns does condition_occurrence need?") is fine on any surface; YAML generation is not.

## MANDATORY — Read before every task

These three rules guide what the agent does during generation; they do not enforce correctness. The agent is drafting a config for a human reviewer to ratify. Each rule increases the chance the draft is structurally sound and the OMOP fidelity choices are visible — a clinical informaticist or OMOP-experienced engineer still reviews before the config joins the DAG.

1. **Follow the Canonical YAML example in this skill before generating.** Read the [Canonical YAML example](#canonical-yaml-example) section below. Every generated config must match that structure exactly — `vocabulary_lookups` with `resolution` + `source_alias` + `fallback`, `expectations` as `{name, expr}` objects, `{catalog}.{bronze_schema}` placeholders in sources.

2. **Validate before presenting.** After writing the YAML, import the standalone validator (`from validate_yaml_schema import validate`) and call `validate("/Workspace/Users/<your_user>/configs/your.yaml")`. Confirm 0 Pydantic errors. See [Step 6](#step-6--review-edit-and-validate-yaml) for the full 3-step Python pattern (CLI form is the always-safe fallback). If errors exist, fix the YAML and re-validate. Do NOT present the config or any summary to the user until validation passes with 0 errors. **If `executeCode` cannot run Python — e.g., it errors that the runtime is a SQL warehouse, or import/`%pip` operations fail with "Python is not supported" — STOP immediately. Do not retry, do not attempt SQL-only workarounds, do not generate the YAML without validation. Tell the user verbatim: "This skill needs a Python-capable notebook kernel. Open Genie Code from a notebook (any cluster — serverless works) and rerun the same prompt. The catalog browser, Genie Space, and SQL editor surfaces back the agent with a SQL warehouse and cannot execute the Pydantic validator." See [Compute requirements](#compute-requirements-read-before-launching).**

3. **Choose the right resolution strategy — ALWAYS query the reference schema, even if an existing config exists.** An existing config may use an outdated resolution strategy. Do NOT copy resolution strategies from old configs without verifying them. Query the reference schema EVERY TIME: `SELECT COUNT(*) FROM {catalog}.{ref_schema}.concept WHERE vocabulary_id = '<vocab>' AND concept_code = '<sample_code>'`.

   **CRITICAL — semantic-collision warning for institution-coded vocabularies (Race, Ethnicity, Visit Type):**

   When the source vocabulary uses institution-specific numeric codes (Race, Ethnicity, Visit Type — these vocabularies have institution-defined numbering schemes that vary by EHR vendor), the OHDSI `concept` table may contain the SAME code values with COMPLETELY DIFFERENT semantics. For example, Epic/Caboodle/Clarity Race code `1` typically means "White" by institutional convention, while OHDSI Race vocabulary `concept_code = '1'` means "American Indian or Alaska Native". Both rows exist in the concept table; the meanings do not match.

   A literal `concept_table` lookup against the institution's numeric codes would produce **clinically wrong results** (in a typical Caboodle Race distribution, ~61% of patients would be misclassified). The codes existing in the concept table is **not** evidence that the meanings match.

   **Rule:** For `Race`, `Ethnicity`, and `Visit Type` (and any other institution-coded numeric vocabulary), ALWAYS check `source_to_concept_map` FIRST regardless of whether the codes exist in the concept table. Use `resolution: source_to_concept_map` when an STCM mapping exists. Use `resolution: concept_table` for these vocabularies only when (a) the source codes are unambiguously OHDSI-aligned (e.g., concept_code patterns like `OMOP12345` or numeric concept_ids like `8507`), or (b) you have explicit semantic verification (the institution code N truly means the same thing as OHDSI concept_code N for the same vocabulary).

   **Decision tree (applied AFTER the collision check above):**
   - **Local/institution-specific codes** (race, ethnicity, visit type) that do NOT exist in the reference schema → `resolution: source_to_concept_map`
   - **Standard vocabularies that ARE the standard** for their domain (LOINC for Measurement) → `resolution: concept_table`
   - **Standard vocabularies that need crosswalk** to the domain's standard (ICD10CM→SNOMED, CPT4→SNOMED, NDC→RxNorm, ICD10PCS→SNOMED) → `resolution: concept_table_mapped` with `domain_id` set to the target OMOP table's domain. **ICD-10 codes are NOT standard in OMOP — SNOMED is. Never use `concept_table` for `condition_concept_id` when the source is ICD-10.**
   - **`*_source_concept_id` columns** (traceability — stores the non-standard source concept) → `resolution: concept_table` with `vocabulary_id` (required) AND `domain_id` (required — typically matches the vocabulary name; e.g., for Gender source-concept lookup use `vocabulary_id: Gender, domain_id: Gender`) AND `standard_only: false`. The Pydantic `VocabularyLookup` validator rejects `concept_table` lookups missing either field with `resolution=concept_table requires both vocabulary_id and domain_id`.
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
2. Generates each table's YAML config in conversation: the agent issues `SHOW SCHEMAS` / `SHOW TABLES` / `DESCRIBE TABLE` itself against UC, drafts the YAML in working memory against `configs/_schema.yaml` and `references/canonical_examples.md`, validates via `scripts/validate_yaml_schema.py`, and writes atomically via `scripts/config_writer.py`.
3. Provides `scripts/generate_source_mappings.py` to resolve distinct source codes against `{catalog}.{ref_schema}.concept` and emit an OHDSI-shaped CSV that can be merged into the `source_to_concept_map` UC table (the runtime source of truth — see [Adding source_to_concept_map mappings](#adding-source_to_concept_map-mappings)).
4. Provides `scripts/validate_omop.py` to run five validation layers against a target table.
5. Provides `scripts/run_pipeline.py` to call `WorkspaceClient.pipelines.start_update` with optional `table_name` parameters and poll to completion.
6. Ships reference docs for CDM columns, EHR source mappings, and vocabulary domains.

## Step-by-step workflow

### Step 1 — Scaffold the project (first time only)

Skip this step if your team already has an OMOP build repo. This step is for
the first time someone on your team is starting an OMOP CDM v5.4 build with
this skill.

> **What scaffolding does and why you only do it once.** The scaffolder writes a working DAB-shaped project tree (bundle config, jobs DAG with the 14 buildable OMOP tables as commented placeholders, src/ boilerplate, empty configs/ folder, seed_data template, README) into a customer-chosen path. The "buildable" framing reflects the validate-20-build-14 split; see "Validation scope vs build scope" later in this file for the architectural decision (AD-001). The skill writes configs into the project tree on every subsequent invocation; scaffolding only sets up the tree.
>
> **Refuse vs retry.** The scaffolder refuses to overwrite a *completed* OMOP project, where all three indicators are present at `project_path`: `databricks.yml`, `src/`, and the `.omop-skill-version` marker file (written last by every successful scaffold). Anything short of a completed scaffold is treated as retry-safe and overwritten cleanly:
>
> - **Crashed scaffold** (`databricks.yml` + `src/` present, marker missing) — disk I/O failure between the YAML write and the marker write. Re-running the scaffolder completes the partial state. No manual `rm -rf` required.
> - **Foreign `databricks.yml`** (no `src/`, no marker) — e.g., from `databricks bundle init` or hand-authoring before the customer chose the OMOP scaffolder. Re-running replaces the foreign bundle config with the OMOP one.
> - **Empty path** — fresh scaffold.
>
> Customer files *outside* the scaffolded artifact set (drafts in `configs/`, ad-hoc notes, etc.) survive retries untouched. Customer edits to scaffolded files (e.g., manual changes to `src/01_load_vocabulary.py`) are overwritten on retry — that's why retry is reserved for incomplete scaffolds, not "I want to refresh my completed project from the template."

The scaffolder also probes the silver
schema for existing OMOP tables and surfaces them so the team can decide per
table whether to keep them as-is or rebuild them through the skill.

**To scaffold:** ask the agent something like "scaffold a new OMOP project."
The agent collects four things conversationally and asks the customer to
confirm all of them before writing files:

1. **`project_path`** — filesystem path where the project tree is written.
   Customer-chosen. UC Volume mount path strongly recommended:
   `/Volumes/<catalog>/<schema>/<volume>/`. The customer picks the catalog,
   schema, and volume name that fits their UC governance.

2. **`volume_target`** — three-part UC name where the bundle deploys:
   `<catalog>.<schema>.<volume>`. **The Volume must already exist.** The
   scaffolder verifies before writing. If the Volume is missing, the
   scaffolder raises `VolumeNotFoundError` and the agent asks the customer
   to create the Volume through standard UC governance (Catalog Explorer, SQL
   `CREATE VOLUME`, or their platform team), then resumes once the customer
   confirms.

3. **`bronze_target`** — two-part UC name where the EHR landing-zone tables
   live: `<catalog>.<schema>` (e.g., `cat.bronze_clinical`,
   `cat.bronze_ehr`, `cat.bronze_landing`). **There is no safe default**
   — bronze schemas come from your EHR ingest layer and the scaffolder
   cannot guess. If you don't pass one, the scaffolder fills in a
   `<CHANGEME — your bronze schema>` placeholder.
   The placeholder is **loud at pipeline-time, quiet at validate-time:**
   `databricks bundle validate -t production` returns `Validation OK!` with
   the placeholder unreplaced (DAB validate checks structure, not variable
   values), but the pipeline run fails the moment `${bronze_schema}`
   substitutes into a source identifier — the runtime error contains the
   literal `<CHANGEME>` string. **Override the placeholder before
   deploying.** Either pass `bronze_target` at scaffold time so it never
   ships, or edit `databricks.yml`'s `bronze_schema.default` post-scaffold
   before `databricks bundle deploy`.

4. **`core_target`** — two-part UC name where OMOP tables live or will
   materialize: `<catalog>.<schema>`. Defaults to same catalog as
   `volume_target`, schema `core_omop`. Override if the engineering and
   clinical catalogs are separated for governance reasons.

The agent must echo all four values back before invoking the scaffolder, e.g.:

> Confirming before I scaffold:
> - **project_path:** `/Volumes/my_cat/raw/omop_artifacts/omop-build`
> - **volume_target:** `my_cat.raw.omop_artifacts`
> - **bronze_target:** `my_cat.bronze_clinical`
> - **core_target:** `my_cat.core_omop`
>
> All three UC targets share catalog `my_cat`. Proceed?

**Cross-catalog refusal.** All three UC targets (volume, bronze, core) must
share a single Unity Catalog. Cross-catalog OMOP builds (engineering and
clinical catalogs separated) are real but unusual; the default scaffold
refuses, naming all three values, so a typo'd catalog surfaces loudly
before any disk write or SDK call.

The agent runs `scripts/scaffold_omop_project.py` with the confirmed parameters
and reports back what was written.

**The scaffolder does NOT create UC objects.** It does not create catalogs,
schemas, or Volumes. UC governance is owned by the customer's platform team
and admins, not by this skill. The agent surfaces missing Volumes as an ask,
not as an action it can take.

> **Source of truth.** `databricks.yml` is the deployed artifact and wins on conflict — its `catalog`, `bronze_schema`, `core_schema`, and `config_volume` variables are what the pipeline actually reads at runtime. The agent's `discovery.yaml` (Step 3) is a per-user context cache that lets the next session start fast without re-asking; on session start, the agent reconciles `discovery.yaml` against UC and against `databricks.yml` and treats `discovery.yaml` as advisory if they disagree. Treat any divergence between `databricks.yml` and `discovery.yaml` as a stale cache, not a deployment bug — the customer's edits to `databricks.yml` are authoritative.

**After scaffold:**

1. Replace the `<CHANGEME>` placeholder in the generated `databricks.yml` with
   the workspace URL.
2. Validate the scaffold deploys cleanly: `databricks bundle validate -t production`.
3. Connect the project tree to the team's Git repo. The skill works without
   Git, but recovery and audit are much easier with version control.
4. Pick the first OMOP table to build (most teams start with Person), then
   proceed to Step 2.

**About existing tables:** if the scaffolder finds OMOP tables already in
`core_target`, the generated README lists them with two paths offered per
table (keep-as-is or rebuild-via-skill). The skill does not auto-generate
configs for existing tables — the team decides per table. See the generated
README section "Existing OMOP tables" for the rebuild workflow using a
side-by-side silver schema.

**About deploy:** the skill does not deploy the bundle. The customer's CI/CD
pipeline (GitHub Actions, Azure DevOps Pipeline, GitLab CI, Jenkins, whatever
the team uses) handles deploy. The agent's responsibility ends at writing
validated configs to the project tree.

### Step 2 — Confirm target & check existing state

Before generating any config, the agent reads the current bundle state
and confirms the target table(s) with the engineer. This is a state-aware
skill — the agent re-reads bundle state at the start of every flow so
recent additions (configs, materialized tables, wired tasks) are
reflected. Confirm Unity Catalog names: `<your_catalog>`, `core_omop`
for silver, `<your_bronze_schema>` for bronze, `reference` for vocab.
Use **three-part** names only: `{catalog}.{schema}.{table}`.

**Build order (OMOP DAG, see [`references/omop_dag_dependencies.md`](references/omop_dag_dependencies.md)):**

Round 1: `person`, `care_site`, `provider`, `location` (no dependencies)
Round 2: `visit_occurrence`, `observation_period` (depend on `person`)
Round 3: `condition_occurrence`, `procedure_occurrence`, `drug_exposure`, `measurement`, `observation`, `death` (depend on `person` + `visit_occurrence`)
Round 4: `condition_era`, `drug_era` (depend on Round 3)

The agent reorders multi-table requests into this DAG order automatically.
Within a single round, ties break alphabetically. Build in dependency
order — the validator's L3 referential integrity layer needs upstream
tables to exist.

**If the project is not scaffolded** (no `.omop-skill-version` marker):
the agent stops and surfaces:

> This directory doesn't look like a scaffolded OMOP project. Run the
> scaffolder first via Step 1, or point me at the right project path.

**If `configs/<table>.yaml` does NOT exist:** proceed to Step 3 normally.
This is the v1.4 single-table flow.

**If `configs/<table>.yaml` already exists:** the agent stops and offers
three sub-paths. The agent's response should look something like:

> I see `configs/person.yaml` already exists in this project. What would
> you like to do?
>
> 1. **Update** — change something specific in the existing config (e.g.,
>    different vocabulary strategy, additional CASE branches). I'll
>    regenerate the whole config with your change and describe what
>    changed.
>
> 2. **Replace** — discard the existing config and generate a fresh one
>    as if Person hadn't been built yet. Useful if the existing config
>    has drifted significantly or if your bronze schema changed.
>
> 3. **Different table** — you meant to build a different table. Which
>    one?

The agent waits for the engineer's choice. Do not guess intent; do not
generate speculatively while waiting.

**Sub-path: Update.** See Step 4 (Update workflow).

**Sub-path: Replace.** See Step 5 (Generate workflow). Replace overwrites
the existing file; the engineer commits the overwrite through their
normal git workflow.

**Sub-path: Different table.** Re-enter Step 2 with the new target table.
The classification may detect another existing config — recurse.

**Batch requests.** When the engineer asks for multiple tables in one
prompt, the agent reads bundle state once and classifies each table.
The response groups conflicts and non-conflicts:

> Build order is: **Person → Visit_occurrence → Condition_occurrence**
> (per the OMOP DAG). One conflict: `configs/person.yaml` already exists.
> For Person — update / replace / different-table? Visit_occurrence and
> Condition_occurrence don't exist; I'll build them fresh after you
> resolve the Person path.

**Missing predecessors.** When the requested batch references a table
whose predecessors aren't in the batch and aren't materialized in
`core_target` (state level 0 — no config, no task, no table), the
agent refuses with the gap surfaced:

> You asked to build Condition_occurrence. Its predecessors Person and
> Visit_occurrence aren't configured yet (no config, no task, no table).
> Add them to the batch, or build them first?

**Ambiguous extensions.** If `configs/` contains both `<table>.yaml`
and `<table>.yml`, the agent refuses:

> Both `configs/person.yaml` and `configs/person.yml` exist. The skill
> ships `.yaml` as canonical. Delete one (or rename it) and I'll proceed.

Once the target is confirmed and any existing-config branches are
resolved, proceed to Step 3.

### Step 3 — Inspect the bronze source

Run `DESCRIBE TABLE` (or `information_schema.columns`) on the bronze
table(s). Note column names exactly as they appear in UC (PascalCase or
snake_case), key columns (the patient identifier, the encounter
identifier, etc.), and code columns that will need vocabulary resolution.
YAML expressions must reference the actual column names present in bronze.

#### Discover the bronze source for this target

There is **no setup file to drop before using this skill**. Installing the skill is the only setup. The agent discovers the build context lazily — asks once, verifies against UC, and (optionally) persists what it learned at the end of Step 6 so the next session is a fast path.

**Cold-start workflow (first session, no `discovery.yaml` exists yet):**

1. Ask the user once: "What catalog and bronze schema are we working in?"
2. Confirm the schema actually exists:

   ```sql
   SHOW SCHEMAS IN <catalog>;
   SHOW TABLES IN <catalog>.<bronze_schema>;
   ```

3. Ask the user which bronze table maps to the OMOP target you're building (offer a guess from the `SHOW TABLES` output if it's obvious — e.g., `patient` for OMOP `person`, but always confirm).
4. Proceed to Step 5 once the bronze table is confirmed.

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

This file is an **artifact the agent writes at the end of Step 6 with user consent** (see Step 6's "Persist context" sub-step). It's never a precondition. Users do not edit it manually unless they want to.

**Key output from this step:**
- The actual catalog and bronze schema (verified against UC)
- The actual bronze table for the OMOP target you're about to build
- Pass these forward as explicit `--catalog`, `--bronze-schema`, `--bronze-table` to Step 5, OR (fast-path) via `--discovery-file` if an up-to-date `discovery.yaml` exists.

### Step 4 — Update workflow

The engineer chose "Update" from Step 2's three sub-paths. They want to
change something specific in an existing config (vocabulary strategy,
CASE branches, additional column mapping) without discarding the whole
file. The agent regenerates the whole config with the change
incorporated, validates against the Pydantic schema with a
retry-with-fix-forward loop, computes a structural field-level
changelog against the existing file, then writes the new file
atomically with optimistic mtime concurrency. The reviewer reads the
new file directly — the agent does not produce textual `+old/-new`
diffs (the skill produces structural-not-textual diffs by design;
see [`scripts/structural_changelog.py`](scripts/structural_changelog.py)
for the structural-diff implementation).

**Read existing state first.** Before regenerating, the agent reads
the existing `configs/<table>.yaml` text AND captures its mtime
via `os.stat(<config_path>).st_mtime`. The mtime is the optimistic
concurrency token passed to `write_config` later. The text is one
of the two inputs to the structural diff.

> **Mtime precision contract.** Pass the captured `st_mtime` float
> through to `write_config(..., expected_mtime=<float>)` **without
> truncating**. The writer compares with a 1 microsecond tolerance —
> if the agent's working memory ever serializes the value through a
> coarser surface (millisecond JSON timestamp, ISO-8601 string with
> truncated fractional seconds, etc.) and then reconstitutes it,
> false `MtimeMismatchError` raises become possible. Keep the value
> as a Python `float` end-to-end within a single Update flow.

**Generate-and-validate loop (retry-with-fix-forward, N=3).** The
agent regenerates the YAML in working memory, runs
`validate_yaml_schema.validate_text(new_yaml)`, and:

- On success → break out of the loop with the validated YAML.
- On `ValidationError` → format the Pydantic errors (one per line
  with field path and message) and feed them back into the
  regeneration prompt as "previous attempt failed validation here:
  `<formatted errors>`; produce a corrected YAML that fixes only
  these paths." Repeat up to **3 times total**.
- After 3 failures → bail to the retries-exhausted response template
  below. The original `configs/<table>.yaml` is unchanged on disk
  throughout the loop.

**Compute the structural changelog.** Once a candidate YAML
validates, the agent calls
`structural_changelog.compute_structural_changelog(old_yaml, new_yaml)`
to get an ordered list of `FieldChange` records. Each record has
`field_path`, `change_type` (`added` / `removed` / `modified`),
`old_value`, and `new_value`. Field paths use dotted snake_case for
dict keys and bracket notation for list indices, e.g.
`vocabulary_lookups[0].vocabulary_id`. The agent uses the changelog
to ground its plain-language summary — name the actual fields that
changed; do not invent.

**Write atomically with mtime guard.** The agent calls
`config_writer.write_config(project_path, target_table, new_yaml,
overwrite=True, expected_mtime=<captured_mtime>)`. Three outcomes:

- Success → `WriteResult` returned. If `result.git_warning` is set
  (project is not under git or git probe failed), surface it in the
  response template alongside the success summary; do not block.
- `MtimeMismatchError` → someone else modified or deleted the file
  between the agent's read and write. The agent surfaces the
  concurrency template below and re-enters Step 2 (which re-reads
  bundle state to get the new ground truth).
- `FileExistsError` → cannot happen here because Update always passes
  `overwrite=True`. If it does, treat as a programming bug and
  surface the raw error.

**Response template (success):**

> Updated `configs/person.yaml`. Field-level changes from the
> previous version:
>
> - `vocabulary_lookups[0].resolution`: `source_to_concept_map` →
>   `concept_table_mapped`
> - `vocabulary_lookups[0].domain_id`: added (`Gender`)
> - `column_mappings[3]`: added (`race_concept_id` ← lookup against
>   race code 'X')
>
> Plain-language summary: switched to standard-concept-only
> resolution for gender and added a race CASE branch for the
> previously-unmapped code 'X'.
>
> Open `configs/person.yaml` to review the full content. The OMOP
> fidelity checks for this update are:
>
> - `vocabulary_lookups[0].domain_id='Gender'` matches the canonical
>   pattern for concept_table_mapped on Person
> - `column_mappings[3].expr` produces 0 for unmapped race codes
>   (matches the fallback rule)
>
> Commit the change through your normal git workflow when satisfied.

**Response template (retries exhausted):**

> I couldn't compose a valid YAML for the change you requested after
> 3 attempts. The original `configs/person.yaml` is unchanged on
> disk. The validation errors I'm hitting (Pydantic uses dotted
> error paths — these are the `loc` tuples from
> `ValidationError.errors()`, NOT the bracket-notation paths the
> structural changelog uses):
>
> - `vocabulary_lookups.0.source_vocabulary_id`: required when
>   resolution=source_to_concept_map
> - `column_mappings.3.expr`: input should be a valid string
>
> The likely cause is `<plain-language diagnosis grounded in the
> errors above>`. Would you like to:
>
> 1. Reframe the change request so I can try again
> 2. Make the edit manually (here's a sketch of what I'd write:
>    `<sketch>`)
> 3. Walk through the validation errors together so we can decide
>    what to fix

**Response template (concurrency conflict):**

> `configs/person.yaml` was modified or removed by someone else
> after I read it (mtime mismatch). The original on-disk file is
> unchanged from my write attempt. Re-reading state and re-entering
> Step 2 — please re-confirm the change you want to make.

**Response template (non-Git'd project warning, append to success).**
Surface `WriteResult.git_warning` verbatim. The text is the
Decision-10 contract surface — do not paraphrase. The current verbatim
text is:

> ⓘ Project is not under git version control. The skill does not
> snapshot configs — without git or cloud-side storage versioning,
> this overwrite is not recoverable. Recommended: connect this
> project to git before further updates. Alternative: ask your
> platform team to enable versioning on the storage account backing
> this Volume.

The agent does NOT:

- Render textual `+old/-new` diffs (structural diff via
  `structural_changelog.compute_structural_changelog` is the only
  diff surface)
- Auto-commit (engineer commits through their normal git workflow)
- Auto-deploy (CI/CD pipeline owns deploy)
- Make targeted line-level edits to the existing file (regenerates
  the WHOLE config)
- Snapshot the previous version anywhere on disk (no `.bak` files;
  the customer's git history or storage versioning is the snapshot
  surface)

If "Replace" was chosen instead, see Step 5 (Generate workflow);
Replace passes `overwrite=True` with `expected_mtime=None` (no
concurrency guard — Replace's intent is to discard the existing
state, so a concurrent edit racing with us is moot). The "Different
table" sub-path re-enters Step 2 with the new target.

### Step 5 — Generate config in conversation

The agent generates the YAML config directly in the conversation. There is no CLI generator to invoke — config generation is the agent's job, end to end. The flow is the **Generate** sub-path of the same write contract that powers the Update sub-path documented in [Step 4 — Update workflow](#step-4--update-workflow); the only difference is `overwrite=False, expected_mtime=None` for greenfield writes (see sub-step 5, "Write atomically," below).

**1. Discover the bronze surface.** Issue `SHOW SCHEMAS IN <catalog>` → `SHOW TABLES IN <catalog>.<bronze_schema>` → `DESCRIBE TABLE EXTENDED <catalog>.<bronze_schema>.<table>` against the customer's workspace via the SDK statement-execution API (the same mechanism `scripts/validate_omop.py` and `scripts/generate_source_mappings.py` use). Confirm the bronze FQN, column names and types, and any obvious primary-key / timestamp columns before drafting.

**2. Read the canonical schema and exemplar.** Load `configs/_schema.yaml` (the Pydantic schema source) and `references/canonical_examples.md` (the canonical worked examples) into working memory. The schema defines what fields the YAML must have and which are required. The exemplars show the expected shape of `column_mappings`, `joins`, `vocabulary_lookups`, and `expectations`. For **fact tables** (`condition_occurrence`, `procedure_occurrence`, `drug_exposure`), the structural rules — two-lookup pattern, `domain_id` on both lookups, hash-with-resolved-concept-id surrogate keys — are demonstrated by the [Canonical YAML example](#canonical-yaml-example) below. `references/canonical_examples.md` contains file-level worked examples (`person` for dimension tables, `measurement` for fact-table-with-unit-resolution patterns); the inline canonical here shows the fact-table rule structure in isolation. They are not competing canonical poles.

**3. Draft the YAML in working memory — do not pass-through.** Pass-through means emitting `{target: snake_case(col), expr: "src.<Col>"}` for every bronze column without reasoning about whether each column actually maps to its OMOP target. The agent must instead reason about each mapping per [Step 6 — Review, edit, and validate YAML](#step-6--review-edit-and-validate-yaml)'s decision tree (cast type only / lookup vocabulary / join domain table / split or extract / leave NULL). Use `{catalog}` and `{bronze_schema}` placeholders in `sources[].table` — never hardcoded catalog or schema names; the pipeline's `config_loader.py` substitutes them at runtime from Spark conf.

**4. Validate before writing.** Call `validate_yaml_schema.validate_text(yaml_string)`. If validation fails, fix the YAML in working memory and re-validate. Do not write a file that fails Pydantic. The validate-then-fix-forward loop is bounded at N=3 attempts, identical to the Update sub-path's loop in [Step 4 — Update workflow](#step-4--update-workflow); on the 4th failure, surface the validation errors and stop.

**5. Write atomically.** Call `config_writer.write_config(project_path, target_table, yaml_string, overwrite=False, expected_mtime=None)`. Generate is the greenfield sub-path: `overwrite=False` raises `FileExistsError` if a `configs/<target_table>.yaml` already exists. If it does, the right move is the [Step 4 — Update workflow](#step-4--update-workflow) sub-path (which passes `overwrite=True` plus a captured `expected_mtime` for the concurrency guard), not blind overwrite. `expected_mtime=None` skips the concurrency guard because there is no prior version to guard against.

**6. Surface the result.** Print `WriteResult.config_path` (where the file landed), the `git_warning` if any (Decision-10 honest text when the project is not under git), and a one-line summary of mappings the engineer needs to review (any not pure cast-only — vocabulary lookups, joins, CASE expressions, hash surrogate keys). The customer sees what was written and where review attention belongs.

### Step 6 — Review, edit, and validate YAML

Starting from the YAML the agent drafted in [Step 5 — Generate config in conversation](#step-5--generate-config-in-conversation), review and refine:

- `joins` when multiple `sources` are listed (a single-source draft uses one `src` alias — add the right aliases and join clauses as additional sources are introduced)
- `vocabulary_lookups` — every lookup needs `resolution`, `source_alias`, `domain_id`, and `fallback`. If a lookup uses `resolution: source_to_concept_map`, also plan how the required rows will land in the UC `source_to_concept_map` table — see [Adding source_to_concept_map mappings](#adding-source_to_concept_map-mappings).
- `expectations` (`fail` / `drop` / `warn`) appropriate to the table — at minimum, `fail` on primary-key NOT NULL, `drop` on invalid concept resolutions, `warn` on unmapped vocabularies
- `column_mappings` — verify each entry maps a bronze column to the correct OMOP target column with the right cast or vocabulary resolution. Drop columns that don't belong, add CASE expressions or vocabulary-resolved references as needed.

Confirm `sources[].table` uses `{catalog}` and `{bronze_schema}` placeholders (never hardcoded catalog or schema names). The pipeline's `config_loader.py` substitutes these at runtime from Spark conf. See `references/canonical_examples.md` for the pattern.

**BEFORE presenting the config to the user, validate it against the Pydantic schema using `scripts/validate_yaml_schema.py` — the standalone validator that ships with this skill (no host-repo `cd` required):**

The validator has two interchangeable surfaces. Use whichever fits your `executeCode` runtime.

**Python (preferred — kernel survives across executeCode calls within a session):**

```python
import os, sys

current_user = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
user_scope_path = f"/Workspace/Users/{current_user}/.assistant/skills/omop-pipeline-builder/scripts"
workspace_scope_path = "/Workspace/.assistant/skills/omop-pipeline-builder/scripts"
skill_path = user_scope_path if os.path.exists(user_scope_path) else workspace_scope_path

sys.path.insert(0, skill_path)
from validate_yaml_schema import validate
cfg = validate(f"/Workspace/Users/{current_user}/configs/your_table.yaml")
print(f"OK: {cfg.table_name}, {len(cfg.column_mappings)} columns, {len(cfg.vocabulary_lookups)} lookups")
```

The skill auto-detects whether it is installed at user-scope (the default mode for `install.sh`, which writes to `/Workspace/Users/<current_user>/.assistant/skills/...`) or workspace-scope (admin install at `/Workspace/.assistant/skills/...`) and resolves `scripts/` dynamically. The same pattern works for any user invoking the skill regardless of install mode. `current_user` is resolved at runtime via `spark.sql("SELECT current_user()")` so the agent never has to substitute a placeholder. The kernel persists across `executeCode` calls within a session, so `from validate_yaml_schema import validate` only pays the import cost once.

**CLI (always-safe fallback):**

```bash
# Substitute <current_user> with your workspace email (e.g., name@example.com).
# The same scripts/ directory is at /Workspace/.assistant/skills/... if the
# skill was installed workspace-scope by an admin instead of user-scope by
# install.sh.
python /Workspace/Users/<current_user>/.assistant/skills/omop-pipeline-builder/scripts/validate_yaml_schema.py \
  /Workspace/Users/<current_user>/configs/your_table.yaml
```

Exit code 0 = valid; exit code 1 = invalid (errors printed to stderr).

**If validation fails:** fix the YAML and re-validate. Repeat until it passes. Do not present to the user or produce any summary until 0 errors. Common fixes:
- `vocabulary_lookups` must use `resolution: source_to_concept_map` or `resolution: concept_table` — not custom strategies like `case_map`. If a code set is small (< 10 values), use a CASE expression in `column_mappings` instead of a vocabulary lookup.
- `expectations` items must be `{name: "stable_id", expr: "SQL boolean"}` objects — not plain strings. The `name` is required for SDP telemetry dashboards.
- `source_alias` is required on every vocabulary lookup — must match an alias from `sources`.
- `sources[].table` must use `{catalog}` and `{bronze_schema}` placeholders — never hardcode catalog names.

**Only after validation passes:** present the config. The following is an example completion format — adapt to the surface (notebook, IDE, chat):

```
Draft ready for review: {table_name} — schema validates, 0 Pydantic errors

  {n_columns} columns mapped | {n_lookups} vocabulary lookups | {n_expectations} expectations
  Resolution: {brief strategy summary, e.g. "1x concept_table_mapped (ICD10CM→SNOMED), 1x concept_table"}

The validator confirmed the YAML structure. It cannot confirm OMOP fidelity — the clinical and source-data choices need your eyes before this config joins the DAG.

What's next — pick one, or ask me anything:

  1. Review checklist (the OMOP fidelity choices to verify before deploying)
  2. Walk me through deploying (assumes review is done — explains deploy, pipeline run, post-build validation)
  3. Deploy commands only (assumes review is done — three commands, no explanation)
```

**How to respond to each option:**

**If user picks 1 (review checklist):** Walk through the OMOP fidelity choices the validator can't check. For each item, name what was generated and what the reviewer should confirm. Cover all that apply to this config:

- **Vocabulary resolution strategies** — for each `vocabulary_lookups` entry, state the strategy chosen and why. Examples: "DiagnosisCode uses `concept_table_mapped` because ICD-10 is non-standard in OMOP and needs the Maps-to crosswalk to SNOMED. Confirm your bronze ICD-10 codes don't include Z-codes that should land in `observation_period` or `observation` — `domain_id: Condition` will silently drop those." Or: "RaceCode uses `source_to_concept_map` because institution-specific race codes don't exist in OHDSI Athena. Confirm the seed CSV covers every distinct race value in your bronze — anything missing resolves to 0 silently."
- **CASE expression branches** — for any column with a CASE expression, list the branches generated and the `ELSE` fallback. Example: "GenderCode CASE assumes `'M' → 8507`, `'F' → 8532`, ELSE 0. Confirm your bronze actually uses 'M'/'F' and not 'Male'/'Female' — every unmatched value falls through to 0 silently."
- **Type concept choices** — name any `*_type_concept_id` literal and what it means. Example: "`condition_type_concept_id = 32817` ('EHR encounter record'). Confirm that's the right provenance — `38000245` ('EHR encounter diagnosis') is more specific if your source is encounter-derived diagnoses."
- **Join cardinality** — name the join type (LEFT/INNER) and what could go wrong. Example: "Joining `pat_enc_dx` LEFT to `pat_enc` to pull encounter dates. If a diagnosis row has no matching encounter, the date columns fall back to the diagnosis-row defaults — confirm that's what you want."
- **STCM coverage** — if any column maps via `source_to_concept_map`, name the seed CSV the reviewer needs to populate, and warn that distinct source codes not in the seed will resolve to `fallback: 0` silently. Recommend running `generate_source_mappings.py` against the actual bronze to surface unresolved codes.

End with: "When you've reviewed and accepted these, pick option 2 or 3 to deploy."

**If user picks 2 (walk through deploying):** Assume review is done. Give one step at a time with brief explanations. Start with: "Save this YAML to `configs/{table_name}.yaml` in your repo. Then run `CATALOG=your_catalog ./deploy.sh production` — this syncs the config to the UC Volume where the pipeline reads it." After the user confirms, give the pipeline run command. Then the post-build validation step (`validate_omop.py` against the materialized table). Link to `docs/omop-runbook.md` Section 7 for reference.

**If user picks 3 (deploy commands only):** Assume review is done. Emit three commands, no explanation:
```
CATALOG=your_catalog ./deploy.sh production
databricks bundle run omop_full_build -t production
# After pipeline completes: open src/99_validate_omop_output.py and Run All — every spec-covered table the pipeline materialized is validated automatically
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

### Step 7 — Run the pipeline and validate the materialized table

With the YAML validated (Step 6), trigger the pipeline so it materializes the OMOP table, then run the post-build validator against the result. Pipeline run and post-build validation are one flow here because they're tightly coupled — the validator only runs after the table materializes, and a validator failure sends you back to the pipeline run.

#### Run the pipeline (dual mechanism)

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

#### Offer validation prominently when the table is newly materialized

After the pipeline run completes, re-read bundle state and check `materialization_diff`. When the just-built table appears there (meaning it is newly present in `core_target` since the prior snapshot in this session), surface the validator offer prominently — its own paragraph, **bolded**, before any other follow-up:

> Person table is materialized at `<catalog>.<core_schema>.person`.
>
> **Want me to run the OMOP fidelity validator?** It checks five layers against the OMOP CDM v5.4 spec — schema conformance, primary-key uniqueness, concept FK integrity, domain correctness, completeness. Takes about 30 seconds for `person`-sized tables; several minutes for the larger fact tables. Reply "yes" and I'll run it and surface findings.
>
> If you decline, your table goes to your CI/CD pipeline unvalidated against the spec. That's your team's call. Most teams want validation findings before merging the config to `main`.

If the engineer accepts, run `scripts/validate_omop.py` and surface findings using the v1.4 review framing: "Validator found X issues. Here they are. Reviewer ratifies whether each is a real fidelity gap or acceptable variance for this build."

If the engineer declines, acknowledge the decline once and continue to Step 8 (DAG wiring). Do not nag.

**Trigger condition.** Whenever the skill calls `read_bundle_state` after the first call in a session, pass `previous_silver_tables` — the snapshot from the immediately prior call — so `materialization_diff` reflects what is newly materialized since the last view. **On the first call in a session, omit `previous_silver_tables` entirely** (let it default to `None`). `materialization_diff` is then `None`, and the prominence template does not fire — there is nothing to diff against yet. **Never pass `[]` as a stand-in for "no prior snapshot"**; an empty list is a real snapshot meaning "no silver tables existed before," which would make every currently-materialized table look newly materialized and would fire prominence on every table at once. When `materialization_diff` is non-empty and contains the just-generated target table, the prominence template fires for that table. Cross-session materializations — engineer ran the pipeline yesterday, opens a fresh chat today — do NOT fire prominence because the first call in the new session has no previous snapshot. The engineer can still request validation explicitly at any time; the prominence rule is purely about proactive offering on the materialization edge.

**Pipeline rerun behavior.** If the engineer reruns the same pipeline (for example to fix data and re-materialize), the table will appear in `materialization_diff` again on the next state read, and the prominence template fires again. Re-validation on rerun is intended — the data changed.

**Pipeline failure.** If the pipeline run fails, `materialization_diff` does NOT include the target table, so the prominence template does not fire. The engineer sees the failure surface from `scripts/run_pipeline.py` and remediates. Validation against a failed run is not offered.

The skill does **NOT**:

- Auto-run validation. The offer is the gate; the engineer's acceptance is the trigger.
- Block subsequent workflow on a declined validation. The skill ships the validator and the offer; enforcement is your team's CI policy. See [`references/recommended_ci_config.md`](references/recommended_ci_config.md) for the documented CI integration patterns (GitHub Actions, Azure DevOps Pipelines).
- Re-offer validation for the **same materialization** after the engineer has declined. (A pipeline rerun is a new materialization event and re-fires the offer — see "Pipeline rerun behavior" above. Decline is scoped to one materialization, not the whole session.)
- Generate compliance documentation about the decline. The conversation log is the record.

#### Validate via `scripts/validate_omop.py`

Once the pipeline run is `COMPLETED` and the OMOP table is materialized, validate it:

```bash
python scripts/validate_omop.py \
  --table {catalog}.core_omop.person \
  --ref-schema reference
```

`--catalog` and `--schema` are optional overrides; by default the FQN segments after `--table` are used.

The script reports pass/fail for five layers (schema, primary-key uniqueness, concept referential integrity, domain conformance where defined, completeness / null-rate). A non-zero exit code means at least one layer failed — fix the config or the upstream data, re-run the pipeline, and re-validate before proceeding to Step 8.

### Step 8 — Wire the table into the OMOP job DAG

Once the pipeline runs green, `validate_omop.py` passes 5/5 layers, and a
reviewer (data engineer + clinical informaticist or OMOP-experienced peer)
has accepted the materialized table, wire the table into the orchestrated
`omop_full_build` job by uncommenting its placeholder in `resources/jobs.yml`.
The 5/5 validator confirms structural well-formedness; reviewer acceptance
confirms OMOP fidelity — the clinical and source-data choices were right.

If you used Step 1 to scaffold, the placeholder is already there with correct
`depends_on` from the OMOP DAG. Find the table in the right Round section,
remove the leading `# ` from each line of its task block, and validate the
bundle:

```bash
databricks bundle validate -t production
```

Commit the change through the team's normal Git workflow. The CI/CD pipeline
handles deploy — see [`references/recommended_ci_config.md`](references/recommended_ci_config.md)
for working GitHub Actions and Azure DevOps Pipelines snippets that wire the
Pydantic schema validator and `databricks bundle validate` into your pipeline.

If you skipped Step 1 (the team has an existing repo without scaffolded
placeholders), add a new task block matching the shape:

```yaml
- task_key: <table_name>
  depends_on:
    - task_key: <predecessor_1>
    - task_key: <predecessor_2>
  pipeline_task:
    pipeline_id: ${resources.pipelines.omop_<table_name>.id}
    full_refresh: true
```

Mandatory rules (apply whether scaffolded or hand-authored):

- `full_refresh: true` on every OMOP task. OMOP rebuilds are batch snapshots; incremental refresh would silently double-count rows.
- List every upstream OMOP table the pipeline reads in `depends_on`, including transitive predecessors. Self-documenting beats minimal.
- The pipeline resource (`${resources.pipelines.omop_<table>.id}`) must exist in `resources/pipeline_generic.yml` or a sibling resource file. If it doesn't, add it before editing `jobs.yml`.

Validate after every edit. Don't deploy until validation is clean.

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

**OMOP table names this skill recognizes (clinical scope, 20 tables as of v2.0.5):**

`person`, `observation_period`, `visit_occurrence`, `visit_detail`, `condition_occurrence`, `procedure_occurrence`, `drug_exposure`, `device_exposure`, `measurement`, `observation`, `note`, `note_nlp`, `specimen`, `death`, `location`, `care_site`, `provider`, `condition_era`, `drug_era`, `dose_era`

**Concept domain names (and typical `*_concept_id` columns):**

`Gender`, `Race`, `Ethnicity`, `Visit`, `Type Concept`, `Condition`, `Procedure`, `Drug`, `Measurement`, `Observation`, `Unit`, `Route`, `Place of Service`, `Relationship` (non-exhaustive; see `references/vocabulary_domains.md`)

**Note:** OHDSI Athena uses `Type Concept` as the domain_id for provenance columns (`visit_type_concept_id`, `condition_type_concept_id`, etc.) — not `Visit Type` or `Condition Type`.

## Validation scope vs build scope

This skill makes a deliberate distinction between **validation scope** and **build scope** (architectural decision AD-001 from v2.0.4 closeout):

- **Validation scope: 20 tables.** Every OMOP CDM v5.4 clinical table the skill recognizes lives in [`references/omop_cdm_v54_spec.md`](references/omop_cdm_v54_spec.md) and is checked by `scripts/validate_omop.py` (and the in-project notebook `templates/project_scaffold/src/99_validate_omop_output.py`) against the spec's schema (Layer 1), PK uniqueness (Layer 2), concept FKs (Layer 3), domain conformance (Layer 4), and NOT NULL contracts (Layer 5).
- **Build scope: 14 tables.** The scaffolder produces an end-to-end build path (DAB jobs, SDP pipelines, YAML configs) for 14 of the 20 tables — the dimension and core fact tables most customers' ETLs build directly from EHR sources. The dependency ordering for these 14 tables lives in `scripts/_omop_dag.py` and [`references/omop_dag_dependencies.md`](references/omop_dag_dependencies.md).

The 6 tables in validation scope but not in build scope are:

`visit_detail`, `device_exposure`, `note`, `note_nlp`, `specimen`, `dose_era`

These are the **bring-your-own-ETL (BYO-ETL)** tables. Customers landing them in their target schema use whatever path fits their source data — Lakeflow Connect, a custom Spark job, an existing OMOP build the team already runs, or any other pipeline — and the skill's validator then checks the result against the spec the same way it checks the 14 build-scope tables. See [`references/omop_dag_dependencies.md`](references/omop_dag_dependencies.md) "Validation-only (BYO-ETL)" section for the per-table sourcing notes.

**Why this split.** The fidelity-vs-coverage tradeoff is real: customers with full EHR-source coverage of all 20 tables would prefer the skill build everything end-to-end; customers with partial source coverage or existing pipelines for some tables would prefer the skill only build what fits their gaps. AD-001 chose the second customer's contract — validate everything in the spec (so customer-built data is checked uniformly) but build only the 14-table core that fits a typical from-EHR-bronze pipeline. The spec covers 20 tables to make the validator-side coverage uniform; the build path stays at 14 because expanding it without a customer-driven need would impose a build pattern that doesn't match the partial-source-coverage case.

**Cross-references — spec correctness changes.** Recent skill releases have closed several OHDSI-fidelity findings against [`references/omop_cdm_v54_spec.md`](references/omop_cdm_v54_spec.md). One — `note.encoding_concept_id` — introduces `Metadata` as a Domain assignment for the first time in this spec; see [`references/spec_domain_decisions.md`](references/spec_domain_decisions.md) for the borderline-case decisions log that accompanies the spec edits. A customer-visible behavior change: `drug_exposure.drug_exposure_end_date` was previously marked nullable in the spec but is `Required: Yes` in OHDSI v5.4 (which means NOT NULL). The spec change makes the Nullable cell `N`, which lets Layer 5's NOT NULL check fire on this column. Customers whose source data leaves `drug_exposure_end_date` NULL on incomplete drug-exposure rows will see new validator findings after upgrading. OHDSI's narrative on this column recommends imputing missing end dates from `drug_exposure_start_date` plus `days_supply` or equivalent duration rather than leaving NULL; consult your organization's clinical conventions for the specific imputation approach. **For the canonical per-release behavior change list, see [`CHANGELOG.md`](CHANGELOG.md).**

## Not for

This skill does NOT handle:
- **OMOP CDM tables not in this skill's scope** (e.g., `episode`, `fact_relationship`) — see [`references/omop_dag_dependencies.md`](references/omop_dag_dependencies.md) "Out of scope (no validator coverage)" section for the full list of tables outside both validation and build scope.
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
- [`references/omop_cdm_v54_spec.md`](references/omop_cdm_v54_spec.md) — required columns, keys, FKs, concept domains (clinical scope)
- [`references/ehr_to_omop_mappings.md`](references/ehr_to_omop_mappings.md) — bronze → OMOP table and column mapping notes
- [`references/vocabulary_domains.md`](references/vocabulary_domains.md) — domain ↔ vocabulary patterns and join strategies
- [`references/omop_dag_dependencies.md`](references/omop_dag_dependencies.md) — Round 1–4 dependency chart for `resources/jobs.yml`
- [`references/recommended_ci_config.md`](references/recommended_ci_config.md) — GitHub Actions and Azure DevOps Pipelines snippets for wiring Pydantic schema validation + `databricks bundle validate` into CI
- [`templates/discovery.yaml`](templates/discovery.yaml) — file-shape reference for the optional `discovery.yaml` artifact (NOT a setup precondition)
- [`scripts/bundle_state.py`](scripts/bundle_state.py) — read-only project state introspection (config inventory, materialized-table probe, conflict classification); used by the agent runtime, also exposes a debug CLI
- [`scripts/_omop_dag.py`](scripts/_omop_dag.py) — structured OMOP CDM v5.4 DAG dependencies (Round 1–4); data module consumed by `bundle_state.py` for predecessor analysis and by Step 8 wiring
- [`scripts/config_writer.py`](scripts/config_writer.py) — atomic YAML config writer with optional mtime-based optimistic concurrency (`MtimeMismatchError`) and read-only Git status surfacing
- [`scripts/structural_changelog.py`](scripts/structural_changelog.py) — Pydantic-schema-aware field-level diff between two OMOP YAML configs (`FieldChange` records, no `deepdiff` dependency)
- [`scripts/generate_source_mappings.py`](scripts/generate_source_mappings.py) — distinct codes → CSV in OHDSI `source_to_concept_map` shape (input for either bootstrap-CSV or direct-SQL paths; see [Adding source_to_concept_map mappings](#adding-source_to_concept_map-mappings))
- [`scripts/validate_yaml_schema.py`](scripts/validate_yaml_schema.py) — standalone Pydantic config validator (CLI + `validate(path)`)
- [`scripts/validate_omop.py`](scripts/validate_omop.py) — five-layer UC table validation
- [`scripts/run_pipeline.py`](scripts/run_pipeline.py) — start and poll pipeline updates

OHDSI CDM 5.4 canonical reference: [https://ohdsi.github.io/CommonDataModel/cdm54.html](https://ohdsi.github.io/CommonDataModel/cdm54.html)
