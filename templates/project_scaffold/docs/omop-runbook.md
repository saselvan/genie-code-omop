# OMOP Transform Framework — Independent Runbook

Run the OMOP CDM v5.4 transform framework on your Azure Databricks workspace with real EHR source data. No screenshare required — follow end-to-end.

**This skill is a solution accelerator.** After scaffolding, you own the generated project and the production data it produces. There is no skill-side support relationship; the skill drafts artifacts that you review and apply (architectural decision AD-002 — see [`AD-002-skill-drafts-customer-reviews.md`](../../.assistant/skills/omop-pipeline-builder/references/AD-002-skill-drafts-customer-reviews.md)). When you need help:

- **Skill bugs or feature requests** — file an issue at https://github.com/saselvan/genie-code-omop/issues. Include your skill version (`.omop-skill-version` in your project root) and the relevant validator / generator output.
- **OMOP CDM questions** — the OHDSI community at https://forums.ohdsi.org is the right surface for vocabulary semantics, domain conformance, and ETL convention questions.
- **Databricks platform questions** — follow your organization's normal Databricks support channels (workspace admin, Databricks support contract, or solution architect).

**Audience:** Data engineers and platform engineers with Databricks CLI and workspace access. Assumes Databricks familiarity but not OMOP expertise.

**Key terms:** see [Appendix D Glossary](#appendix-d-glossary) for definitions of OMOP CDM, OHDSI, SDP, DAB, Genie Code, Pydantic, silver/core, and other terms used throughout.

**Prerequisite:** Your EHR source tables must already be accessible in a Unity Catalog schema. If they're not, work with your data platform team to land them first — that's a separate workstream.

---

## Why This Framework Exists (read this first)

You could write SQL directly in an SDP pipeline — `@dp.table` with a big SELECT statement. That works for one table. It falls apart at scale because of **vocabulary mapping**.

OMOP doesn't just want your EHR source columns renamed. It wants every clinical code resolved to a **standard OHDSI concept**. That means:

- **70,000+ ICD-10 codes** need to crosswalk to SNOMED via the `concept_relationship` table's "Maps to" chain. One ICD-10 code can map to multiple SNOMED concepts — OHDSI says you create a row for each. You can't write that as a CASE statement.
- **Local codes** (your race codes, ethnicity codes, encounter types) don't exist in OHDSI at all — they need a `source_to_concept_map` seed table that you maintain.
- **Deprecated concepts** get silently loaded if you don't filter `invalid_reason IS NULL`. Your data passes validation today and fails after a vocabulary refresh.
- **Every OMOP table has the same pattern** — sources, joins, vocab lookups, column mappings, expectations. Writing that pattern 12 times in SQL is 12 opportunities to get the vocab resolution wrong.

The framework solves this by separating **what** (YAML config) from **how** (pipeline code):

| Layer | What it does | Why SDP alone doesn't do it |
|---|---|---|
| **YAML configs** | Declare sources, joins, vocab lookups, column mappings, expectations per OMOP table | SDP has no concept of metadata-driven transforms — it runs Python you write |
| **config_loader.py** (Pydantic) | Validates YAML structure before the pipeline runs — catches typos, wrong resolution strategies, missing fields | SDP has no config validation — bad code runs and fails at runtime |
| **vocab_resolver.py** | Joins to `concept`, `source_to_concept_map`, and `concept_relationship` with invalid-concept filtering, standard-concept enforcement, domain-based one-to-many routing | SDP doesn't know about OHDSI vocabulary — you'd write these joins by hand per table |
| **column_mapper.py** | Builds SELECT expressions from YAML — applies CASTs, CASE statements, NULLs with correct OMOP types | Convenience — could be done in SQL, but YAML is diffable, reviewable, and AI-authorable |
| **5-layer validator** | Checks schema, PK uniqueness, concept RI, domain conformance, completeness against OMOP CDM v5.4 spec | SDP expectations check row-level quality — they don't check OMOP-level conformance |
| **Genie Code skill** | AI generates YAML configs from bronze table metadata, validates against Pydantic schema, checks vocab coverage | You could write configs by hand — but for 12+ tables, AI-assisted authoring saves weeks |

**The bottom line:** SDP gives you serverless compute, expectations, and a pipeline graph. The framework gives you OMOP-correct vocabulary resolution, schema validation, and AI-assisted config authoring. Together, they turn "migrate to OMOP" from a 6-month SQL project into a repeatable YAML-driven workflow.

---

## Quick Start

If you want to get Person + Visit Occurrence running end-to-end, here's the sequence. Each step links to the detailed section.

1. **Set up workspace** — CLI profile, skills, bundle variables, schemas ([Section 1](#1-pre-requisites--genie-code-agent-mode-setup))
2. **Discover your EHR source schema** — audit tables, columns, code values ([Section 2](#2-clinical-schema-discovery))
3. **Load OHDSI vocabulary** — download from Athena, upload, run loader, verify row counts ([Section 6.1](#61-loading-ohdsi-vocabulary))
4. **Seed source_to_concept_map** — map your local race, ethnicity, gender, visit type codes ([Section 6.2](#62-seeding-source_to_concept_map-with-your-codes))
5. **Generate/update Person config** — regenerate with Genie or edit column names manually ([Section 4](#4-reference-file-modifications))
6. **Deploy + run Person** — `./deploy.sh production`, run pipeline, validate 5 layers ([Section 7.1](#71-example-1-person-round-1--full-walkthrough))
7. **Repeat for Visit Occurrence** — seed encounter type codes, deploy, run, validate ([Section 7.2](#72-example-2-visit-occurrence-round-2--condensed))
8. **Add more tables** — follow the pattern: generate → validate → deploy → run → validate → wire DAG ([Section 7.4](#74-adding-more-tables--the-pattern))

**Key repo references:**
- [`SKILL.md`](.assistant/skills/omop-pipeline-builder/SKILL.md) — 8-step workflow for generating YAML configs via Genie Code
- [`configs/_schema.yaml`](configs/_schema.yaml) — Pydantic-enforced YAML structure
- [`references/omop_cdm_v54_spec.md`](.assistant/skills/omop-pipeline-builder/references/omop_cdm_v54_spec.md) — OMOP CDM v5.4 column specs
- [`references/ehr_to_omop_mappings.md`](.assistant/skills/omop-pipeline-builder/references/ehr_to_omop_mappings.md) — EHR source → OMOP table/column mapping notes
- [`references/omop_dag_dependencies.md`](.assistant/skills/omop-pipeline-builder/references/omop_dag_dependencies.md) — Round 1-4 dependency chart

---

## 1. Pre-requisites & Genie Code Agent Mode Setup

### 1.1 Workspace Requirements

| Requirement | How to verify |
|---|---|
| Unity Catalog enabled | Workspace admin → Settings → Unity Catalog |
| **Partner-powered AI features** enabled | Account admin → Account Settings → AI/BI → Partner-powered AI features (must be ON at both account AND workspace level). Without this, Genie Code Agent mode is unavailable. |
| **Genie Code Agent mode** available | Open any notebook → Genie Code sidebar (right panel) → toggle to **Agent** mode. If you only see Chat mode, partner-powered AI is not enabled — contact your workspace admin. |
| **Geo availability** | Genie Code is a Designated Service. If unavailable in your region, admin may need to disable "Enforce data processing within workspace Geography for AI features." See [Databricks Geos](https://learn.microsoft.com/en-us/azure/databricks/resources/databricks-geos). |
| SQL Warehouse running (serverless preferred) | SQL Warehouses page → at least one active |
| Databricks CLI >= 0.230 | `databricks --version` |
| Python 3.11+ | `python --version` |
| Python packages | `pip install databricks-sdk pyyaml pydantic` then verify: `python -c "import databricks.sdk; import yaml; import pydantic"` |
| `jq` (JSON processor, used in CLI commands) | `jq --version` — install with `brew install jq` (macOS) or `apt install jq` (Linux) |
| Git access to the omop_etl repo | `git clone <your-fork-url>` |
| OHDSI vocabulary zip from https://athena.ohdsi.org | Free registration required; select all vocabularies. After requesting, **Athena emails you a download link** — this can take minutes to hours, not instant. ~1.2 GB zip. |

**Cost:** Genie Code Agent mode is included at no additional cost for all Databricks customers. You pay only for compute used to run generated code (SQL warehouse, notebook clusters). No separate AI/ML SKU required.

### 1.2 CLI Profile

Set up a CLI profile pointing to your Azure workspace. Use whatever profile name fits your convention:

```bash
databricks configure --profile <your-profile-name>
# When prompted: paste your Azure workspace URL + PAT (or --auth-type=oauth)
databricks current-user me --profile <your-profile-name>   # confirms auth works
```

Then export it so all commands in this runbook (and `deploy.sh`) pick it up:

```bash
export DATABRICKS_CONFIG_PROFILE=<your-profile-name>
```

All commands below use `--profile $DATABRICKS_CONFIG_PROFILE`. If you use the `DEFAULT` profile, skip the export — CLI uses it automatically.

### 1.3 Install Genie Code Skills

```bash
# 1. Install ai-dev-kit skills (SDP, DABs, Jobs skills that omop-pipeline-builder chains against)
git clone https://github.com/databricks-solutions/ai-dev-kit.git
cd ai-dev-kit
./databricks-skills/install_skills.sh --install-to-genie --profile $DATABRICKS_CONFIG_PROFILE

# 2. Verify (should list 36+ skills)
databricks workspace list "/Workspace/Users/$(databricks current-user me --profile $DATABRICKS_CONFIG_PROFILE -o json | jq -r .userName)/.assistant/skills/" --profile $DATABRICKS_CONFIG_PROFILE

# 3. Deploy the omop-pipeline-builder skill
cd /path/to/omop_etl
databricks workspace import-dir .assistant/skills/omop-pipeline-builder \
  "/Workspace/Users/$(databricks current-user me --profile $DATABRICKS_CONFIG_PROFILE -o json | jq -r .userName)/.assistant/skills/omop-pipeline-builder" \
  --overwrite --profile $DATABRICKS_CONFIG_PROFILE
```

**Verify Agent Mode:** Open any notebook in your workspace → look for the Genie Code toggle in the right sidebar → switch to **Agent** mode. If you don't see Agent mode, contact your workspace admin — it requires workspace-level enablement.

### 1.4 Configure Bundle Variables

Open `databricks.yml`, find the `production` target:

```yaml
targets:
  production:
    workspace:
      host: https://adb-XXXXX.NN.azuredatabricks.net  # ← your Azure workspace URL
    variables:
      catalog: CHANGEME         # ← your Unity Catalog name
      bronze_schema: clinical     # ← schema where EHR source tables live
      core_schema: core_omop      # keep as-is
      ref_schema: reference       # keep as-is
      notification_email: data-team@example.com  # ← your team DL
```

| Variable | Description | Where to find your value |
|---|---|---|
| `catalog` | Unity Catalog for OMOP data | Catalog Explorer → your catalog name |
| `bronze_schema` | Schema containing EHR source tables | `SHOW SCHEMAS IN <catalog>` |
| `core_schema` | Target schema for OMOP tables | Default: `core_omop` |
| `ref_schema` | Schema for OHDSI vocabulary | Default: `reference` |
| `notification_email` | Alert recipient for pipeline failures | Your team distribution list |

Validate:
```bash
databricks bundle validate -t production --profile $DATABRICKS_CONFIG_PROFILE
# Expected: "Validation OK!"
```

### 1.5 Create Schemas and Volumes

```sql
CREATE SCHEMA IF NOT EXISTS {catalog}.core_omop;
CREATE SCHEMA IF NOT EXISTS {catalog}.reference;
CREATE VOLUME IF NOT EXISTS {catalog}.reference.vocabulary_files;
CREATE VOLUME IF NOT EXISTS {catalog}.core_omop.configs;
```

Replace `{catalog}` with your actual catalog name. `bronze_schema` (where EHR source tables live) should already exist.

---

## 2. EHR Schema Discovery

The framework was built against synthetic EHR source tables with simplified column names. Your real EHR source will differ. This section helps you audit the differences before writing any configs.

### 2.1 Synthetic Schema Reference

This is what the demo used. Your columns WILL be different.

| Table | Columns |
|---|---|
| `patient` | PatientID, BirthDate, GenderCode, RaceCode, EthnicityCode, ZipCode, DeathDate |
| `patient_identifier` | PatientID, MRN, IdentityType |
| `encounter` | EncounterID, PatientID, AdmitDateTime, DischargeDateTime, EncounterType, ProviderID |
| `encounter_diagnosis` | EncounterID, DiagnosisCode, DiagnosisType, DiagnosisDateTime |
| `procedure_dim` | ProcedureID, ProcedureCode, ProcedureName |
| `procedure_order` | OrderID, EncounterID, PatientID, ProcedureCode, OrderDateTime |
| `medication_order` | OrderID, EncounterID, PatientID, MedicationNDC, OrderDateTime, Quantity |
| `clinical_measurement` | MeasID, EncounterID, PatientID, MeasName, MeasValue, MeasDateTime |

### 2.2 Discovery Queries

Run these in the **SQL Editor** (left sidebar → SQL Editor) connected to your SQL Warehouse. Replace `{catalog}` and `{bronze_schema}` with your actual values throughout.

**List all tables:**
```sql
SELECT table_name, table_type
FROM {catalog}.information_schema.tables
WHERE table_schema = '{bronze_schema}'
ORDER BY table_name;
```

**Describe each table:**
```sql
DESCRIBE TABLE {catalog}.{bronze_schema}.patient;
DESCRIBE TABLE {catalog}.{bronze_schema}.patient_identifier;
DESCRIBE TABLE {catalog}.{bronze_schema}.encounter;
DESCRIBE TABLE {catalog}.{bronze_schema}.encounter_diagnosis;
-- repeat for any other EHR source tables
```

**Full column inventory (single query):**
```sql
SELECT c.table_name, c.column_name, c.data_type, c.is_nullable
FROM {catalog}.information_schema.columns c
WHERE c.table_schema = '{bronze_schema}'
  AND c.table_name IN ('patient', 'patient_identifier', 'encounter', 'encounter_diagnosis')
ORDER BY c.table_name, c.ordinal_position;
```

**Row counts:**
```sql
SELECT 'patient' AS tbl, COUNT(*) AS cnt FROM {catalog}.{bronze_schema}.patient
UNION ALL SELECT 'patient_identifier', COUNT(*) FROM {catalog}.{bronze_schema}.patient_identifier
UNION ALL SELECT 'encounter', COUNT(*) FROM {catalog}.{bronze_schema}.encounter
UNION ALL SELECT 'encounter_diagnosis', COUNT(*) FROM {catalog}.{bronze_schema}.encounter_diagnosis;
```

**Distinct code audit (critical for vocabulary mapping):**
```sql
-- Gender codes
SELECT DISTINCT GenderCode, COUNT(*) AS cnt
FROM {catalog}.{bronze_schema}.patient GROUP BY GenderCode ORDER BY cnt DESC;

-- Race codes
SELECT DISTINCT RaceCode, COUNT(*) AS cnt
FROM {catalog}.{bronze_schema}.patient GROUP BY RaceCode ORDER BY cnt DESC;

-- Ethnicity codes
SELECT DISTINCT EthnicityCode, COUNT(*) AS cnt
FROM {catalog}.{bronze_schema}.patient GROUP BY EthnicityCode ORDER BY cnt DESC;

-- Encounter types
SELECT DISTINCT EncounterType, COUNT(*) AS cnt
FROM {catalog}.{bronze_schema}.encounter GROUP BY EncounterType ORDER BY cnt DESC;

-- Diagnosis types
SELECT DISTINCT DiagnosisType, COUNT(*) AS cnt
FROM {catalog}.{bronze_schema}.encounter_diagnosis GROUP BY DiagnosisType ORDER BY cnt DESC;

-- Sample ICD-10 codes (top 20)
SELECT DiagnosisCode, COUNT(*) AS cnt
FROM {catalog}.{bronze_schema}.encounter_diagnosis GROUP BY DiagnosisCode ORDER BY cnt DESC LIMIT 20;
```

**Note:** Adjust column names if yours differ from the synthetic schema above. That's the whole point of this discovery step.

### 2.3 Schema Discovery Worksheet

Fill this in for each table. This becomes your mapping reference for Section 4.

**patient:**

| Synthetic Column | Your Column | Data Type | Notes |
|---|---|---|---|
| PatientID | | | |
| BirthDate | | | |
| GenderCode | | | |
| RaceCode | | | |
| EthnicityCode | | | |
| ZipCode | | | |
| DeathDate | | | |

Copy this template for `patient_identifier`, `encounter`, `encounter_diagnosis`, and any other tables.

### 2.4 Genie Code Discovery (optional)

Open a notebook → Genie Code Agent mode → type:
```
Describe the bronze table {catalog}.{bronze_schema}.patient and list all columns with their data types
```

---

## 3. What Will Break (Synthetic vs Real EHR source)

Expect these differences. Every row has a concrete fix.

| Category | What's Different | Impact | How to Fix |
|---|---|---|---|
| **Column names** | Synthetic: `PatientID`, `BirthDate`. Real: may be `PATIENT_ID`, `DATE_OF_BIRTH`, etc. | Every `column_mappings[].expr`, `joins[].condition`, `vocabulary_lookups[].source_column` breaks | Update YAML configs with real column names from your DESCRIBE output. Or regenerate via Genie. |
| **Data types** | Synthetic: IDs inferred as INT from CSV. Real: may be BIGINT, STRING, etc. | CAST expressions may fail or produce unexpected results | Review DESCRIBE data types; adjust CASTs in `column_mappings` |
| **Table names** | Synthetic: lowercase (`patient`, `encounter`). Real: may be uppercase or different names entirely | `sources[].table` references fail | Update `sources[].table` in each config. Or create views in your bronze schema. |
| **Race code values** | Synthetic: numeric (`1`=White, `2`=Black). Real: likely text descriptions or EHR codes | `source_to_concept_map` lookups all return 0 | Run distinct code audit, build new seed CSV (Section 6) |
| **Ethnicity code values** | Synthetic: `H`, `NH`, `U`. Real: different codes | Same as above | Same as above |
| **Gender code values** | Synthetic: `M`, `F`, `U`. Real: may differ | CASE expression in `column_mappings` returns 0 | Update CASE values to match your codes |
| **Encounter types** | Synthetic: `Inpatient`, `Outpatient`, `Emergency`, `Observation`. Real: may be `IP`, `OP`, `ED`, etc. | `visit_concept_id` resolves to 0 | Add your encounter type codes to `source_to_concept_map` |
| **Diagnosis types** | Synthetic: `Primary`, `Secondary`, `Admitting`. Real: may be codes or different strings | `condition_type_concept_id` CASE returns 0 | Update CASE expression in condition_occurrence config |
| **Additional columns** | Real EHR source has 50-200 columns per table (vs 5-7 synthetic) | No breakage — opportunity for richer mapping | Optional: add to configs as needed |
| **Missing tables** | Your EHR source may not have exact equivalents for all 8 synthetic bronze source tables | Configs for those OMOP tables need different source tables | Identify equivalent tables via schema discovery |

---

## 4. Reference File Modifications

### Files that MUST change

| File | What to change | Why |
|---|---|---|
| `configs/<table>.yaml` (each table the agent generates) | Column names in `sources[].table`, `joins[].condition`, `vocabulary_lookups[].source_column`, `column_mappings[].expr` | Your column names differ from synthetic |
| `seed_data/source_to_concept_map_custom.csv` | Source code values (race, ethnicity, gender, visit type codes) | Your codes differ from synthetic |
| `databricks.yml` | `production` target variables + workspace host | Points to your workspace |

### Files that MIGHT change

| File | When to change |
|---|---|
| `references/ehr_to_omop_mappings.md` | If you have EHR source tables or columns not covered |

### Files that should NOT change

`src/02_omop_transform_pipeline.py`, `src/config_loader.py`, `src/vocab_resolver.py`, `src/column_mapper.py`, `src/01_load_vocabulary.py`, `configs/_schema.yaml`, `resources/pipeline_generic.yml`, `resources/jobs.yml`

These are framework internals — shared across all tables. Don't touch them.

### Regenerating a config with Genie

Instead of manually editing column names, regenerate from your real schema:
```
Generate an OMOP config for person from {catalog}.{bronze_schema}.patient
```
The skill samples your actual table, reads the OMOP spec, and emits a config with your real column names. Validate with Pydantic before using (the skill does this automatically).

---

## 5. Vocabulary Mapping Guide

### 5.1 OMOP Vocabulary in 60 Seconds

- The `reference.concept` table has ~10M rows covering ICD-10, SNOMED, LOINC, CPT4, NDC, RxNorm, and more
- Every `*_concept_id` column in an OMOP table must point to a valid `concept.concept_id` — or 0 for "unknown"
- Some vocabularies are **standard** in OMOP (SNOMED for conditions, RxNorm for drugs, LOINC for measurements)
- Some are **non-standard** (ICD-10, CPT4, NDC) — they exist in the concept table but need to be mapped to their standard equivalents via a "Maps to" relationship
- `source_to_concept_map` is for **local/institution-specific codes** that don't exist in OHDSI at all (your race codes, ethnicity codes, encounter type strings)
- **All three resolution strategies filter out deprecated/invalid concepts automatically.** If a concept's `invalid_reason` is set (deprecated or upgraded), the resolver skips it and falls back to 0. If you seed a `source_to_concept_map` row pointing to a deprecated concept_id, it's silently dropped. This protects you during vocabulary refreshes — when OHDSI retires a concept, your pipeline adapts without config changes.

### 5.2 Which Resolution Strategy?

```
Is the code in a standard vocabulary (ICD10CM, SNOMED, LOINC, CPT4, NDC, RxNorm)?
│
├── YES: Is that vocabulary itself the "standard" for its domain?
│   ├── YES (e.g., LOINC for Measurement) → resolution: concept_table
│   └── NO  (e.g., ICD10CM needs SNOMED)  → resolution: concept_table_mapped [see status note]
│
└── NO: Is it a local/institution-specific code?
    ├── Small set (<10 values) → inline CASE in column_mappings
    └── Larger set → resolution: source_to_concept_map (seed your own mapping table)
```

### 5.3 Resolution Strategy: `source_to_concept_map` (proven)

For local codes: race, ethnicity, gender, visit type, any institution-specific code.

```yaml
vocabulary_lookups:
  - source_alias: pat
    source_column: RaceCode
    target_column: race_concept_id
    resolution: source_to_concept_map
    source_vocabulary_id: Race
    fallback: 0
```

Requires seeding `reference.source_to_concept_map` with your codes (see Section 6).

### 5.4 Resolution Strategy: `concept_table` (proven)

For standard vocabularies where the concept_code IS the standard concept (e.g., LOINC for measurements).

```yaml
vocabulary_lookups:
  - source_alias: dx
    source_column: DiagnosisCode
    target_column: condition_source_concept_id
    resolution: concept_table
    vocabulary_id: ICD10CM
    domain_id: Condition
    fallback: 0
```

This returns the ICD-10 concept directly. Use for `*_source_concept_id` columns where you want the source vocabulary's concept, not the standard mapping.

### 5.5 Resolution Strategy: `concept_table_mapped` (ICD-10 → SNOMED crosswalk)

For non-standard vocabularies that need crosswalk to standard concepts: ICD-10-CM → SNOMED, CPT4 → SNOMED, NDC → RxNorm, ICD-10-PCS → SNOMED.

The resolver: (1) finds the source concept by `concept_code` + `vocabulary_id`, (2) traverses the "Maps to" relationship in `concept_relationship`, (3) returns the standard target `concept_id`. Filters out deprecated/invalid concepts on both sides. Only returns standard concepts (`standard_concept = 'S'`) by default.

```yaml
vocabulary_lookups:
  # Standard concept (ICD-10 → SNOMED via Maps to)
  - source_alias: dx
    source_column: DiagnosisCode
    target_column: condition_concept_id
    resolution: concept_table_mapped
    vocabulary_id: ICD10CM
    domain_id: Condition
    fallback: 0

  # Source concept (ICD-10 concept directly, for traceability)
  - source_alias: dx
    source_column: DiagnosisCode
    target_column: condition_source_concept_id
    resolution: concept_table
    vocabulary_id: ICD10CM
    domain_id: Condition
    standard_only: false
    fallback: 0
```

**`domain_id` is required** — it filters one-to-many mappings to the correct OMOP table. An ICD-10 code like T44.8X2D maps to 4 SNOMED concepts across 2 domains (3 Condition + 1 Observation). Setting `domain_id: Condition` keeps only the 3 Condition targets. The Observation target is picked up when you build the `observation` table with `domain_id: Observation`.

**One-to-many within the same domain:** Per OHDSI convention, one source code mapping to multiple standard concepts in the same domain produces **multiple output rows**. For example, "viral hepatitis with hepatic coma" (one ICD code) produces 2 `condition_occurrence` rows — one for viral hepatitis, one for hepatic coma. This is correct OMOP behavior, not a bug.

**Surrogate key must account for fan-out.** Include the resolved `condition_concept_id` in your key expression:

```yaml
# WRONG — duplicate keys when one source row fans out:
- target: condition_occurrence_id
  expr: "ROW_NUMBER() OVER (ORDER BY dx.EncounterID, dx.DiagnosisCode)"

# RIGHT — unique across fan-out rows:
- target: condition_occurrence_id
  expr: "ROW_NUMBER() OVER (ORDER BY dx.EncounterID, dx.DiagnosisCode, condition_concept_id)"
```

**Optional parameters:**

| Parameter | Default | When to override |
|---|---|---|
| `relationship_id` | `"Maps to"` | Set to `"Maps to unit"` for measurement `unit_concept_id`, or `"Maps to value"` for `value_as_concept_id` |
| `standard_only` | `true` | Set to `false` for `*_source_concept_id` columns (non-standard concepts are expected there) |

**Verify the crosswalk works for your codes:**

```sql
-- Test a sample ICD-10 → SNOMED mapping
SELECT c.concept_code AS source_code,
       c2.concept_id AS standard_concept_id,
       c2.concept_name AS standard_name,
       c2.domain_id
FROM {catalog}.reference.concept c
JOIN {catalog}.reference.concept_relationship cr
  ON c.concept_id = cr.concept_id_1 AND cr.relationship_id = 'Maps to'
JOIN {catalog}.reference.concept c2
  ON cr.concept_id_2 = c2.concept_id
WHERE c.vocabulary_id = 'ICD10CM' AND c.concept_code = 'E11.9'
  AND c2.standard_concept = 'S' AND c2.invalid_reason IS NULL;
-- Expected: SNOMED concept 201826 (Type 2 diabetes mellitus)
```

**Reference behavior:** With a fully-loaded OHDSI vocabulary (~100K ICD-10 concepts, ~7.3M Maps-to relationships), single-code lookups like E11.9, I10, J45.909, M54.5 resolve 1:1 to a single SNOMED concept. Codes that map to multiple domains (e.g., T44.8X2D → 3 Condition + 1 Observation) are correctly filtered by `domain_id` to land in the right OMOP table.

### 5.6 How to Check if Your Codes Exist in OHDSI

Before choosing a resolution strategy, check if your codes are already loaded:

```sql
-- Check if sample ICD-10 codes exist
SELECT concept_code, concept_id, concept_name, standard_concept
FROM {catalog}.reference.concept
WHERE vocabulary_id = 'ICD10CM'
  AND concept_code IN ('E11.9', 'I10', 'J45.909');
```

If rows return, use `concept_table` (or `concept_table_mapped` once validated). If no rows, either the vocabulary isn't loaded or the code format differs — check for dots, dashes, leading characters.

### 5.7 Semantic Gotchas

- **`visit_type_concept_id` is provenance, NOT visit kind.** Use concept 32817 ("EHR encounter record") for all EHR-sourced encounters. Clinical visit kind (Inpatient, Outpatient, etc.) goes in `visit_concept_id`.
- **`condition_type_concept_id` uses the "Type Concept" domain**, not the "Condition" domain. Same for `procedure_type_concept_id`, `drug_type_concept_id`, etc.
- **Always preserve `*_source_value`** alongside mapped `*_concept_id` for traceability.
- **CPT4 is license-gated.** The `CONCEPT_CPT4.csv` may be empty until you complete the separate CPT4 license step in Athena. Expect `procedure_concept_id` to resolve to 0 for CPT codes until this is done.

---

## 6. Reference Table Population

### 6.1 Loading OHDSI Vocabulary

**Step 1: Download from Athena**
- Go to https://athena.ohdsi.org, register (free), select all vocabularies, click Download
- The zip is ~1.2 GB; download may take 10-15 min

**Step 2: Extract and clean**
```bash
mkdir -p /tmp/ohdsi_vocab
unzip -o "vocabulary_download_v5_*.zip" -d /tmp/ohdsi_vocab
# Remove non-CSV artifacts
mkdir -p /tmp/ohdsi_vocab_extras
mv /tmp/ohdsi_vocab/{cpt4.jar,cpt.sh,readme.txt,cpt.bat} /tmp/ohdsi_vocab_extras/ 2>/dev/null || true
```

**Step 3: Upload to UC Volume**
```bash
databricks fs cp -r /tmp/ohdsi_vocab/ \
  "/Volumes/{catalog}/reference/vocabulary_files/" \
  --overwrite --profile $DATABRICKS_CONFIG_PROFILE
```

**Step 4: Run the vocabulary load job**

This first run loads OHDSI vocabulary only (concept, concept_relationship, etc.). Your custom `source_to_concept_map` codes are seeded separately in Section 6.2 — you'll re-run this job after uploading your seed CSV.

```bash
databricks bundle run setup_vocabulary -t production --profile $DATABRICKS_CONFIG_PROFILE
```

**Step 5: Verify**
```sql
SELECT COUNT(*) FROM {catalog}.reference.concept;           -- expect ~6-10M
SELECT COUNT(*) FROM {catalog}.reference.concept_relationship;  -- expect ~55M
SELECT COUNT(*) FROM {catalog}.reference.source_to_concept_map; -- expect 0 (seeded next)
```

**If the load-bearing tables are empty (`concept`, `concept_relationship`):** As of v2.0.7 this fails the job loudly — `01_load_vocabulary.py` includes a post-load assertion that raises `RuntimeError` when either of these tables is zero rows after the COPY INTO loop completes (the COPY INTO loop itself catches per-file failures so the CPT4-license-gated case stays a soft skip; the post-load assertion catches the catastrophic-failure case where every COPY INTO failed). The error message names the three common causes inline: (1) CSV files not present on the Volume — verify with `databricks fs ls "/Volumes/{catalog}/reference/vocabulary_files/"`; (2) `COPY INTO` hit a type mismatch — re-run `src/01_load_vocabulary.py` as a notebook and check the per-cell `SKIP` messages; (3) wrong volume path — verify the `vocab_volume_path` widget value. Tables not asserted (legitimately empty in some configurations): `source_to_concept_map` (empty until you seed in Section 6.2), `concept_cpt4` (CPT4-license-gated; soft-skipped), and `concept_synonym` / `concept_ancestor` / `drug_strength` (may be omitted from minimal Athena bundles).

**Customer impact for v2.0.7 upgrades:** If your v2.0.6 vocabulary load was silently zero-row and your downstream pipelines were resolving every `*_concept_id` to 0, your v2.0.7 vocabulary load will now exit non-zero with the diagnostic above instead of exiting SUCCESS. Re-run row-count verification on your existing `{catalog}.reference.concept` table before scheduling the v2.0.7 job — if it's already zero, the v2.0.6 silent failure is the upstream cause and the v2.0.7 assertion is correctly surfacing it.

### 6.2 Seeding `source_to_concept_map` with Your Codes

**Step 1: Run distinct code audit** (queries from Section 2.2 — distinct gender, race, ethnicity, encounter type codes)

**Step 2: Find target concept_ids**

For each code set, look up the correct OHDSI concept_id:

```sql
-- Race concepts
SELECT concept_id, concept_name FROM {catalog}.reference.concept
WHERE vocabulary_id = 'Race' AND domain_id = 'Race';
-- Common: 8527 (White), 8516 (Black/AA), 8515 (Asian), 8557 (Pacific Islander), 8657 (AI/AN)

-- Ethnicity concepts
SELECT concept_id, concept_name FROM {catalog}.reference.concept
WHERE vocabulary_id = 'Ethnicity' AND domain_id = 'Ethnicity';
-- Common: 38003563 (Hispanic), 38003564 (Not Hispanic)

-- Gender concepts
SELECT concept_id, concept_name FROM {catalog}.reference.concept
WHERE vocabulary_id = 'Gender' AND domain_id = 'Gender';
-- Common: 8507 (Male), 8532 (Female)

-- Visit concepts
SELECT concept_id, concept_name FROM {catalog}.reference.concept
WHERE vocabulary_id = 'Visit' AND domain_id = 'Visit';
-- Common: 9201 (Inpatient), 9202 (Outpatient), 9203 (ER), 581385 (Observation)
```

**Step 3: Build your seed CSV**

Format must match exactly:
```
source_code,source_concept_id,source_vocabulary_id,source_code_description,target_concept_id,target_vocabulary_id,valid_start_date,valid_end_date,invalid_reason
```

Example rows (replace with YOUR codes):
```
White,0,Race,White,8527,Race,19700101,20991231,
Black,0,Race,Black or African American,8516,Race,19700101,20991231,
Hispanic,0,Ethnicity,Hispanic or Latino,38003563,Ethnicity,19700101,20991231,
IP,0,Visit,Inpatient,9201,Visit,19700101,20991231,
OP,0,Visit,Outpatient,9202,Visit,19700101,20991231,
```

Save as `seed_data/source_to_concept_map_custom.csv`.

**Step 4: Automated resolution (optional)**

The `generate_source_mappings.py` script can scan your bronze table and attempt to resolve codes:
```bash
python .assistant/skills/omop-pipeline-builder/scripts/generate_source_mappings.py \
  --source-vocabulary-id Race \
  --source-table {catalog}.{bronze_schema}.patient \
  --source-code-column RaceCode \
  --catalog {catalog} --ref-schema reference \
  --profile $DATABRICKS_CONFIG_PROFILE
```

**Step 5: Upload and reload**
```bash
databricks fs cp seed_data/source_to_concept_map_custom.csv \
  "/Volumes/{catalog}/reference/vocabulary_files/source_to_concept_map_custom.csv" \
  --overwrite --profile $DATABRICKS_CONFIG_PROFILE
```
Then re-run the vocabulary setup job (it uses MERGE — safe to re-run, no duplicates).

**Step 6: Verify**
```sql
SELECT * FROM {catalog}.reference.source_to_concept_map
WHERE source_vocabulary_id = 'Race';
```

**Iterating:** Add new code sets by appending rows to the CSV and re-running. Monitor coverage via `warn` expectations in pipeline output (e.g., `known_race_concept: race_concept_id != 0` tells you what percentage resolved).

### 6.3 Drafting `source_to_concept_map` at scale (multi-vocabulary, config-driven)

Section 6.2 Step 4 covers the single-vocabulary bootstrap path via `generate_source_mappings.py`. Once you have multiple per-table YAML configs landed (typically by the time you're past Person + Visit Occurrence and onto Condition / Drug / Procedure / Measurement / Observation), the per-vocabulary one-off invocations become repetitive. The `generate_source_concept_map.py` generator (v2.0.7+) drafts STCM rows for **all source vocabularies declared across multiple per-table configs in one run** by reading each config's `source_vocabulary[]` block.

This generator follows the same draft → review → MERGE handoff pattern as Section 6.2 (architectural decision AD-002 — see [`references/AD-002-skill-drafts-customer-reviews.md`](../../.assistant/skills/omop-pipeline-builder/references/AD-002-skill-drafts-customer-reviews.md)). The generator drafts CSV rows; you review the coverage report; `01_load_vocabulary.py` MERGEs the CSV. The generator does not auto-MERGE.

**Prerequisite — declare source vocabularies in your per-table configs.**

Each per-table YAML config that needs draft generation gets a `source_vocabulary[]` block declaring which OHDSI vocabulary each source column's codes belong to. This is distinct from `vocabulary_lookups[]` (which governs runtime resolver behavior); `source_vocabulary[]` is generator input only.

```yaml
# configs/condition_occurrence.yaml
source_vocabulary:
  - source_alias: dx
    source_column: DiagnosisCode
    vocabulary_id: ICD10CM
  - source_alias: dx
    source_column: SecondaryDiagnosisCode
    vocabulary_id: ICD10CM
```

The Pydantic schema validates that `source_alias` matches an entry in `sources[]` and that `vocabulary_id` is a non-empty string; the generator surfaces unrecognized vocabularies in the coverage report rather than failing.

**Step 1: Run the multi-vocabulary draft generator.**

```bash
python .assistant/skills/omop-pipeline-builder/scripts/generate_source_concept_map.py \
  --configs configs/condition_occurrence.yaml \
            configs/observation.yaml \
            configs/person.yaml \
            configs/visit_occurrence.yaml \
  --catalog {catalog} \
  --bronze-schema {bronze_schema} \
  --ref-schema reference \
  --output seed_data/source_to_concept_map_custom.csv \
  --output-report-dir reports \
  --profile $DATABRICKS_CONFIG_PROFILE
```

What the generator does:

1. Reads each YAML config's `source_vocabulary[]` and `sources[]` blocks; resolves `{catalog}` / `{bronze_schema}` placeholders into source-table FQNs.
2. For each `(source_alias, source_column)` tuple, runs `SELECT DISTINCT <column> FROM <fqn> WHERE <column> IS NOT NULL` against the bronze data.
3. For each declared `vocabulary_id`, batches the distinct codes and queries `reference.concept` (chunk size 500 by default) to find direct matches.
4. For non-standard concepts found in step 3, queries `reference.concept_relationship` for `Maps to` targets (single-hop traversal — this is OHDSI's Approach 1; multi-hop chains surface as `unresolved_ambiguous`).
5. Writes drafted STCM rows to the CSV: `target_concept_id` is the standard concept reached, or 0 for codes in the three `unresolved_*` buckets.
6. Emits a markdown coverage report at `reports/source_mapping_coverage_<timestamp>.md` summarizing the resolution outcome (skip with `--no-report` if you don't need it).

**Step 2: Review the coverage report.**

The report scopes manual review work via five mutually exclusive resolution buckets:

- **`resolved_direct`** — source code matched a STANDARD concept directly (e.g. SNOMED for conditions when the source data is already SNOMED-coded). Trust the drafted `target_concept_id`.
- **`resolved_via_maps_to`** — source code matched a non-standard concept whose `Maps to` chain reached a standard concept (the typical ICD10CM → SNOMED case). Trust the drafted `target_concept_id`; spot-check a sample for clinical appropriateness.
- **`unresolved_no_concept`** — source code is not present in `reference.concept` for the declared vocabulary. Likely a custom EHR code, a deprecated code, or a code-format mismatch (dots / dashes / leading characters). Manual mapping required.
- **`unresolved_no_maps_to`** — source code matches a non-standard concept that has no `Maps to` relationship in the loaded vocabulary version. Check the OHDSI Athena release notes for the version you loaded; the missing relationship may be a known gap (the OHDSI vocabulary team adds Maps-to entries iteratively across releases).
- **`unresolved_ambiguous`** — source code's `Maps to` target is itself non-standard (multi-hop chain) OR has multiple standard targets. v2.0.7's resolver does not auto-traverse multi-hop or auto-pick among multiple standards; manual review owns the choice.

Per-column attribution in the report tells you which per-table YAML config to revisit for each unresolved code — codes from `configs/condition_occurrence.yaml` get attributed to that config, separately from codes from `configs/observation.yaml`, even when both columns share `vocabulary_id: ICD10CM`.

**Step 3: Manually map the unresolved rows.**

Open the drafted CSV (`seed_data/source_to_concept_map_custom.csv`) and locate rows with `target_concept_id = 0`. For each, look up the appropriate standard concept via OHDSI Athena (https://athena.ohdsi.org) — search by source code, by source description, or by clinical synonym. Replace `target_concept_id = 0` with the chosen standard concept_id; update `target_vocabulary_id` accordingly.

**Step 4: Upload and reload (same as Section 6.2 Step 5).**

```bash
databricks fs cp seed_data/source_to_concept_map_custom.csv \
  "/Volumes/{catalog}/reference/vocabulary_files/source_to_concept_map_custom.csv" \
  --overwrite --profile $DATABRICKS_CONFIG_PROFILE

databricks bundle run setup_vocabulary -t production --profile $DATABRICKS_CONFIG_PROFILE
```

The MERGE in `01_load_vocabulary.py` is idempotent — re-running with an updated CSV updates the rows with new `target_concept_id` values without producing duplicates (MERGE keys on `source_code` + `source_vocabulary_id`).

**Step 5: Re-validate the affected tables.**

After the MERGE completes, re-run the affected pipelines and re-run `validate_omop.py` (or the in-project `99_validate_omop_output.py` notebook) to confirm Layer 3 (FK integrity) findings have shrunk for the columns you remapped.

#### Worked example — sample coverage report

The generator emits the following kind of report. Concrete numbers and codes vary by your bronze data; the structure and section ordering are stable across runs. This sample is rendered from synthetic CoverageData; no real customer data is included.

````markdown
# Source-to-concept mapping coverage report

_Generated: `2026-05-04T14-30-22`_

Drafted source_to_concept_map rows from the mapping generator. This report is the post-generation triage artifact for the draft generator (`scripts/generate_source_concept_map.py`). Use it to scope manual mapping work for the unresolved buckets before running `01_load_vocabulary.py` to MERGE the CSV into the UC `source_to_concept_map` Delta table.

## Overview

- Vocabularies analyzed: **3**
- (source_alias, source_column) tuples covered: **4**
- Distinct source codes seen: **4236**
- Resolved (standard concept reached): **4121** (97.3%)
- Unresolved (manual mapping required): **115**

## Per-vocabulary coverage

Five mutually exclusive resolution buckets per source vocabulary. `resolved_direct` + `resolved_via_maps_to` is the resolved-to-standard-concept count; the three `unresolved_*` columns identify gaps that need manual mapping.

| vocabulary_id | total | resolved | direct | via Maps-to | unresolved | no concept | no Maps-to | ambiguous | resolution % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ICD10CM | 4218 | 4109 | 0 | 4109 | 109 | 37 | 58 | 14 | 97.4% |
| Race | 12 | 8 | 8 | 0 | 4 | 4 | 0 | 0 | 66.7% |
| Visit | 6 | 4 | 4 | 0 | 2 | 2 | 0 | 0 | 66.7% |

## Per-column coverage attribution

Distinct codes seen per `(source_alias, source_column)` tuple, with the originating per-table YAML config for traceability.

| config | table | source_alias | source_column | vocabulary_id | distinct codes | resolved | unresolved |
|---|---|---|---|---|---:|---:|---:|
| configs/condition_occurrence.yaml | condition_occurrence | dx | DiagnosisCode | ICD10CM | 3914 | 3826 | 88 |
| configs/observation.yaml | observation | prob | ProblemListCode | ICD10CM | 612 | 591 | 21 |
| configs/person.yaml | person | pat | RaceCode | Race | 12 | 8 | 4 |
| configs/visit_occurrence.yaml | visit_occurrence | enc | EncounterTypeCode | Visit | 6 | 4 | 2 |

## Sample unmapped codes

Up to **20** unmapped codes per vocabulary, in lexicographic order. Use these as the starting point for manual mapping work.

### ICD10CM
B97.29
J12.82
M79.604
R05.9
U07.1
Z11.52
Z20.822

### Race
DECLINED
OTHER
UNK
UNREPORTED

### Visit
LWBS
TELE

## Recommended next steps

1. **Resolved rows**: trust the drafted `target_concept_id` values for codes in the `resolved_direct` and `resolved_via_maps_to` buckets.
2. **Unresolved rows (`target_concept_id = 0`)**: manual mapping is required before `01_load_vocabulary.py` MERGEs the CSV (per AD-002 — skill-drafts / customer-reviews handoff).
3. **Per-column attribution**: prioritize manual mapping work by per-table YAML config — configs with the largest `unresolved` columns warrant the most attention.
4. **Vocabulary coverage**: vocabularies where `total_distinct_codes` is large but `resolution %` is low may indicate a vocabulary version mismatch, a CPT4 license-gating issue, or a custom code system incorrectly tagged with an OHDSI vocabulary_id.

## Generation metadata

- Generator: `scripts/generate_source_concept_map.py` (v2.0.7+)
- Resolver: `scripts/_concept_resolver.py` (Approach 1, single-hop Maps-to)
- valid_start_date written to STCM rows: `20260504`
- valid_end_date written to STCM rows: `20991231`
- Concept-lookup IN-list chunk size: `500`
- Configs read: 4
````

The actual emitted report includes per-vocabulary metadata footers and renderer-level escaping for special characters in code values; see `scripts/_coverage_report.py` for the rendering source of truth.

#### When to use which generator

| Situation | Use |
|---|---|
| One vocabulary, one column, you're bootstrapping the first STCM rows | `generate_source_mappings.py` (Section 6.2 Step 4) |
| Multiple vocabularies across multiple per-table configs, configs already landed | `generate_source_concept_map.py` (this section) |
| Running coverage analysis without re-drafting the CSV | (defers to v2.0.8) — until then, re-run `generate_source_concept_map.py` and inspect the new coverage report |
| You need an HTML coverage report instead of markdown | (defers to v2.0.8) — until then, view the markdown report in any modern editor or paste into a wiki |

### 6.4 Vocabulary refresh procedure

OHDSI publishes Athena vocabulary releases on an irregular cadence (typically quarterly, sometimes more frequent for safety / regulatory updates such as new ICD codes for emerging conditions or RxNorm updates for newly approved drugs). Refreshing the vocabulary on your workspace is operationally similar to the first-load procedure (Section 6.1), but the consequences are different — a refresh can both improve coverage (corrections) and reduce coverage (regressions), and you need to discriminate the two.

#### When to refresh

- **Quarterly check.** Visit https://athena.ohdsi.org and compare the latest release date against your loaded version. Your loaded version is recoverable from the vocabulary's `vocabulary` table: `SELECT vocabulary_id, vocabulary_version FROM {catalog}.reference.vocabulary WHERE vocabulary_id = 'ICD10CM';` (and analogously for SNOMED, RxNorm, LOINC, CPT4).
- **Triggered by a known content event.** New ICD code blocks (e.g. annual ICD-10-CM updates released by CMS each October), new vaccine/drug entries in RxNorm, or OHDSI-announced safety corrections may justify an out-of-cycle refresh.
- **Before validating a new bronze data window.** If your bronze source data has expanded into a code range you didn't previously have (e.g. a new clinical service launched, or a new ICU unit's terminology came online), refreshing before drafting STCM rows for the new codes raises the resolution percentage in the coverage report.

Refreshing more frequently than quarterly is rarely necessary outside the safety/regulatory case; OHDSI's Maps-to relationships and standard_concept assignments are stable enough between releases that quarterly cadence is the conventional baseline.

#### How to refresh

The mechanics are identical to Section 6.1 — re-download from Athena, re-extract, re-upload to the UC Volume, re-run the vocabulary load job. `01_load_vocabulary.py` uses MERGE for `concept`, `concept_relationship`, and the other vocabulary tables, so re-running is idempotent: rows that haven't changed remain unchanged, rows with updates land their updates, and new rows are inserted.

```bash
unzip -o "vocabulary_download_v5_*.zip" -d /tmp/ohdsi_vocab
mv /tmp/ohdsi_vocab/{cpt4.jar,cpt.sh,readme.txt,cpt.bat} /tmp/ohdsi_vocab_extras/ 2>/dev/null || true

databricks fs cp -r /tmp/ohdsi_vocab/ \
  "/Volumes/{catalog}/reference/vocabulary_files/" \
  --overwrite --profile $DATABRICKS_CONFIG_PROFILE

databricks bundle run setup_vocabulary -t production --profile $DATABRICKS_CONFIG_PROFILE
```

The CPT4 license refresh (if you use CPT4) is a separate step — re-run the CPT4 license workflow in Athena before downloading, otherwise CPT4 codes will resolve to 0 after refresh just as they did on first load.

#### What to expect — regression vs correction discrimination

A refresh produces three kinds of diff against your previously-loaded vocabulary:

**Corrections (good, expected):**

- New `Maps to` relationships added that weren't present in your previous version. Codes that previously landed in `unresolved_no_maps_to` now resolve via Maps-to traversal.
- Newly added concept_codes for source codes that didn't previously exist in the vocabulary. Codes that previously landed in `unresolved_no_concept` now resolve.
- Updated `standard_concept` assignments where a more accurate standard was promoted.

**Regressions (potentially bad, may need action):**

- Concepts that previously had `standard_concept = 'S'` now have `standard_concept = NULL` (no longer standard). Pipelines that relied on the previously-standard concept now resolve to 0 because the resolver only emits standard concepts. This is OHDSI's standard-concept maintenance — the de-promoted concept usually has an alternative; check the concept's Maps-to entries in Athena.
- Concepts that previously existed are now marked with `invalid_reason = 'D'` (deprecated) or `'U'` (upgraded). The resolver filters these out (Section 5.1 covers this); the customer-side effect is that codes pointing to the deprecated concept now resolve to 0 unless an upgrade replacement exists in concept_relationship.
- Maps-to relationships that previously existed have been removed. Codes that resolved via Maps-to in the previous version now land in `unresolved_no_maps_to`.

**Neutral changes:**

- Vocabulary version bumps with no semantic change. Common for minor releases that update timestamps without altering the concept content for vocabularies your data uses.

#### How to detect regressions vs corrections

The coverage report (Section 6.3) is the natural diagnostic surface. Run it before and after the refresh against the same per-table configs and bronze data, then diff the two reports:

```bash
# Before refresh — capture baseline coverage
python .assistant/skills/omop-pipeline-builder/scripts/generate_source_concept_map.py \
  --configs configs/*.yaml \
  --catalog {catalog} --bronze-schema {bronze_schema} \
  --output-report-dir reports/before_refresh \
  --output seed_data/draft_before.csv \
  --profile $DATABRICKS_CONFIG_PROFILE

# Run the vocabulary refresh (above)
# ... 

# After refresh — capture post-refresh coverage  
python .assistant/skills/omop-pipeline-builder/scripts/generate_source_concept_map.py \
  --configs configs/*.yaml \
  --catalog {catalog} --bronze-schema {bronze_schema} \
  --output-report-dir reports/after_refresh \
  --output seed_data/draft_after.csv \
  --profile $DATABRICKS_CONFIG_PROFILE

# Diff the two coverage reports
diff reports/before_refresh/source_mapping_coverage_*.md \
     reports/after_refresh/source_mapping_coverage_*.md
```

The per-vocabulary table's resolution-percentage column makes the diff direction obvious: a vocabulary where `resolution %` increased is a correction; a vocabulary where it decreased is a regression. The per-column attribution table tells you which per-table YAML config to investigate for regressed columns.

For a more granular signal, run `validate_omop.py` Layer 3 (FK integrity) before and after the refresh — the FAIL counts on `*_concept_id` columns surface regressions where previously-valid concept_ids no longer resolve.

#### What to do when regressions are found

For each regressed code:

1. Look up the previous standard concept_id in OHDSI Athena. The Athena concept page surfaces `valid_start_date`, `valid_end_date`, and `invalid_reason` — these tell you whether the concept was deprecated, de-standardized, or upgraded.
2. If the concept was upgraded (`invalid_reason = 'U'`), check the `Concept Replaced by` relationship in Athena for the new standard concept. Update your manual STCM row (or the drafted STCM row, if you re-ran the generator) to point at the new standard.
3. If the concept was deprecated without replacement, the source code no longer has a clinical OHDSI mapping. Decide whether to (a) accept resolution to 0 (the row will surface as a Layer 3 finding but won't break the pipeline), or (b) map manually to the closest clinically appropriate standard concept based on your domain expertise.
4. If the regression is unexpected for your data (a code you'd expect to remain mapped suddenly doesn't), file an issue at the OHDSI vocabulary GitHub (https://github.com/OHDSI/Vocabulary-v5.0/issues) — community-curated vocabularies sometimes have unintended regressions that the OHDSI team accepts as bug reports.

#### Idempotency contract — safe to re-run

`01_load_vocabulary.py` uses Delta MERGE on `concept` (key: `concept_id`), `concept_relationship` (key: `concept_id_1, concept_id_2, relationship_id`), `vocabulary` (key: `vocabulary_id`), and the other vocabulary tables. Re-running the vocabulary load job is safe and produces no duplicates. The COPY INTO → MERGE pipeline is the recommended pattern for both first-load and refresh; the runbook does not provide a separate "refresh-mode" job because the same job works for both cases.

If a refresh is interrupted mid-load (network blip, warehouse auto-scaling event, transient SDK error), simply re-run `databricks bundle run setup_vocabulary -t production`. Delta tracks loaded files via the operation log, so COPY INTO skips already-loaded files; MERGE re-applies cleanly against any partially-loaded rows.

---

## 7. End-to-End Execution

### New Table Playbook (reference this for every table)

```
1. Generate config  →  Genie prompt or copy existing config
2. Validate config  →  from config_loader import load_config; load_config("configs/X.yaml")
3. Deploy           →  ./deploy.sh production
4. Run pipeline     →  UI / CLI / Genie
5. Validate output  →  99_validate_omop_output.py (5 layers)
6. Wire into DAG    →  Uncomment task in jobs.yml, set depends_on, bundle validate
```

See [`SKILL.md`](.assistant/skills/omop-pipeline-builder/SKILL.md) for the detailed 8-step workflow. See [`references/omop_dag_dependencies.md`](.assistant/skills/omop-pipeline-builder/references/omop_dag_dependencies.md) for Round 1-4 dependencies.

### 7.1 Example 1: Person (Round 1) — Full Walkthrough

**Deploy the bundle:**
```bash
cd /path/to/omop_etl
CATALOG=CHANGEME ./deploy.sh production
# Does: bundle validate → bundle deploy → sync configs to Volume → upload skill
# Set CATALOG to match your databricks.yml catalog variable
```

**Run the Person pipeline:**

Option A (UI): Workflows → Jobs → `omop_full_build_production` → click **Run now** dropdown arrow → **Run selected tasks** → check only `person` → Run

Option B (CLI — runs the entire job, person first then visit_occurrence):
```bash
databricks bundle run omop_full_build -t production --profile $DATABRICKS_CONFIG_PROFILE
```

Option C (run just the person pipeline directly):
```bash
databricks pipelines start-update <pipeline-id> --full-refresh --profile $DATABRICKS_CONFIG_PROFILE
# Find pipeline-id: databricks pipelines list-pipelines --profile $DATABRICKS_CONFIG_PROFILE | grep omop_person
```

Option D (Genie Code — open a notebook, Agent mode):
```
Run the person pipeline
```

**Validate (5 layers):**

Open `src/99_validate_omop_output.py` as a notebook in your workspace (import it or open from the bundle's synced files). It uses Databricks notebook widgets — set these values in the widget bar at the top:

| Widget name | Value |
|---|---|
| `catalog` | Your catalog name (e.g., `CHANGEME`) |
| `core_schema` | `core_omop` |
| `ref_schema` | `reference` |
| `table` | `person` |

Click **Run All**.

| Layer | What it checks | If it fails |
|---|---|---|
| L1 Schema | All required columns present with correct types | Column name wrong in config → fix `column_mappings` → redeploy → re-run |
| L2 PK | `person_id` is unique | Join produces duplicates → check join logic or dedup |
| L3 RI | `concept_ids` resolve to `reference.concept` | `source_to_concept_map` missing your codes → seed more codes (Section 6) |
| L4 Domain | Concepts are in the correct domain | Wrong `vocabulary_id` or `domain_id` in vocabulary_lookup |
| L5 Completeness | NOT NULL columns are not null | Bronze column is genuinely NULL, or join lost rows → investigate source data |

**Iterate:** Fix config → `./deploy.sh production` → re-run pipeline → re-validate. Repeat until 5/5 pass.

**Verify Unity Catalog lineage (do this once after your first successful pipeline run):**

Navigate to **Catalog Explorer** → `{catalog}` → `core_omop` → `person` → **Lineage** tab. You should see:

- Column-level lineage from `{bronze_schema}.patient` (e.g., `bronze_clinical.patient`) → `core_omop.person`
- The vocabulary reference tables (`reference.concept`, `reference.source_to_concept_map`) as upstream dependencies
- Every column in `core_omop.person` traced back to its source expression

This lineage is automatic — SDP traces the Spark execution plan and records it in Unity Catalog. If you don't see lineage:
- Confirm the pipeline ran on a UC-enabled cluster (serverless always is)
- Confirm both bronze and silver tables are in Unity Catalog (not hive_metastore)
- Check **Catalog Explorer** → **Lineage** tab, not the table preview tab

Use lineage to answer audit questions: "where does `race_concept_id` come from?" → click the column → see `patient.RaceCode` → `source_to_concept_map` → `concept_id 8527`. This is the traceability chain PHI auditors and research governance teams need.

### 7.2 Example 2: Visit Occurrence (Round 2) — Condensed

Key differences from Person:
- **Depends on Person** — Person must pass validation first
- **Encounter type vocabulary:** uses `source_to_concept_map` with `source_vocabulary_id: Visit` for `visit_concept_id`. You must seed your encounter type codes (Section 6).
- **Provenance concept:** `visit_type_concept_id` is a literal `32817` ("EHR encounter record") — NOT a vocabulary lookup. This is provenance (how the record was captured), not visit kind.
- **Date range expectation:** `visit_start_date <= visit_end_date` as a `drop` expectation — bad date ranges are removed.

See your generated `configs/visit_occurrence.yaml` for the complete config. Adjust column names to match your schema, deploy, run, validate — same pattern as Person.

### 7.3 Example 3: Condition Occurrence (Round 3) — With Placeholder

Key differences from Person and Visit:
- **Depends on Person + Visit Occurrence** — both must pass first
- **ICD-10 vocabulary:** uses `concept_table` for `condition_source_concept_id` (direct ICD-10 concept lookup). `condition_concept_id` (standard SNOMED concept) requires `concept_table_mapped` — **see Section 5.5 status note**.
- **No natural primary key:** `encounter_diagnosis` has no single-column PK. Uses `ROW_NUMBER()` or `xxhash64()` to generate `condition_occurrence_id`.
- **Diagnosis type CASE:** `condition_type_concept_id` maps Primary/Secondary/Admitting to OHDSI Type Concept IDs via inline CASE. Update the CASE values to match your `DiagnosisType` codes.

Generate the config using the Genie Code skill (see prompt below). The skill produces a Pydantic-validated YAML config with the correct vocabulary resolution strategies.

Genie prompt:
```
Generate an OMOP config for condition_occurrence from {catalog}.{bronze_schema}.encounter_diagnosis
```

### 7.4 Adding More Tables — The Pattern

For each new OMOP table:

1. **Generate config:** Genie Code prompt or copy an existing config and modify
2. **Validate config:** `from config_loader import load_config; load_config("configs/your_table.yaml")`
3. **Deploy:** `./deploy.sh production`
4. **Run pipeline:** UI, CLI, or Genie
5. **Validate output:** Open `src/99_validate_omop_output.py` as notebook, set widgets, Run All
6. **Wire into DAG:** Uncomment the task in `resources/jobs.yml`, set `depends_on`, `databricks bundle validate` before deploy

See `SKILL.md` for the complete 8-step workflow. See `references/omop_dag_dependencies.md` for the Round 1-4 dependency chart.

**Always run `databricks bundle validate -t production` before deploying.**

### 7.5 BYO-ETL: validation-only tables

This skill builds 14 of the 20 OMOP CDM v5.4 tables that `validate_omop.py` checks. The other 6 — `visit_detail`, `device_exposure`, `note`, `note_nlp`, `specimen`, `dose_era` — are validation-only.

This is architectural decision AD-001. The 14 tables this skill builds cover the OMOP coverage most projects need (person, condition_occurrence, drug_exposure, procedure_occurrence, measurement, observation, visit_occurrence, death, condition_era, drug_era, plus the dimension tables care_site, location, provider, observation_period). The 6 BYO-ETL tables require data sources too varied to template — so the skill validates them but lets you build them however fits your data.

#### When you need a BYO-ETL table

Three patterns cover the most common BYO-ETL data sources:

**`device_exposure` (clinical device data)** — Lakeflow Connect ingests from device-tracking systems and EHR device modules; you can also build it from your own bronze tables via custom SQL/Spark. Land your data in `core_omop.device_exposure` matching the OMOP CDM v5.4 spec columns; `validate_omop.py` does the rest. Domain anchor: `device_concept_id` in the Device domain.

**`note` and `note_nlp` (clinical notes and NLP-derived data)** — your NLP pipeline (OHDSI HADES, custom transformer, vendor tooling like Linguamatics or AWS Comprehend Medical) produces `note` rows from raw text and `note_nlp` rows from extracted entities. Land them in `core_omop.note` and `core_omop.note_nlp` matching the spec columns; `validate_omop.py` checks domain conformance on `language_concept_id` (Language domain), `encoding_concept_id` (Metadata domain), `note_type_concept_id` (Type Concept domain), and the NLP-side concepts.

**`specimen`, `visit_detail`, `dose_era` (case-by-case)** — lab system integrations populate `specimen`. EHR sub-visit tracking (e.g., ICU admission events within an inpatient stay) populates `visit_detail`. Drug exposure roll-up at the ingredient level produces `dose_era` (similar to `drug_era` but ingredient-only — you compute it from `drug_exposure` after that pipeline runs). Each requires custom ETL; the skill validates whatever you build.

#### How to use the validator on BYO-ETL tables

1. Build your BYO-ETL data into `core_omop.<table_name>` via your chosen ETL.
2. Run `python validate_omop.py --table <catalog>.core_omop.<table_name>` from the skill's `scripts/` directory (the validator runs against one table per invocation; the scaffolded `src/99_validate_omop_output.py` notebook iterates all 20 spec-covered tables in one run).
3. For tables you've built, the validator runs all 5 layers (schema, PK uniqueness, FK integrity, domain conformance, NOT NULL completeness).
4. For tables you haven't built yet, Layer 1 short-circuits with a `schema:table_missing` finding pointing back to this section. No traceback; subsequent layers cleanly skip.
5. Fix any findings. Section 8 'Validation Failures (Post-Pipeline)' below has common-fix mappings.

#### Why this asymmetry exists

OMOP CDM v5.4 spec coverage is the conformance test surface; build coverage is the engineering surface. The 6 BYO-ETL tables are real OHDSI v5.4 clinical tables — the spec must include them so customers who do build them get conformance checks. The build surface is narrower because templating ETL for clinical devices, NLP outputs, or specimen tracking would produce code that doesn't match any specific customer's data shape. Customer ETL plus skill validation is the right contract.


---

## 8. Troubleshooting

### Config Validation Errors (Pydantic)

| Error | Cause | Fix |
|---|---|---|
| `extra fields not permitted` | YAML has a key not in the schema | Check `configs/_schema.yaml` for valid keys. Common: `on:` instead of `condition:`, `strategy:` instead of `resolution:` |
| `resolution=source_to_concept_map requires source_vocabulary_id` | Missing `source_vocabulary_id` | Add `source_vocabulary_id: Race` (or your vocab) |
| `resolution=concept_table requires vocabulary_id and domain_id` | Missing required fields | Add both `vocabulary_id` and `domain_id` |
| Expectations are strings not objects | Each needs `{name, expr}` | Convert `"person_id IS NOT NULL"` → `{name: valid_person_id, expr: "person_id IS NOT NULL"}` |
| `on:` parsed as boolean True | YAML 1.1 gotcha | Use `condition:` for join predicates, never `on:` |

### Bundle Deploy Errors

| Error | Cause | Fix |
|---|---|---|
| `cannot resolve bundle auth configuration` | CLI profile host doesn't match `databricks.yml` host | Ensure `host:` matches `databricks configure --profile` |
| `pipeline_id reference not found` | Pipeline resource not in `pipeline_generic.yml` | Uncomment or add the pipeline block for the new table |
| `unknown task_key in depends_on` | Referenced a task that doesn't exist | Uncomment prerequisite tasks or remove the dependency |

### Pipeline Runtime Errors

| Error | Cause | Fix |
|---|---|---|
| `Pipeline configuration key 'table_name' is missing` | Pipeline config block doesn't set `table_name` | Check `resources/pipeline_generic.yml` configuration section |
| `Cannot read YAML config at '/Volumes/...'` | Config not synced to UC Volume | Re-run `./deploy.sh production` |
| `UNRESOLVED_COLUMN` | Column name in YAML doesn't match bronze table | Run `DESCRIBE TABLE`, fix the column reference in your config |
| `Table or view not found` | Source table path wrong or doesn't exist | Check `sources[].table` path, verify table with `SHOW TABLES` |
| `from pyspark import pipelines as dp` fails | Runtime too old for SDP | Use Databricks Runtime 16.0+ or serverless compute |

### Vocabulary Resolution (All concept_ids = 0)

| Symptom | Cause | Fix |
|---|---|---|
| All `race_concept_id = 0` | `source_to_concept_map` not seeded with your race codes | Run distinct code audit, seed the map (Section 6) |
| All `condition_concept_id = 0` with `source_to_concept_map` | ICD-10 shouldn't use STCM | Change to `resolution: concept_table` with `vocabulary_id: ICD10CM` |
| Concept_code doesn't match | Code format differs (dots, dashes, leading chars) | Check: `SELECT concept_code FROM {catalog}.reference.concept WHERE vocabulary_id = 'ICD10CM' AND concept_code LIKE 'E11%'` |
| CPT4 concepts all 0 | CPT4 vocab not loaded (license-gated) | Complete CPT4 license step in Athena, reload vocabulary |

### Validation Failures (Post-Pipeline)

| Layer | Failure | Fix |
|---|---|---|
| L1 Schema | Missing column | Add to `column_mappings` in config. Check `references/omop_cdm_v54_spec.md` for required columns. |
| L1 Schema | Wrong type | Adjust CAST (e.g., `CAST(... AS BIGINT)` for IDs, `CAST(... AS TIMESTAMP)` for datetimes) |
| L2 PK | Duplicates | Fix join (row explosion) or add dedup logic |
| L3 RI | Orphan concept_ids | Seed `source_to_concept_map` or fix vocabulary_lookup |
| L4 Domain | Wrong domain | Check `vocabulary_id` and `domain_id` filters |
| L5 Completeness | Unexpected NULLs | Bronze data quality issue, or adjust expectation to `warn` |

### Genie Code Issues

| Issue | Fix |
|---|---|
| Skill doesn't fire | Re-deploy: `./deploy.sh production`. Verify path: `databricks workspace list "/Workspace/Users/<you>/.assistant/skills/"` |
| YAML has 10+ Pydantic errors | Skill didn't read canonical example. Re-prompt: "Read references/canonical_examples.md first, then regenerate." |
| Genie hangs > 90 seconds | Fall back to pre-generated configs as starting points and edit manually. Then file a hang report at https://github.com/saselvan/genie-code-omop/issues — include the table you were generating, the bronze schema shape, and the agent transcript if available. The skill has no automated hang telemetry; the GitHub Issues queue is the only signal the maintainers see for production hang patterns. |

---

## 9. Security considerations

This skill operates inside Genie Code Agent mode, which provides certain platform-level protections (Unity Catalog identity propagation, Lakeguard query isolation, model-input filtering at the Anthropic API layer, confirmation gates for destructive operations). Other security-relevant properties depend on workspace and account configuration that the skill cannot enforce. This section names the configuration prerequisites and skill-flow-specific responsibilities customers should review before running this skill in production against PHI or otherwise-sensitive data.

This is not a comprehensive security guide; it is a checklist of skill-relevant items that customers in regulated environments commonly need to address. Your organization's security review process is the authoritative surface for which controls are required.

### 9.1 Skills are not secrets

The contents of `.assistant/skills/` (this skill's instructions, references, scripts, templates) are accessible to any user with workspace access who can invoke Genie Code. Genie Code can be coerced via prompt patterns to write out skill content. Treat everything in your scaffolded project's `.assistant/skills/`, `src/`, `configs/`, `seed_data/`, and `references/` directories as readable by anyone who can reach the workspace.

What this means in practice:

- **Do not embed credentials, API keys, or PHI in any skill-readable file.** This includes seeded `source_to_concept_map_custom.csv` rows (the source codes themselves usually aren't PHI but verify before committing), per-table YAML configs, and reference documentation. Use Databricks Secrets (https://learn.microsoft.com/en-us/azure/databricks/security/secrets) for runtime credentials; reference them via `${{ secrets.* }}` substitution in `databricks.yml` rather than inlining.
- **Do not assume agent-side instructions stay private.** If you customize SKILL.md or add reference files for your project, treat the content as effectively public-within-workspace.
- **Generated YAML configs are not secrets either.** The agent writes configs into `configs/`; anyone with workspace read access can see them. The configs reference UC catalog/schema/table names — confirm these names are not themselves sensitive (e.g. patient-cohort-name leakage via a table name).

### 9.2 Restricted Access network mode

The Databricks platform default is **Full Access** network egress (unrestricted outbound from compute resources). Customers in regulated environments typically opt into **Restricted Access** at the account level, which limits egress to an allowlist of Databricks-managed endpoints plus customer-configured destinations.

The skill cannot enforce Restricted Access — this is an account-level configuration decision. If your organization's security policy requires restricted egress for AI features, confirm with your account admin that Restricted Access is enabled for the workspaces where this skill will run. The Databricks documentation on AI feature network configuration covers the specifics for Genie Code Agent mode (https://learn.microsoft.com/en-us/azure/databricks/generative-ai/agent-framework/managed-features).

### 9.3 Verbose audit logging

Default Databricks audit logging captures workspace-level events (who logged in, what notebooks were run, what jobs completed) but may not capture the granular query and command execution detail that HIPAA, HITECH, or your organization's audit requirements demand.

For HLS / regulated production use:

- Enable verbose audit logging at the workspace level (https://learn.microsoft.com/en-us/azure/databricks/admin/account-settings/verbose-logs).
- Confirm that audit log delivery to your SIEM or audit-log destination is configured and validated.
- Verify that workspace queries against `core_omop` tables produce auditable trail entries that meet your retention and access-tracking requirements.

Workspace audit logs capture executed SQL and notebook operations. Agent-side reasoning traces, intermediate tool calls, and generated-but-not-executed code are NOT confirmed to be in customer-queryable system tables today. If your audit requirements include capturing what the agent considered (not just what it executed), this is a known gap — see Section 9.6.

### 9.4 HIPAA / Compliance Security Profile workspace mode

Genie Code Agent mode is HIPAA-eligible only in workspaces configured under Databricks' Compliance Security Profile (CSP). If your skill use will touch PHI:

- Confirm with your account admin that the workspace is in CSP mode (https://learn.microsoft.com/en-us/azure/databricks/security/privacy/hipaa).
- Confirm that your Business Associate Agreement (BAA) with Databricks is in place and covers AI features.
- Confirm that the catalogs and schemas the skill will read (bronze, silver, reference) are within the CSP-protected workspace boundary.

The skill itself does not implement HIPAA-specific data handling at the platform layer (encryption, access controls, audit trails are platform-provided in CSP mode). The skill does need you to verify the workspace prerequisite is met.

### 9.5 Customer responsibility for reviewing generated code

Per AD-002 (skill-drafts / customer-reviews handoff — see [`references/AD-002-skill-drafts-customer-reviews.md`](../../.assistant/skills/omop-pipeline-builder/references/AD-002-skill-drafts-customer-reviews.md)), the skill generates artifacts that the customer reviews and applies. Generated YAML configs, drafted STCM rows, and scaffolded Python notebooks are all draft artifacts.

The security implication: **the customer is the security review gate for everything the skill generates.** This includes:

- **YAML configs.** Generated configs reference your UC table paths, vocabulary lookups, and column expressions. Review for unintended data exposure (e.g. a join that exposes more PHI than the OMOP target requires) before deployment.
- **Drafted STCM rows.** The generator writes `target_concept_id` values based on OHDSI vocabulary lookups. Confirm that resolved standard concepts are clinically appropriate before MERGEing the CSV — a wrong mapping can mis-classify clinical data downstream.
- **Scaffolded notebooks.** `01_load_vocabulary.py`, `02_omop_transform_pipeline.py`, `99_validate_omop_output.py` and other scaffolded files are reference implementations. Customers running them in production should review them as code their team owns, not as black-box infrastructure.

Standard secure-development practices apply to all generated code: code review before merge, change-tracking via git, and post-deploy validation via the 5-layer validator and your organization's data-quality monitoring.

### 9.6 Audit-trail gap for agent reasoning

Workspace audit logs capture executed SQL and Python operations. They do not currently capture:

- The agent's reasoning trace (what the agent considered before producing output).
- Intermediate tool calls the agent made during a generation flow (e.g. workspace introspection queries the agent ran to discover bronze schema).
- Generated-but-not-executed code (e.g. a YAML config draft that was generated, reviewed, and rejected without ever being deployed).

For most use cases, this gap is acceptable — the executed-operation audit trail is sufficient for compliance and forensic purposes. For organizations with audit requirements that include the agent's deliberation surface (rather than just outcomes), this is a known limitation. The conventional mitigations:

- Capture agent transcripts manually at decision points (e.g. when reviewing a generated YAML config, save the prompt + response pair to a project-tracked decisions log).
- Treat the per-table YAML config diffs in your git history as the authoritative record of "what the agent produced and what the customer accepted." The git commit log is the de facto deliberation-outcome audit trail even when the deliberation itself isn't captured.
- File a feature request at https://github.com/saselvan/genie-code-omop/issues if your organization's audit requirements would benefit from richer agent-side telemetry — the skill's surface area for this is limited but maintainer-visible feedback shapes future decisions.

---

## Appendix A: Common OHDSI Concept IDs

| Domain | Code | concept_id | concept_name |
|---|---|---|---|
| Gender | M | 8507 | Male |
| Gender | F | 8532 | Female |
| Race | — | 8527 | White |
| Race | — | 8516 | Black or African American |
| Race | — | 8515 | Asian |
| Race | — | 8557 | Native Hawaiian or Pacific Islander |
| Race | — | 8657 | American Indian or Alaska Native |
| Ethnicity | — | 38003563 | Hispanic or Latino |
| Ethnicity | — | 38003564 | Not Hispanic or Latino |
| Visit | — | 9201 | Inpatient Visit |
| Visit | — | 9202 | Outpatient Visit |
| Visit | — | 9203 | Emergency Room Visit |
| Visit | — | 581385 | Observation Room |
| Provenance | — | 32817 | EHR encounter record |

## Appendix B: File Inventory

| File | Purpose | When to modify |
|---|---|---|
| `configs/<table>.yaml` (each agent-generated config) | Per-table ETL contract | Adjust column names for your EHR source |
| `configs/_schema.yaml` | YAML schema definition (Pydantic) | Never (framework internal) |
| `seed_data/source_to_concept_map_custom.csv` | Local code → concept_id mapping | Replace with your codes |
| `src/02_omop_transform_pipeline.py` | Generic SDP pipeline | Never (framework internal) |
| `src/config_loader.py` | Pydantic validation + source resolution | Never (framework internal) |
| `src/vocab_resolver.py` | Vocabulary join logic | Never (framework internal) |
| `src/column_mapper.py` | SQL expression builder | Never (framework internal) |
| `src/01_load_vocabulary.py` | OHDSI vocabulary loader | Never (run as-is) |
| `src/99_validate_omop_output.py` | 5-layer OMOP validator | Never (run as-is) |
| `databricks.yml` | Bundle config + targets | Set your workspace + variables |
| `deploy.sh` | Deploy automation | Only if your profile name differs |
| `resources/jobs.yml` | DAG task definitions | Uncomment tasks as you activate tables |
| `resources/pipeline_generic.yml` | Pipeline resource definitions | Uncomment pipelines as you activate tables |
| `.assistant/skills/omop-pipeline-builder/SKILL.md` | Genie Code skill instructions | Only to add new reference docs |

## Appendix C: Production Readiness Pointers

This framework builds the transform layer. The operational layer — monitoring, security, testing, capacity — uses standard Databricks features. Don't rebuild these; use them:

| Concern | Where to go |
|---|---|
| **Data volume / performance** | [SDP pipeline performance tuning](https://docs.databricks.com/aws/en/ldp/performance) — partitioning, cluster sizing, auto-scaling, when broadcast joins hit limits |
| **Monitoring / alerting** | [Pipeline event log](https://docs.databricks.com/aws/en/ldp/monitoring-ui) — SDP writes expectation metrics to the event log. Build dashboards on `system.pipeline_events` to track concept resolution rates over time |
| **Security / PHI** | [Column masks and row filters](https://docs.databricks.com/aws/en/security/data-governance/column-masks-row-filters) — apply to `core_omop` tables. [Audit logging](https://docs.databricks.com/aws/en/security/audit-logs) for access tracking |
| **Data lineage** | [Unity Catalog lineage](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage) — automatic column-level lineage from bronze → silver. Visible in Catalog Explorer |
| **Regression testing** | [Databricks Labs DQX](https://github.com/databrickslabs/dqx) or snapshot-compare queries: save a golden row count + concept distribution before vocab refresh, diff after |
| **SLA / capacity planning** | [Job SLA monitoring](https://docs.databricks.com/aws/en/jobs/run-sla) — set duration thresholds on `omop_full_build` tasks. Start with 2x your current runtime as the SLA |

## Appendix D: Glossary

| Term | Definition |
|---|---|
| **OMOP CDM** | Observational Medical Outcomes Partnership Common Data Model — standard schema for clinical data |
| **OHDSI** | Observational Health Data Sciences and Informatics — the community that maintains OMOP |
| **Athena** | OHDSI's vocabulary browser and download service (athena.ohdsi.org) |
| **SDP** | Spark Declarative Pipelines (formerly DLT) — Databricks pipeline runtime |
| **DAB** | Declarative Automation Bundles — Databricks IaC for deploying jobs, pipelines, configs |
| **Genie Code** | Databricks AI coding assistant with Agent mode for skill-based workflows |
| **concept_id** | Integer key in the OHDSI `concept` table representing a clinical concept |
| **source_to_concept_map** | OHDSI table for mapping local/institution-specific codes to standard concept_ids |
| **concept_table_mapped** | Resolution strategy that follows "Maps to" relationships from non-standard to standard concepts |
| **EHR source** | the EHR vendor's enterprise data warehouse (the source of your organization's clinical data) |
| **Pydantic** | Python validation library — enforces the YAML config schema |
| **UC** | Unity Catalog — Databricks governance layer for data and AI assets |
| **silver / core** | Same OMOP target schema, two names from two cultural frames. **Silver** comes from medallion-architecture conventions (bronze → silver → gold tiers). **Core** comes from OHDSI / OMOP CDM naming (the "core OMOP tables" are the standardized clinical data tables). Both terms appear in this skill's docs and code (e.g., the `core_target` config field that customers set, the `core_omop` default schema name, `bundle_state.py`'s `silver_tables` field, the `_probe_silver_tables` workspace probe, and references to "silver tables in Unity Catalog" elsewhere in this runbook). They name the same schema; the dual vocabulary is preserved because each frame has its audience |
