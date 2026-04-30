# EHR source to OMOP CDM v5.4 — your organization mapping reference

> **Note:** Source table names in this document follow Epic Clarity conventions. The mapping logic generalizes to other EHRs — substitute the equivalent source table for your platform.

EHR source extracts for your organization land in bronze with **PascalCase** column names and no separators (for example `PatientID`, `AdmitDateTime`). On ingest, the companion **snake-case-column-renamer** skill can normalize names to `snake_case`; YAML configs must reference the **actual** bronze column names present in Unity Catalog.

## Table-level routing

| EHR source table | Primary keys / grain | Typical OMOP targets | Notes |
|----------------|---------------------|----------------------|-------|
| `patient` | `PatientID` | `person`, facets of `death`, `location` (via zip) | Demographics and death facts. |
| `identity_id` | `PatientID` + identity type | `person.person_source_value` | Join for MRN and other identifiers. |
| `pat_enc` | `EncounterID` | `visit_occurrence`, `observation_period` (rollup) | Encounter window drives visits. |
| `pat_enc_dx` | `EncounterID` + diagnosis rows | `condition_occurrence` | Diagnosis codes → Condition domain. |
| `clarity_eap` | `ProcedureID` (reference) | supporting dim for `procedure_occurrence` | Procedure dictionary / descriptions. |
| `order_proc` | `OrderID` | `procedure_occurrence` | Orders with procedure codes & times. |
| `order_med` | `OrderID` | `drug_exposure` | Medication orders; NDC / formularies. |
| `flowsheet_meas` | `MeasID` | `measurement` (vitals, flowsheets) | Numeric/text results → LOINC or local codes. |

## `patient` — column hints

| EHR source column | OMOP target(s) | Notes |
|-----------------|------------------|-------|
| `PatientID` | `person.person_id` | Surrogate key alignment. |
| `BirthDate` | `year_of_birth`, `month_of_birth`, `day_of_birth`, `birth_datetime` | Use date parts + timestamp as needed. |
| `GenderCode` | `gender_concept_id` | Often CASE to standard Gender concepts; unknown → `0`. |
| `RaceCode` | `race_concept_id` | Frequently requires `source_to_concept_map` or org-specific vocabulary. |
| `EthnicityCode` | `ethnicity_concept_id` | Same as race — rarely a straight `concept_code` join. |
| `ZipCode` | `location_id` (indirect) | Geocode or map via `location` builder; nullable. |
| `DeathDate` | `death.death_date` | Populate `death` when present; else omit row. |

## `identity_id` — column hints

| EHR source column | OMOP target(s) | Notes |
|-----------------|------------------|-------|
| `PatientID` | join key to `patient` / `person` | Maintain consistent IDs. |
| `MRN` | `person.person_source_value` | Pick a primary identifier per business rules. |
| `IdentityType` | filter / precedence | Use when multiple identifier rows exist. |

## `pat_enc` — column hints

| EHR source column | OMOP target(s) | Notes |
|-----------------|------------------|-------|
| `EncounterID` | `visit_occurrence.visit_occurrence_id` | Or generated surrogate if remapped. |
| `PatientID` | `visit_occurrence.person_id` | FK to `person`. |
| `AdmitDateTime` | `visit_start_datetime`, `visit_start_date` | Cast to date for date fields. |
| `DischargeDateTime` | `visit_end_datetime`, `visit_end_date` | Nullable for open encounters. |
| `EncounterType` | `visit_concept_id` | Map EHR source encounter types to standard Visit concepts. |
| `ProviderID` | `visit_occurrence.provider_id` | Resolve to `provider` table if loaded. |

## `pat_enc_dx` — column hints

| EHR source column | OMOP target(s) | Notes |
|-----------------|------------------|-------|
| `EncounterID` | `visit_occurrence_id` | Join to visit builder. |
| `DiagnosisCode` | `condition_source_value`, maps to `condition_concept_id` | ICD-10-CM typical; use vocab + STCM. |
| `DiagnosisType` | `condition_type_concept_id` | Primary/secondary role → Type Concept. |
| `DiagnosisDateTime` | `condition_start_datetime`, `condition_start_date` | Fallback to encounter dates if missing. |

## `clarity_eap` — column hints

| EHR source column | OMOP target(s) | Notes |
|-----------------|------------------|-------|
| `ProcedureID` | bridge keys for orders | Not always written directly to OMOP. |
| `ProcedureCode` | CPT / HCPCS / local | Join to `concept` or STCM; CPT4 may be absent until licensed. |
| `ProcedureName` | `procedure_source_value` helper | Useful for QA, not a CDM required field by itself. |

## `order_proc` — column hints

| EHR source column | OMOP target(s) | Notes |
|-----------------|------------------|-------|
| `OrderID` | `procedure_occurrence_id` (if 1:1) | Often generate surrogate keys. |
| `EncounterID` | `visit_occurrence_id` | Required for visit linkage when available. |
| `PatientID` | `person_id` | |
| `ProcedureCode` | `procedure_source_value` → `procedure_concept_id` | |
| `OrderDateTime` | `procedure_datetime`, `procedure_date` | |

## `order_med` — column hints

| EHR source column | OMOP target(s) | Notes |
|-----------------|------------------|-------|
| `OrderID` | `drug_exposure_id` (if 1:1) | Usually surrogate. |
| `EncounterID` | `visit_occurrence_id` | |
| `PatientID` | `person_id` | |
| `MedicationNDC` | `drug_source_value` → `drug_concept_id` | RxNorm / NDC resolution paths. |
| `OrderDateTime` | `drug_exposure_start_*` | |
| `Quantity` | `quantity` | Unit handling may need enrichment. |

## `flowsheet_meas` — column hints

| EHR source column | OMOP target(s) | Notes |
|-----------------|------------------|-------|
| `MeasID` | `measurement_id` | Surrogate strategy per org. |
| `EncounterID` | `visit_occurrence_id` | |
| `PatientID` | `person_id` | |
| `MeasName` | `measurement_source_value` | Map to LOINC where possible. |
| `MeasValue` | `value_as_number` or `value_as_string` | Parse numerics carefully. |
| `MeasDateTime` | `measurement_datetime`, `measurement_date` | |

## Semantic gotchas

- **`visit_type_concept_id` is provenance, not visit kind.** Per OMOP, this field encodes how the visit record was captured (claims, EHR, registry, etc.). EHR source encounter rows from the EHR should use **`32817` — “EHR”** as a standard provenance literal. The clinical visit category belongs in **`visit_concept_id`** (Visit domain), not `visit_type_concept_id`.
- **Diagnosis type vs condition status:** map EHR source “primary/secondary” style fields to **`condition_type_concept_id`** (Type Concept domain), not to visit type.
- **Source concepts vs standard concepts:** retain `*_source_value` / `*_source_concept_id` alongside mapped standard `*_concept_id` fields for traceability.

## Common EHR source column patterns

- **PascalCase tokens:** `PatientID`, `EncounterID`, `DiagnosisCode`, `OrderDateTime`.
- **Key suffix:** `*ID` for surrogate or business identifiers.
- **Timestamp suffix:** `*DateTime` for event times; separate `*Date` columns are less common in raw EHR source but appear after renames.
- **No separators:** avoid assuming `snake_case` until landing / rename is applied — verify with `DESCRIBE TABLE`.

## Companion skill

Use **snake-case-column-renamer** when bronze should standardize on `snake_case` at landing. This pipeline skill’s YAML must use whichever column names exist in UC after that step.
