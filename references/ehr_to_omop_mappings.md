# Bronze-to-OMOP CDM v5.4 — column-mapping reference

> **Note:** Bronze table and column names in this reference are illustrative (snake_case neutral). Substitute the actual table and column names from your Unity Catalog bronze schema. The OMOP CDM v5.4 target columns and structural resolution patterns are EHR-agnostic.

YAML configs reference the **actual** bronze column names present in UC — verify with `DESCRIBE TABLE` before authoring. The companion **snake-case-column-renamer** skill can normalize PascalCase landings to `snake_case` at ingest if you want a uniform naming convention.

## Bronze table → OMOP target routing

| Bronze table (illustrative) | Grain | Typical OMOP targets | Notes |
|---|---|---|---|
| patient table | one row per patient | `person`, facets of `death`, `location` (via zip) | Demographics + death facts |
| patient identifier table | one row per (patient, identifier) | `person.person_source_value` | Join for MRN and other identifiers |
| encounter table | one row per encounter/visit | `visit_occurrence`, `observation_period` | Encounter window drives visits |
| encounter diagnosis table | one row per (encounter, diagnosis) | `condition_occurrence` | Diagnosis codes → Condition domain |
| procedure order table | one row per ordered procedure | `procedure_occurrence` | Orders with procedure codes + times |
| medication order table | one row per ordered medication | `drug_exposure` | NDC / formulary codes → Drug domain |
| clinical measurement table | one row per measurement | `measurement` | LOINC or local codes; numeric values |

## Generic OMOP-target column patterns

Regardless of bronze shape, OMOP fact and dimension tables share recurring patterns:

| OMOP target column | Resolution pattern | Notes |
|---|---|---|
| `person_id` | `CAST(pat.<patient_id_col> AS BIGINT)` | Surrogate-aligned to bronze patient PK |
| `year_of_birth`, `month_of_birth`, `day_of_birth`, `birth_datetime` | `YEAR(...)`, `MONTH(...)`, `DAY(...)`, `CAST(... AS TIMESTAMP)` | Decompose a single birth-date column |
| `gender_concept_id` | Small `CASE` for M/F → 8507/8532, else 0 | Trivial enough to skip a vocabulary lookup |
| `race_concept_id`, `ethnicity_concept_id` | `source_to_concept_map` with `source_vocabulary_id: Race` / `Ethnicity` | Local codes; rarely match standard concepts directly |
| `*_concept_id` for diagnosis, procedure, drug | `concept_table_mapped` with `domain_id` set to target domain | ICD-10 → SNOMED, CPT4 → SNOMED, NDC → RxNorm via "Maps to" |
| `*_source_concept_id` | `concept_table` with `standard_only: false` + same `domain_id` | Traceability — preserves the original non-standard concept |
| `unit_concept_id` (measurement) | `concept_table_mapped` with `relationship_id: "Maps to unit"`, `domain_id: Unit` | LOINC carries the unit semantics |
| `visit_type_concept_id`, `condition_type_concept_id`, etc. | Literal `32817` ("EHR") for EHR-sourced rows | Provenance, not visit kind — see semantic gotchas below |
| `*_source_value` | `CAST(... AS STRING)` of the original code | OMOP CDM v5.4 specifies VARCHAR(50) |

## Semantic gotchas

- **`visit_type_concept_id` is provenance, not visit kind.** Per OMOP, this field encodes how the visit record was captured (claims, EHR, registry, etc.). Encounter rows from an EHR should use `32817` ("EHR"). The clinical visit category belongs in `visit_concept_id` (Visit domain).
- **Diagnosis type vs condition status:** map "primary/secondary diagnosis" style fields to `condition_type_concept_id` (Type Concept domain), not to visit type.
- **Source concepts vs standard concepts:** retain `*_source_value` and `*_source_concept_id` alongside the mapped standard `*_concept_id` for traceability. Two lookups per coded fact column.
- **Fact-table fan-out:** `concept_table_mapped` may produce multiple rows per source row when one ICD-10 code maps to multiple SNOMED concepts. Surrogate keys must include the resolved `*_concept_id` in the hash.
- **`domain_id` is required for `concept_table` and `concept_table_mapped`:** filters one-to-many fan-out to the correct OMOP domain and is also Pydantic-validated.

## Companion skill

Use **snake-case-column-renamer** when bronze should standardize on `snake_case` at landing. This skill's YAML examples assume snake_case columns — substitute whichever names exist in UC after the rename step (or leave PascalCase if you don't run the renamer).
