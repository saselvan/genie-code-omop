# Canonical YAML examples — person and measurement

> **Reading order:** start with `person` below for the simplest dimension-table shape,
> then read the **inline canonical example in [`SKILL.md`](../SKILL.md#canonical-yaml-example)**
> (`condition_occurrence`) for the fact-table shape that demonstrates the highest-failure
> structural rules — two-lookup pattern, `domain_id` requirement, hash-based surrogate keys
> with the resolved `*_concept_id` in the hash. Then read `measurement` below for the
> specialized "Maps to + Maps to unit" pattern.
>
> All three examples use illustrative snake_case column names; substitute the columns from
> your bronze table. The structural patterns (resolution strategies, two-lookup rule,
> hash keys, `domain_id`) apply regardless.

## person — dimension-table shape (`source_to_concept_map`)

A simple dimension table: one source row per person, no fan-out. Demonstrates
`source_to_concept_map` for institution-specific codes (race, ethnicity) and a small
inline `CASE` for trivial gender mapping.

```yaml
# Source column names are illustrative — substitute the columns from your bronze table.
# The structural patterns (resolution strategies, two-lookup rule, hash keys, domain_id) apply regardless.
table_name: person
target_schema: core_omop
description: "OMOP CDM v5.4 Person from a patient table joined to a patient_identifier table."

sources:
  - alias: pat
    table: "{catalog}.{bronze_schema}.<your_patient_table>"
  - alias: id
    table: "{catalog}.{bronze_schema}.<your_patient_identifier_table>"

joins:
  - left: pat
    right: id
    type: left
    condition: "pat.patient_id = id.patient_id"

vocabulary_lookups:
  - source_alias: pat
    source_column: race_code
    target_column: race_concept_id
    resolution: source_to_concept_map
    source_vocabulary_id: Race
    fallback: 0
  - source_alias: pat
    source_column: ethnicity_code
    target_column: ethnicity_concept_id
    resolution: source_to_concept_map
    source_vocabulary_id: Ethnicity
    fallback: 0

column_mappings:
  - target: person_id
    expr: "CAST(pat.patient_id AS BIGINT)"
  - target: gender_concept_id
    expr: "CASE WHEN pat.gender_code = 'M' THEN 8507 WHEN pat.gender_code = 'F' THEN 8532 ELSE 0 END"
  - target: year_of_birth
    expr: "YEAR(pat.birth_date)"
  - target: person_source_value
    expr: "CAST(id.mrn AS STRING)"

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

**Why this shape:**
- `source_to_concept_map` for race/ethnicity because institution-specific codes are not in OHDSI Athena.
- Small `CASE` for gender_concept_id (only 3 values: M/F/unknown) — cheaper than a vocabulary lookup.
- No `xxhash64` surrogate key here — the bronze `patient_id` is already a stable PK; passthrough is safe for dimension tables.

> **Next: read [`SKILL.md`'s inline `condition_occurrence` canonical example](../SKILL.md#canonical-yaml-example)** for the two-lookup fact-table shape that LLMs reliably get wrong without explicit guidance.

## measurement — `concept_table_mapped` with `Maps to unit`

Specialized: requires THREE vocabulary lookups (standard concept + unit concept + source concept). Demonstrates the `relationship_id: "Maps to unit"` override for resolving LOINC measurement codes to UCUM unit concepts.

```yaml
# Source column names are illustrative — substitute the columns from your bronze table.
# The structural patterns (resolution strategies, two-lookup rule, hash keys, domain_id) apply regardless.
table_name: measurement
target_schema: core_omop
description: "OMOP CDM v5.4 measurement from a clinical-measurement-shaped table."

sources:
  - alias: fs
    table: "{catalog}.{bronze_schema}.<your_clinical_measurement_table>"
  - alias: enc
    table: "{catalog}.{bronze_schema}.<your_encounter_table>"

joins:
  - left: fs
    right: enc
    type: left
    condition: "fs.encounter_id = enc.encounter_id"

vocabulary_lookups:
  # Standard concept: LOINC → standard Measurement concept via Maps to crosswalk
  - source_alias: fs
    source_column: measurement_name
    target_column: measurement_concept_id
    resolution: concept_table_mapped
    vocabulary_id: LOINC
    domain_id: Measurement
    fallback: 0
  # Unit concept: LOINC → UCUM unit via Maps to unit crosswalk
  - source_alias: fs
    source_column: measurement_name
    target_column: unit_concept_id
    resolution: concept_table_mapped
    vocabulary_id: LOINC
    relationship_id: "Maps to unit"
    domain_id: Unit
    fallback: 0
  # Source concept: LOINC concept directly (non-standard, for traceability)
  - source_alias: fs
    source_column: measurement_name
    target_column: measurement_source_concept_id
    resolution: concept_table
    vocabulary_id: LOINC
    domain_id: Measurement
    standard_only: false
    fallback: 0

column_mappings:
  - target: measurement_id
    expr: "xxhash64(CONCAT_WS('|', CAST(fs.measurement_id AS STRING), CAST(measurement_concept_id AS STRING)))"
  - target: person_id
    expr: "CAST(enc.patient_id AS BIGINT)"
  - target: measurement_date
    expr: "DATE(fs.measurement_datetime)"
  - target: measurement_datetime
    expr: "fs.measurement_datetime"
  - target: measurement_type_concept_id
    expr: "32817"
  - target: value_as_number
    expr: "CAST(fs.measurement_value AS DOUBLE)"
  - target: measurement_source_value
    expr: "fs.measurement_name"

expectations:
  fail:
    - name: valid_pk
      expr: "measurement_id IS NOT NULL"
    - name: valid_person
      expr: "person_id IS NOT NULL"
  warn:
    - name: known_measurement
      expr: "measurement_concept_id != 0"
    - name: has_value
      expr: "value_as_number IS NOT NULL"
```

**Why this shape:**
- Three vocabulary lookups, not two: standard measurement concept, **unit concept**, source concept.
- `relationship_id: "Maps to unit"` is the only override needed — default `"Maps to"` resolves to the standard concept.
- `value_as_number` uses safe `CAST(... AS DOUBLE)` — source values may contain non-numeric strings that produce NULL.
- Surrogate key includes `measurement_concept_id` in the hash for one-to-many fan-out safety (same rule as `condition_occurrence`).
