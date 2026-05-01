# seed_data/

This directory holds source-to-concept-map (STCM) seed data for OMOP vocabulary resolution.

## What goes here

`source_to_concept_map_*.csv` files following the OHDSI STCM schema:

| Column | Notes |
|--------|-------|
| source_code | Code as it appears in your bronze data |
| source_concept_id | Usually 0 unless source code has its own concept_id |
| source_vocabulary_id | Vocabulary identifier (e.g. ICD10CM, your local vocab name) |
| source_code_description | Human-readable description |
| target_concept_id | Resolved OHDSI concept_id |
| target_vocabulary_id | Target vocabulary the concept_id belongs to |
| valid_start_date | YYYYMMDD, typically 19700101 |
| valid_end_date | YYYYMMDD, typically 20991231 |
| invalid_reason | Empty if currently valid |

## How to populate

1. For institution-specific codes (race, ethnicity, local visit types) that don't exist in OHDSI Athena, hand-author rows here.
2. For standard vocabularies (ICD-10, LOINC) that need crosswalk to OMOP standards, prefer `concept_table_mapped` resolution in the YAML config — those don't need STCM rows.
3. Generate STCM CSVs from your distinct source codes using the skill's `generate_source_mappings.py` helper (see SKILL.md).

The vocabulary resolver (`src/vocab_resolver.py`) loads STCM rows at pipeline build time and joins them in for `resolution: source_to_concept_map` lookups.
