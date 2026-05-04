# OMOP Common Data Model v5.4 — Tables covered by this skill

This note is an **abridged** abridged summary of required shapes for validation and mapping. Canonical definitions, cardinality, and full optional column lists live in the OHDSI CDM documentation: [OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/cdm54.html).

Machine-readable tables below use: **Nullable** `N` = NOT NULL, `Y` = nullable. **PK** `Y` marks the primary key column. **FK** uses `concept.concept_id` or `person.person_id` style hints. **Domain** is used by `scripts/validate_omop.py` for domain conformance checks on `*_concept_id` columns.

## Encoding Principles

### The encoding rule

**Domain** and **FK** Table cells in this spec follow OHDSI's table cells where OHDSI is explicit. Where OHDSI's table cell is silent, this spec encodes unambiguous narrative-stated values and leaves the cell blank where (a) OHDSI's narrative is actively cautioned, (b) OHDSI's table cell explicitly contradicts narrative, or (c) the column is polymorphic (FK target depends on a companion column).

This rule produces conformance checks that match what an OHDSI conformance test would check (see [OHDSI DataQualityDashboard](https://github.com/OHDSI/DataQualityDashboard)) while avoiding false positives on columns where OHDSI's intent is genuinely ambiguous.

### Examples from the existing spec

These four cases illustrate each branch of the rule. Each cites a specific spec row plus the OHDSI table cell and narrative shape it follows.

- **Encoded narrative** (OHDSI table cell silent, narrative explicit) — `care_site.place_of_service_concept_id` is encoded with Domain `Visit`. OHDSI's [care_site table cell](https://ohdsi.github.io/CommonDataModel/cdm54.html#care_site) leaves the FK Domain blank, but the ETL Conventions narrative explicitly says "Choose the concept in the visit domain that best represents the setting in which healthcare is provided in the Care Site." This case was triaged during v2.0.4a sub-phase A and is the canonical example that surfaced this principle.

- **Blank because OHDSI is silent** (OHDSI table cell silent, narrative also silent or "no specified domain") — `observation.observation_concept_id` has Domain blank. OHDSI's [observation table cell](https://ohdsi.github.io/CommonDataModel/cdm54.html#observation) leaves FK Domain blank, and the ETL Conventions narrative explicitly says "There is no specified domain that the Concepts in this table must adhere to." Same shape: `death.cause_concept_id` (OHDSI narrative says "There is no specified domain for this concept").

- **Blank because OHDSI cautions** (OHDSI table cell silent, narrative warns the vocabulary is unstable) — `procedure_occurrence.modifier_concept_id` has Domain blank. OHDSI's [procedure_occurrence table cell](https://ohdsi.github.io/CommonDataModel/cdm54.html#procedure_occurrence) leaves FK Domain blank, and the User Guide narrative says "the modifiers are intended to give additional information about the procedure but as of now the vocabulary is under review." Encoding any specific Domain here would over-commit to a vocabulary OHDSI itself flags as unsettled.

- **Blank because polymorphic** (FK target depends on a companion `*_event_field_concept_id` column) — `measurement.measurement_event_id` has FK blank. OHDSI's [measurement table cell](https://ohdsi.github.io/CommonDataModel/cdm54.html#measurement) marks Foreign Key as "No" because the linked record's table is determined at runtime by `meas_event_field_concept_id`. Same shape: `observation.observation_event_id`, `note.note_event_id`.

### Pointer to decisions log

For borderline cases where the principle's application is genuinely ambiguous (e.g., a narrative that mentions a domain in passing but doesn't mandate it, or a column whose narrative names two candidate domains), see [`spec_domain_decisions.md`](./spec_domain_decisions.md) for case-by-case reasoning.

## person

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| person_id | BIGINT | N | Y | | |
| gender_concept_id | INT | N | N | concept.concept_id | Gender |
| year_of_birth | INT | N | N | | |
| month_of_birth | INT | Y | N | | |
| day_of_birth | INT | Y | N | | |
| birth_datetime | TIMESTAMP | Y | N | | |
| race_concept_id | INT | N | N | concept.concept_id | Race |
| ethnicity_concept_id | INT | N | N | concept.concept_id | Ethnicity |
| location_id | BIGINT | Y | N | location.location_id | |
| provider_id | BIGINT | Y | N | provider.provider_id | |
| care_site_id | BIGINT | Y | N | care_site.care_site_id | |
| person_source_value | STRING | Y | N | | |
| gender_source_value | STRING | Y | N | | |
| gender_source_concept_id | INT | Y | N | concept.concept_id | |
| race_source_value | STRING | Y | N | | |
| race_source_concept_id | INT | Y | N | concept.concept_id | |
| ethnicity_source_value | STRING | Y | N | | |
| ethnicity_source_concept_id | INT | Y | N | concept.concept_id | |

## observation_period

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| observation_period_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| observation_period_start_date | DATE | N | N | | |
| observation_period_end_date | DATE | N | N | | |
| period_type_concept_id | INT | N | N | concept.concept_id | Type Concept |

## visit_occurrence

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| visit_occurrence_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| visit_concept_id | INT | N | N | concept.concept_id | Visit |
| visit_start_date | DATE | N | N | | |
| visit_start_datetime | TIMESTAMP | Y | N | | |
| visit_end_date | DATE | N | N | | |
| visit_end_datetime | TIMESTAMP | Y | N | | |
| visit_type_concept_id | INT | N | N | concept.concept_id | Type Concept |
| provider_id | BIGINT | Y | N | provider.provider_id | |
| care_site_id | BIGINT | Y | N | care_site.care_site_id | |
| visit_source_value | STRING | Y | N | | |
| visit_source_concept_id | INT | Y | N | concept.concept_id | |
| admitted_from_concept_id | INT | Y | N | concept.concept_id | Visit |
| admitted_from_source_value | STRING | Y | N | | |
| discharged_to_concept_id | INT | Y | N | concept.concept_id | Visit |
| discharged_to_source_value | STRING | Y | N | | |
| preceding_visit_occurrence_id | BIGINT | Y | N | visit_occurrence.visit_occurrence_id | |

## visit_detail

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| visit_detail_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| visit_detail_concept_id | INT | N | N | concept.concept_id | Visit |
| visit_detail_start_date | DATE | N | N | | |
| visit_detail_start_datetime | TIMESTAMP | Y | N | | |
| visit_detail_end_date | DATE | N | N | | |
| visit_detail_end_datetime | TIMESTAMP | Y | N | | |
| visit_detail_type_concept_id | INT | N | N | concept.concept_id | Type Concept |
| provider_id | BIGINT | Y | N | provider.provider_id | |
| care_site_id | BIGINT | Y | N | care_site.care_site_id | |
| visit_detail_source_value | STRING | Y | N | | |
| visit_detail_source_concept_id | INT | Y | N | concept.concept_id | |
| admitted_from_concept_id | INT | Y | N | concept.concept_id | Visit |
| admitted_from_source_value | STRING | Y | N | | |
| discharged_to_source_value | STRING | Y | N | | |
| discharged_to_concept_id | INT | Y | N | concept.concept_id | Visit |
| preceding_visit_detail_id | BIGINT | Y | N | visit_detail.visit_detail_id | |
| parent_visit_detail_id | BIGINT | Y | N | visit_detail.visit_detail_id | |
| visit_occurrence_id | BIGINT | N | N | visit_occurrence.visit_occurrence_id | |

## condition_occurrence

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| condition_occurrence_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| condition_concept_id | INT | N | N | concept.concept_id | Condition |
| condition_start_date | DATE | N | N | | |
| condition_start_datetime | TIMESTAMP | Y | N | | |
| condition_end_date | DATE | Y | N | | |
| condition_end_datetime | TIMESTAMP | Y | N | | |
| condition_type_concept_id | INT | N | N | concept.concept_id | Type Concept |
| condition_status_concept_id | INT | Y | N | concept.concept_id | Condition Status |
| stop_reason | STRING | Y | N | | |
| provider_id | BIGINT | Y | N | provider.provider_id | |
| visit_occurrence_id | BIGINT | Y | N | visit_occurrence.visit_occurrence_id | |
| visit_detail_id | BIGINT | Y | N | visit_detail.visit_detail_id | |
| condition_source_value | STRING | Y | N | | |
| condition_source_concept_id | INT | Y | N | concept.concept_id | |
| condition_status_source_value | STRING | Y | N | | |

## procedure_occurrence

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| procedure_occurrence_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| procedure_concept_id | INT | N | N | concept.concept_id | Procedure |
| procedure_date | DATE | N | N | | |
| procedure_datetime | TIMESTAMP | Y | N | | |
| procedure_end_date | DATE | Y | N | | |
| procedure_end_datetime | TIMESTAMP | Y | N | | |
| procedure_type_concept_id | INT | N | N | concept.concept_id | Type Concept |
| modifier_concept_id | INT | Y | N | concept.concept_id | |
| quantity | INT | Y | N | | |
| provider_id | BIGINT | Y | N | provider.provider_id | |
| visit_occurrence_id | BIGINT | Y | N | visit_occurrence.visit_occurrence_id | |
| visit_detail_id | BIGINT | Y | N | visit_detail.visit_detail_id | |
| procedure_source_value | STRING | Y | N | | |
| procedure_source_concept_id | INT | Y | N | concept.concept_id | |
| modifier_source_value | STRING | Y | N | | |

## drug_exposure

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| drug_exposure_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| drug_concept_id | INT | N | N | concept.concept_id | Drug |
| drug_exposure_start_date | DATE | N | N | | |
| drug_exposure_start_datetime | TIMESTAMP | Y | N | | |
| drug_exposure_end_date | DATE | N | N | | |
| drug_exposure_end_datetime | TIMESTAMP | Y | N | | |
| verbatim_end_date | DATE | Y | N | | |
| drug_type_concept_id | INT | N | N | concept.concept_id | Type Concept |
| stop_reason | STRING | Y | N | | |
| refills | INT | Y | N | | |
| quantity | FLOAT | Y | N | | |
| days_supply | INT | Y | N | | |
| sig | STRING | Y | N | | |
| route_concept_id | INT | Y | N | concept.concept_id | Route |
| lot_number | STRING | Y | N | | |
| provider_id | BIGINT | Y | N | provider.provider_id | |
| visit_occurrence_id | BIGINT | Y | N | visit_occurrence.visit_occurrence_id | |
| visit_detail_id | BIGINT | Y | N | visit_detail.visit_detail_id | |
| drug_source_value | STRING | Y | N | | |
| drug_source_concept_id | INT | Y | N | concept.concept_id | |
| route_source_value | STRING | Y | N | | |
| dose_unit_source_value | STRING | Y | N | | |

## device_exposure

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| device_exposure_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| device_concept_id | INT | N | N | concept.concept_id | Device |
| device_exposure_start_date | DATE | N | N | | |
| device_exposure_start_datetime | TIMESTAMP | Y | N | | |
| device_exposure_end_date | DATE | Y | N | | |
| device_exposure_end_datetime | TIMESTAMP | Y | N | | |
| device_type_concept_id | INT | N | N | concept.concept_id | Type Concept |
| unique_device_id | STRING | Y | N | | |
| production_id | STRING | Y | N | | |
| quantity | INT | Y | N | | |
| provider_id | BIGINT | Y | N | provider.provider_id | |
| visit_occurrence_id | BIGINT | Y | N | visit_occurrence.visit_occurrence_id | |
| visit_detail_id | BIGINT | Y | N | visit_detail.visit_detail_id | |
| device_source_value | STRING | Y | N | | |
| device_source_concept_id | INT | Y | N | concept.concept_id | |
| unit_concept_id | INT | Y | N | concept.concept_id | Unit |
| unit_source_value | STRING | Y | N | | |
| unit_source_concept_id | INT | Y | N | concept.concept_id | |

## measurement

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| measurement_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| measurement_concept_id | INT | N | N | concept.concept_id | Measurement |
| measurement_date | DATE | N | N | | |
| measurement_datetime | TIMESTAMP | Y | N | | |
| measurement_time | STRING | Y | N | | |
| measurement_type_concept_id | INT | N | N | concept.concept_id | Type Concept |
| operator_concept_id | INT | Y | N | concept.concept_id | Meas Value Operator |
| value_as_number | FLOAT | Y | N | | |
| value_as_concept_id | INT | Y | N | concept.concept_id | Meas Value |
| unit_concept_id | INT | Y | N | concept.concept_id | Unit |
| range_low | FLOAT | Y | N | | |
| range_high | FLOAT | Y | N | | |
| provider_id | BIGINT | Y | N | provider.provider_id | |
| visit_occurrence_id | BIGINT | Y | N | visit_occurrence.visit_occurrence_id | |
| visit_detail_id | BIGINT | Y | N | visit_detail.visit_detail_id | |
| measurement_source_value | STRING | Y | N | | |
| measurement_source_concept_id | INT | Y | N | concept.concept_id | |
| unit_source_value | STRING | Y | N | | |
| unit_source_concept_id | INT | Y | N | concept.concept_id | |
| value_source_value | STRING | Y | N | | |
| measurement_event_id | BIGINT | Y | N | | |
| meas_event_field_concept_id | INT | Y | N | concept.concept_id | |

## observation

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| observation_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| observation_concept_id | INT | N | N | concept.concept_id | |
| observation_date | DATE | N | N | | |
| observation_datetime | TIMESTAMP | Y | N | | |
| observation_type_concept_id | INT | N | N | concept.concept_id | Type Concept |
| value_as_number | FLOAT | Y | N | | |
| value_as_string | STRING | Y | N | | |
| value_as_concept_id | INT | Y | N | concept.concept_id | |
| qualifier_concept_id | INT | Y | N | concept.concept_id | |
| unit_concept_id | INT | Y | N | concept.concept_id | Unit |
| provider_id | BIGINT | Y | N | provider.provider_id | |
| visit_occurrence_id | BIGINT | Y | N | visit_occurrence.visit_occurrence_id | |
| visit_detail_id | BIGINT | Y | N | visit_detail.visit_detail_id | |
| observation_source_value | STRING | Y | N | | |
| observation_source_concept_id | INT | Y | N | concept.concept_id | |
| unit_source_value | STRING | Y | N | | |
| qualifier_source_value | STRING | Y | N | | |
| observation_event_id | BIGINT | Y | N | | |
| obs_event_field_concept_id | INT | Y | N | concept.concept_id | |

## death

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| person_id | BIGINT | N | Y | person.person_id | |
| death_date | DATE | N | N | | |
| death_datetime | TIMESTAMP | Y | N | | |
| death_type_concept_id | INT | Y | N | concept.concept_id | Type Concept |
| cause_concept_id | INT | Y | N | concept.concept_id | |
| cause_source_value | STRING | Y | N | | |
| cause_source_concept_id | INT | Y | N | concept.concept_id | |

## note

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| note_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| note_date | DATE | N | N | | |
| note_datetime | TIMESTAMP | Y | N | | |
| note_type_concept_id | INT | N | N | concept.concept_id | Type Concept |
| note_class_concept_id | INT | N | N | concept.concept_id | |
| note_title | STRING | Y | N | | |
| note_text | STRING | N | N | | |
| encoding_concept_id | INT | N | N | concept.concept_id | Metadata |
| language_concept_id | INT | N | N | concept.concept_id | Language |
| provider_id | BIGINT | Y | N | provider.provider_id | |
| visit_occurrence_id | BIGINT | Y | N | visit_occurrence.visit_occurrence_id | |
| visit_detail_id | BIGINT | Y | N | visit_detail.visit_detail_id | |
| note_source_value | STRING | Y | N | | |
| note_event_id | BIGINT | Y | N | | |
| note_event_field_concept_id | INT | Y | N | concept.concept_id | |

## note_nlp

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| note_nlp_id | BIGINT | N | Y | | |
| note_id | BIGINT | N | N | | |
| section_concept_id | INT | Y | N | concept.concept_id | |
| snippet | STRING | Y | N | | |
| offset | STRING | Y | N | | |
| lexical_variant | STRING | N | N | | |
| note_nlp_concept_id | INT | Y | N | concept.concept_id | |
| note_nlp_source_concept_id | INT | Y | N | concept.concept_id | |
| nlp_system | STRING | Y | N | | |
| nlp_date | DATE | N | N | | |
| nlp_datetime | TIMESTAMP | Y | N | | |
| term_exists | STRING | Y | N | | |
| term_temporal | STRING | Y | N | | |
| term_modifiers | STRING | Y | N | | |

## specimen

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| specimen_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| specimen_concept_id | INT | N | N | concept.concept_id | Specimen |
| specimen_type_concept_id | INT | N | N | concept.concept_id | Type Concept |
| specimen_date | DATE | N | N | | |
| specimen_datetime | TIMESTAMP | Y | N | | |
| quantity | FLOAT | Y | N | | |
| unit_concept_id | INT | Y | N | concept.concept_id | Unit |
| anatomic_site_concept_id | INT | Y | N | concept.concept_id | Spec Anatomic Site |
| disease_status_concept_id | INT | Y | N | concept.concept_id | |
| specimen_source_id | STRING | Y | N | | |
| specimen_source_value | STRING | Y | N | | |
| unit_source_value | STRING | Y | N | | |
| anatomic_site_source_value | STRING | Y | N | | |
| disease_status_source_value | STRING | Y | N | | |

## location

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| location_id | BIGINT | N | Y | | |
| address_1 | STRING | Y | N | | |
| address_2 | STRING | Y | N | | |
| city | STRING | Y | N | | |
| state | STRING | Y | N | | |
| zip | STRING | Y | N | | |
| county | STRING | Y | N | | |
| location_source_value | STRING | Y | N | | |
| country_concept_id | INT | Y | N | concept.concept_id | Geography |
| country_source_value | STRING | Y | N | | |
| latitude | FLOAT | Y | N | | |
| longitude | FLOAT | Y | N | | |

## care_site

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| care_site_id | BIGINT | N | Y | | |
| care_site_name | STRING | Y | N | | |
| place_of_service_concept_id | INT | Y | N | concept.concept_id | Visit |
| location_id | BIGINT | Y | N | location.location_id | |
| care_site_source_value | STRING | Y | N | | |
| place_of_service_source_value | STRING | Y | N | | |

## provider

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| provider_id | BIGINT | N | Y | | |
| provider_name | STRING | Y | N | | |
| npi | STRING | Y | N | | |
| dea | STRING | Y | N | | |
| specialty_concept_id | INT | Y | N | concept.concept_id | Provider |
| care_site_id | BIGINT | Y | N | care_site.care_site_id | |
| year_of_birth | INT | Y | N | | |
| gender_concept_id | INT | Y | N | concept.concept_id | Gender |
| provider_source_value | STRING | Y | N | | |
| specialty_source_value | STRING | Y | N | | |
| specialty_source_concept_id | INT | Y | N | concept.concept_id | |
| gender_source_value | STRING | Y | N | | |
| gender_source_concept_id | INT | Y | N | concept.concept_id | |

## condition_era

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| condition_era_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| condition_concept_id | INT | N | N | concept.concept_id | Condition |
| condition_era_start_date | DATE | N | N | | |
| condition_era_end_date | DATE | N | N | | |
| condition_occurrence_count | INT | Y | N | | |

## drug_era

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| drug_era_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| drug_concept_id | INT | N | N | concept.concept_id | Drug |
| drug_era_start_date | DATE | N | N | | |
| drug_era_end_date | DATE | N | N | | |
| drug_exposure_count | INT | Y | N | | |
| gap_days | INT | Y | N | | |

## dose_era

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| dose_era_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| drug_concept_id | INT | N | N | concept.concept_id | Drug |
| unit_concept_id | INT | N | N | concept.concept_id | Unit |
| dose_value | FLOAT | N | N | | |
| dose_era_start_date | DATE | N | N | | |
| dose_era_end_date | DATE | N | N | | |
