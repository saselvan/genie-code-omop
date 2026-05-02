# OMOP Common Data Model v5.4 — Tables covered by this skill

This note is an **abridged** abridged summary of required shapes for validation and mapping. Canonical definitions, cardinality, and full optional column lists live in the OHDSI CDM documentation: [OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/cdm54.html).

Machine-readable tables below use: **Nullable** `N` = NOT NULL, `Y` = nullable. **PK** `Y` marks the primary key column. **FK** uses `concept.concept_id` or `person.person_id` style hints. **Domain** is used by `scripts/validate_omop.py` for domain conformance checks on `*_concept_id` columns.

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

## drug_exposure

| Column | Type | Nullable | PK | FK | Domain |
|--------|------|----------|----|----|--------|
| drug_exposure_id | BIGINT | N | Y | | |
| person_id | BIGINT | N | N | person.person_id | |
| drug_concept_id | INT | N | N | concept.concept_id | Drug |
| drug_exposure_start_date | DATE | N | N | | |
| drug_exposure_start_datetime | TIMESTAMP | Y | N | | |
| drug_exposure_end_date | DATE | Y | N | | |
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
