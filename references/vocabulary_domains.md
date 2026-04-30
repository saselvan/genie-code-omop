# Vocabulary and domain cheat sheet (OMOP CDM v5.4)

This reference summarizes how OMOP **domains**, **vocabularies**, and **`source_to_concept_map` (STCM)** fit together for common vocabulary joins. Canonical tables: `concept`, `vocabulary`, `domain`, `concept_relationship`, `concept_ancestor`, and optional STCM.

## Domain → typical `vocabulary_id` values

Domains are stored on `concept.domain_id`. Vocabularies are stored on `concept.vocabulary_id`. A domain can contain multiple vocabularies; standard concepts for an entity type are not always from a single vocabulary.

| Clinical use | OMOP domain (examples) | Common vocabulary_ids | Join pattern |
|--------------|------------------------|-------------------------|--------------|
| Problems / diagnoses | Condition | `SNOMED`, `ICD10CM`, `ICD9CM`, `ICD10` | Map source ICD codes through STCM or `concept` + `Maps to` relationships to a **standard** Condition concept. |
| Procedures | Procedure | `SNOMED`, `CPT4`, `HCPCS`, `ICD10PCS` | CPT4 often **license-gated** in Athena downloads — see note below. |
| Medications | Drug | `RxNorm`, `NDC`, `HCPCS` | NDC → RxNorm via relationships or drug staging tools; verify invalid/expired NDCs. |
| Labs / vitals | Measurement | `LOINC` | Local panel codes frequently need STCM rows until mapped to LOINC. |
| Visit kind | Visit | `Visit`, `UB04 Pt visit`, `CMS Place of Service` | Distinguish **visit kind** (`visit_concept_id`, Visit domain) from **visit record provenance** (`visit_type_concept_id`, Visit Type domain). |
| Record provenance | Type Concept | `Type Concept` | `condition_type_concept_id`, `drug_type_concept_id`, `visit_type_concept_id`, etc. |
| Gender / race / ethnicity | Gender, Race, Ethnicity | `Gender`, `Race`, `Ethnicity` | EHR source numeric/letter codes may require STCM (see `CONTEXT_ATOM` seed examples). |

## Non-trivial mapping flows

1. **Standard concept pipeline:** `source_code` in a known `vocabulary_id` → row in `concept` → use `concept_id` as the `*_concept_id` field when it is a **standard** concept per your ETL rules.
2. **Maps to / Maps from:** use `concept_relationship` when the source vocabulary entry is non-standard but links to a standard concept (common for ICD/SNOMED bridges).
3. **STCM:** when the hierarchy does not cover an org-specific code set, add rows to the **`source_to_concept_map`** Delta table in UC (`{catalog}.{ref_schema}.source_to_concept_map`) so the resolver can translate **source_code + source_vocabulary_id → target_concept_id**. The table is the runtime source of truth — the resolver does not read CSVs. Two supported write paths: direct SQL/MERGE (recommended for ongoing ops) or the git-tracked bootstrap CSV at `seed_data/source_to_concept_map_custom.csv` MERGEd by `src/01_load_vocabulary.py` (recommended for repo-shipped foundational mappings). See [SKILL.md → Adding source_to_concept_map mappings](../SKILL.md#adding-source_to_concept_map-mappings).

## CPT4 licensing note

**CPT4** content is **license-gated** in OHDSI Athena distributions. Bulk vocabulary loads may omit CPT4 until the CPT4 license key step is completed; `CONCEPT_CPT4.csv` may be missing or empty early on. Expect procedure validation gaps until CPT4 is fully loaded — prefer SNOMED/HCPCS where available and document interim behavior.

## Practical checklist for builders

- Confirm `vocabulary_id` filters match the **actual** codes in bronze (for example `ICD10CM` vs `ICD10`).
- Keep **0** as “unknown / uncoded” only where OMOP allows; validation scripts often exclude `0` from concept FK checks.
- For **Visit vs Visit Type**, never encode clinical visit category into `visit_type_concept_id` — that field is **provenance** (Type Concept / Visit Type), not clinical visit class.

## OHDSI reference

- CDM specification: [https://ohdsi.github.io/CommonDataModel/cdm54.html](https://ohdsi.github.io/CommonDataModel/cdm54.html)
- Vocabulary documentation and relationship semantics: [https://ohdsi.github.io/CommonDataModel/vocabulary.html](https://ohdsi.github.io/CommonDataModel/vocabulary.html) (see site navigation for the v5.4 vocabulary chapter).
