# Changelog

This changelog tracks `omop-pipeline-builder` skill behavior changes that affect how `validate_omop.py` evaluates your data. After upgrading the skill, review entries newer than your previous skill version — some changes may surface findings on data that previously passed validation.

## v2.0.5 (2026-05-03)

### Validator behavior changes

**`drug_exposure.drug_exposure_end_date` now NOT NULL** (DC-003).
OHDSI v5.4 specifies this column as Required=Yes; v2.0.4 silently passed customer data with NULL values in this column. v2.0.5's validator surfaces NULLs as Layer 5 (Completeness) findings.

If your data has NULL `drug_exposure_end_date` values, you have two OHDSI-conformant options:

- **Impute end dates from start + duration.** OHDSI's typical pattern for open-ended drug exposures: `drug_exposure_end_date = drug_exposure_start_date + days_supply`. Apply in your `column_mappings` for `drug_exposure.yaml`.
- **Fall back to the start date if duration is unknown.** OHDSI's documented fallback for ongoing exposures with no duration data: `drug_exposure_end_date = drug_exposure_start_date`. Document the imputation in your team's wiki so reviewers know the column reflects best-available data, not literal end events.

**`note.encoding_concept_id` Domain now `Metadata`** (DC-011).
v2.0.4's spec left this column's Domain blank; v2.0.5 encodes `Metadata` per the principle preamble (single-domain anchor in the OHDSI narrative — concept 32678 'UTF-8' resolves to Metadata domain via OHDSI WebAPI; concept 0 'No matching concept' also Metadata).

If your `note` data populates `encoding_concept_id` with concepts outside the Metadata domain, Layer 4 (Domain conformance) will surface them as findings. The expected values are concept 32678 (UTF-8) for UTF-8 encoded notes or concept 0 (no matching concept) for unknown encoding. `note` is one of the BYO-ETL tables (see `omop-runbook.md` Section 7.5); your NLP pipeline owns the encoding_concept_id assignment.

### Documentation additions

- `omop_cdm_v54_spec.md` gains an 'Encoding Principles' preamble documenting the spec authoring rule for Domain cells (when to populate vs leave blank).
- New file `spec_domain_decisions.md` (in the skill repo's `references/` directory) logs borderline-case Domain decisions for spec contributors.

### What didn't change

- The 14-buildable / 20-validatable scope split (architectural decision AD-001) is unchanged. The skill builds the same 14 tables it built in v2.0.4; the same 6 BYO-ETL tables remain validation-only.
- The 5-layer validator structure is unchanged (Layer 1 Schema, Layer 2 PK, Layer 3 FK concepts, Layer 4 Domain, Layer 5 NOT NULL). The behavior changes above are encoded in the spec data the validator reads, not in new validator logic.

For the full v2.0.5 cycle context, see the skill repo's `BACKLOG.md` and `SESSION-STATE.md` v2.0.5 entry.
