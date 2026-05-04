# Changelog

This changelog tracks `omop-pipeline-builder` skill behavior changes that affect how `validate_omop.py` evaluates your data. After upgrading the skill, review entries newer than your previous skill version — some changes may surface findings on data that previously passed validation.

## v2.0.6 (2026-05-04)

### Validator output changes

**PASS Summary enriched** (UX-004).
The validator's terminal Summary line now surfaces per-layer pass counts (columns matched, PK status, concept_id columns checked, domain mismatches, NOT NULL violations), total rows checked, and (on FAIL) the names of which layers failed. Backward-grep compatible: scripts that grep `Summary: OK` or `Summary: FAILED` continue to work, and the prior single-line markers still appear inside the new multi-line block.

This is an output-shape change, not a data-behavior change — the validator evaluates your data the same way; only the Summary block's structure changed. If you parse Summary output programmatically, treat the change as additive: new lines appear after the existing `Summary: OK` / `Summary: FAILED` line.

**FAIL trailer to runbook** (UX-002 + UX-006).
When the validator fails, output now points to `docs/omop-runbook.md` Section 8 troubleshooting tables. Same trailer shape on the standalone Pydantic config validator (`validate_yaml_schema.py`) when it raises `ValidationError`. Backward-compatible additive — no existing message text changes; the trailer prints only when failures > 0.

**`table_missing` message names AD-001 framing** (UX-001).
When the validator hits a table that doesn't exist in your catalog, the message now distinguishes three cases: (1) BYO-ETL tables — `device_exposure`, `note`, `note_nlp`, `specimen`, `visit_detail`, `dose_era` — which the skill validates but does not build (see `omop-runbook.md` Section 7.5 'BYO-ETL: validation-only tables'); (2) buildable tables that this pipeline run did not yet build; (3) typos in your validator invocation. Backward-grep anchors preserved: `does not exist in catalog`, `Subsequent layers will skip`, and `FAIL: table` still match.

### Build error changes

**`BuildScopeError` replaces raw `KeyError`**.
DAG lookups in `scripts/_omop_dag.py` against non-buildable tables now raise a typed `BuildScopeError` exception with an AD-001-framed message naming the BYO-ETL tables and the validation-vs-build distinction. `BuildScopeError` is a subclass of `KeyError`, so existing `except KeyError` callers continue to catch; new callers can `except BuildScopeError` to distinguish the BYO-ETL case from other key errors.

### Documentation additions

- New runbook Section 7.5 'BYO-ETL: validation-only tables' (`docs/omop-runbook.md`) documents the validate-20-build-14 architectural decision and customer-side ETL patterns for the 6 BYO-ETL tables — Lakeflow Connect for `device_exposure`, customer NLP pipeline for `note`/`note_nlp`, custom ETL for `specimen`/`visit_detail`/`dose_era`.
- Generated `README.md` (rendered into your project tree by the scaffolder) gains a 'Validation scope vs build scope' section.
- The skill's `references/recommended_ci_config.md` gains a 'Differential post-deploy validation' subsection covering the buildable vs BYO-ETL split for CI's post-deploy validator step.
- `silver` / `core` glossary added to `omop-runbook.md` Appendix D and to the skill's `scripts/bundle_state.py` module docstring, naming the dual vocabulary as synonyms with rationale (medallion-architecture vs OHDSI naming, both legitimate).

### What didn't change

- The 14-buildable / 20-validatable scope split (AD-001) is unchanged. The same 14 tables build; the same 6 BYO-ETL tables remain validation-only.
- The 5-layer validator structure is unchanged. Layer 1-5 evaluate your data the same way as v2.0.5; only the Summary output's shape changed.
- The Pydantic config schema (`validate_yaml_schema.py`) is unchanged — your existing `configs/<table>.yaml` files validate identically; only the FAIL output adds a trailer.

For the full v2.0.6 cycle context, see the skill repo's `BACKLOG.md` and `SESSION-STATE.md` v2.0.6 entry.

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
