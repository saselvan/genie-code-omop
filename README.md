# omop-pipeline-builder

A Databricks Genie Code skill for building OMOP CDM v5.4 pipelines on top of an existing Databricks clinical lakehouse. Generates per-table YAML transform configs through a conversational flow, scaffolds a working Databricks Asset Bundle project, and ships a five-layer OHDSI conformance validator.

## What this is

`omop-pipeline-builder` turns "we have an EHR-derived bronze landing zone in Unity Catalog and we need an OMOP CDM v5.4 silver layer" into a working Databricks Asset Bundle project. The skill scaffolds the project tree, drafts per-table YAML configs through a conversational flow that queries your Unity Catalog directly, validates each draft against a Pydantic schema, and writes the validated config atomically. Five validation layers (schema, primary keys, concept FK integrity, domain conformance, NOT NULL completeness) check materialized OMOP tables against the OHDSI v5.4 spec.

The skill validates 20 OMOP tables and auto-builds 14. The remaining 6 tables (`device_exposure`, `note`, `note_nlp`, `specimen`, `visit_detail`, `dose_era`) are bring-your-own-ETL — the skill validates them against the spec when you populate them, but the build templates focus on the 14-table core that matches a typical from-EHR-bronze pipeline. See `templates/project_scaffold/docs/omop-runbook.md` Section 7.5 for the rationale and BYO-ETL loading patterns.

## Installation

Drop the repo contents into your Databricks workspace under `.assistant/skills/omop-pipeline-builder/`. Genie Code Agent will discover the skill on next launch.

```bash
cd /Workspace/.assistant/skills
git clone https://github.com/saselvan/genie-code-omop.git omop-pipeline-builder
```

**Requirements:** Databricks workspace with Unity Catalog; notebook with a Python-capable cluster (the skill cannot run on a SQL warehouse — Step 6 uses Pydantic); Python 3.11+ with `databricks-sdk`, `pyyaml`, `pydantic`; read access to an OHDSI vocabulary `concept` table in UC; bronze-layer source tables already landed in UC. See `SKILL.md` "Compute requirements" for the full launch-surface compatibility matrix.

## Usage

Open a notebook, attach a Python-capable cluster, launch Genie Code Agent, and tell the agent what to build:

> Build OMOP person from my bronze patient table at `mycat.bronze_clinical.patient`. Vocabulary reference is `mycat.reference.concept`. Volume for configs: `mycat.raw.omop_artifacts`.

The agent runs the scaffolder (first time only), generates `configs/person.yaml` from your bronze table's schema, validates it, writes it to your project tree, and tells you what's next. Repeat per table.

## What gets generated

`databricks.yml` (DAB bundle config), `resources/jobs.yml` (workflow DAG), `resources/pipeline_generic.yml` (Spark Declarative Pipeline templates), `src/` pipeline source, `configs/_schema.yaml` (Pydantic schema), `seed_data/` template for institution-specific code mappings, `docs/omop-runbook.md` (your project's runbook), `docs/CHANGELOG.md` (skill version log), `tests/` (Pydantic-schema CI gate for your generated configs), and a `README.md` with a next-steps walkthrough. Per-table configs land in `configs/<table>.yaml`. The agent never overwrites silently — if a config exists for the table you ask about, it surfaces three sub-paths (Update / Replace / Different table) and waits for your choice.

## Validation

| Layer | Checks |
|---|---|
| L1 Schema | Columns present, types match the OHDSI v5.4 spec |
| L2 PK | Primary key uniqueness |
| L3 Concept FK | `*_concept_id` columns resolve in `reference.concept` |
| L4 Domain | Resolved concepts match the spec's expected domain |
| L5 Completeness | NOT NULL columns are not null |

CLI: `python scripts/validate_omop.py --table mycat.core_omop.person`. Notebook: `templates/project_scaffold/src/99_validate_omop_output.py` iterates all 20 spec-covered tables in one run. For CI integration, see `references/recommended_ci_config.md`.

## Architecture decisions

The skill validates 20 OMOP tables but auto-builds only 14. See `templates/project_scaffold/docs/omop-runbook.md` Section 7.5 "BYO-ETL: validation-only tables" and `SKILL.md` "Validation scope vs build scope" for the rationale and BYO-ETL loading patterns for the 6 validation-only tables.

## Versioning and changes

Releases follow semver-ish (`major.minor.patch`). See [`CHANGELOG.md`](CHANGELOG.md) for per-release behavior changes — some upgrades surface findings on data that previously passed `validate_omop.py`.

## Support and contributions

This skill is maintained by Samuel Selvan. Issues are tracked in this repo's GitHub issues. PRs are welcome but not all will be accepted; the skill's design and roadmap reflect the maintainer's HLS architecture experience and prioritization.

## License

MIT — see [LICENSE](LICENSE).
