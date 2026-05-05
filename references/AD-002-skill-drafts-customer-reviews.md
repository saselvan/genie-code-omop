# AD-002 — Skill drafts; customer reviews (handoff pattern)

This skill generates artifacts that customers apply to their production OMOP build. The skill never auto-applies a generated artifact to customer-controlled production state without an explicit customer review step. This is **architectural decision AD-002** — the skill-drafts / customer-reviews handoff pattern.

AD-002 sits alongside AD-001 (validate-20-build-14, documented inline in `SKILL.md` under "Validation scope vs build scope") as a foundational skill design principle. AD-001 scopes *what the skill builds*; AD-002 scopes *how the skill hands its output to the customer*.

## What "drafts" means

A drafted artifact has three properties:

1. **Provenance is preserved.** Every generated row, file, or config carries enough metadata for a reviewer to trace which generator run produced it. The `source_to_concept_map` draft generator (`scripts/generate_source_concept_map.py`) writes a uniform `valid_start_date` to every row in a single run; the per-table YAML configs land at writeable paths the customer owns; the 5-layer validator's `Finding` dataclass carries layer, status, check, and message for every finding.
2. **The customer-controlled application step is separate from the draft step.** Drafted STCM rows land in a CSV the customer inspects before `01_load_vocabulary.py` MERGEs them into the UC `source_to_concept_map` Delta table. Drafted YAML configs land in `configs/` for the customer to review before `./deploy.sh` syncs them to a UC Volume. The skill never closes the loop on its own output.
3. **Unresolved cases are explicit, not silent.** When the skill cannot draft a confident answer, it produces an explicit "this needs review" marker rather than guessing. The STCM draft generator's coverage report separates resolution into five mutually exclusive buckets (see `scripts/_concept_resolver.py::VocabularyCoverage`); rows in `unresolved_no_concept`, `unresolved_no_maps_to`, and `unresolved_ambiguous` carry `target_concept_id = 0` and surface in the coverage report's "Sample unmapped codes" section. Customers can scope manual review work directly from the report rather than diff-grepping the CSV.

## What "reviews" means

The customer-side review surface is owned by a domain expert — typically a clinical informaticist, terminologist, OMOP architect, or data engineer with OHDSI vocabulary fluency. The review step is non-trivial:

- For drafted STCM rows: confirm that resolved `target_concept_id` values are clinically appropriate for the source code's actual usage in the customer's data. Spot-check resolved rows; manually map unresolved rows. The OHDSI Athena vocabulary browser (https://athena.ohdsi.org) is the canonical lookup surface.
- For drafted YAML configs: confirm column-level mappings, vocabulary lookups, and join logic match the customer's bronze schema and clinical conventions. The Pydantic validator (`config_loader.py`) catches structural errors but cannot catch semantic mis-mappings.
- For 5-layer validator findings: triage which findings are bronze-data-quality issues (fix upstream), which are config issues (fix the YAML and re-deploy), and which represent expected-but-imperfect coverage (mark with `warn` expectations rather than `drop`).

The skill makes the review tractable by surfacing findings in human-readable form (markdown coverage report, structured `Finding` dataclass for DataFrame display, layered validator output with cross-references to the runbook). It does not make the review optional.

## Why this split

Three reasons drive the AD-002 contract:

**Domain expertise cannot be delegated to a model.** OHDSI vocabulary semantics have edge cases that model-driven generation handles inconsistently: one-to-many Maps-to chains across multiple domains, deprecated concepts that ought to map to their replacements, vocabulary version drift where a code's standard concept changes between Athena releases, license-gated vocabularies (CPT4) where the source data exists but the concept lookup is empty, and customer-specific local codes that legitimately belong outside OHDSI vocabularies. Domain experts handle these cases via judgment rooted in clinical context; model-driven heuristics produce silently-wrong mappings that surface only as Layer 4 (Domain) validator findings months later.

**Agentic systems can hallucinate, and HLS data carries compliance constraints that hallucination violates.** Generated YAML configs that look syntactically valid can encode wrong vocabulary lookups; drafted STCM rows that match by code-string can map to clinically inappropriate standard concepts. HIPAA, HITECH, and customer-internal data governance impose review requirements on derived clinical data that an agentic skill cannot satisfy on the customer's behalf — the customer's compliance posture requires a human in the loop. AD-002 makes that loop explicit rather than implicit.

**The skill's contract is solution-accelerator-shaped, not managed-product-shaped.** This skill is a publicly-distributed solution accelerator (`github.com/saselvan/genie-code-omop`); after the customer scaffolds a project, they own the generated code. There is no skill-side runtime that re-applies generated artifacts on the customer's behalf. AD-002 makes that ownership boundary first-class: the skill drafts on the customer's review surface; the customer's review-and-deploy decision is the boundary between skill output and customer production state.

## Where the pattern manifests

AD-002 is visible in these surfaces (the list is illustrative, not exhaustive):

- **Source-to-concept mapping draft** (`scripts/generate_source_concept_map.py`, v2.0.7+) — drafts STCM rows from per-table YAML configs by traversing OHDSI's `Maps to` chain (single-hop, Approach 1). Output is a CSV with explicit `target_concept_id = 0` markers on unresolved rows; coverage report (`reports/source_mapping_coverage_<timestamp>.md`) scopes manual review work via the five-bucket resolution breakdown. The `01_load_vocabulary.py` MERGE step is customer-triggered, not generator-triggered.
- **Single-vocabulary STCM bootstrap** (`scripts/generate_source_mappings.py`) — earlier and narrower instance of the same pattern: scans one bronze column for one source vocabulary, drafts CSV rows, customer reviews and re-uploads. v2.0.7's multi-vocabulary generator extends this pattern to config-driven, multi-vocabulary generation; both follow the AD-002 handoff contract.
- **Per-table YAML configs** (`configs/<table>.yaml`) — agent-generated via the Genie Code conversational flow (see `SKILL.md` Step 5). Land at writeable paths the customer owns; Pydantic validation catches structural errors at draft time; semantic correctness is the customer's review responsibility before `./deploy.sh` syncs configs to the UC Volume.
- **5-layer validator** (`scripts/validate_omop.py`, `scripts/_omop_validator.py`) — surfaces structured findings without auto-mutating customer data. Layer 4 (Domain conformance) findings, in particular, often surface AD-002-relevant cases: drafted vocabulary mappings that match by code-string but resolve to a concept in the wrong domain.
- **DAG wiring** (`resources/jobs.yml`, `resources/pipeline_generic.yml`) — scaffolded with commented placeholders for non-Person tables; the customer uncomments per table after the per-table YAML config is reviewed and the pipeline run is verified. The skill does not auto-uncomment tasks based on config presence.
- **`source_to_concept_map_custom.csv` seed file** (`templates/project_scaffold/seed_data/`) — scaffolded with example rows the customer replaces with their own codes. The MERGE behavior in `01_load_vocabulary.py` is idempotent and re-runnable, but the customer is the trigger.

## Anti-patterns explicitly avoided

AD-002 means the skill *deliberately does not* implement these shapes, even when they would be technically straightforward:

- **Auto-MERGE of drafted STCM rows into the Delta table at generator-run time.** The generator writes CSV; `01_load_vocabulary.py` MERGEs the CSV. The two are decoupled so the customer can inspect the CSV (and the coverage report) before MERGE. Generating-and-MERGEing in one shot would skip the review surface and silently load model-drafted mappings into customer-of-record state.
- **Auto-resolution of ambiguous Maps-to chains via heuristics.** When a non-standard concept's Maps-to chain has multiple standard targets, or when the chain is multi-hop (target is itself non-standard), v2.0.7's resolver surfaces the case as `unresolved_ambiguous` with `target_concept_id = 0` rather than picking a target via heuristics like "lowest concept_id" or "most recent valid_start_date". Heuristic disambiguation produces silently-wrong mappings; explicit unresolved markers make the review work tractable.
- **Auto-deployment of regenerated configs.** Re-running the conversational generation flow against an existing per-table config produces a draft replacement; the customer reviews the diff and decides to deploy. The skill does not implement a "regenerate-and-deploy" shortcut.

## Cross-references

- AD-001 (inline in `SKILL.md` under "Validation scope vs build scope") — validate-20-build-14 architectural decision.
- `scripts/_concept_resolver.py::VocabularyCoverage` — the five-bucket resolution data structure that makes the unresolved-vs-resolved distinction first-class in the coverage report.
- `scripts/_coverage_report.py::render_markdown_text` — the report renderer that surfaces unresolved buckets in customer-facing markdown.
- `templates/project_scaffold/docs/omop-runbook.md` Section 6.3 ("Drafting source_to_concept_map at scale") — customer-facing workflow that walks through the draft → review → MERGE handoff.
- `references/canonical_examples.md` — agent-side reference for the YAML draft surface; the configs the agent generates are themselves drafts the customer reviews.
- The cycle-by-cycle pre-flight discipline applied to skill-internal artifacts is itself an instance of AD-002: code drafts get reviewed, with explicit anti-finding mechanisms when reviewer judgment overrides generator output.
