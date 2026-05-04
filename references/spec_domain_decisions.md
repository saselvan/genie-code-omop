# Spec Domain Decisions Log

This log captures case-by-case reasoning for borderline domain calls in [`omop_cdm_v54_spec.md`](./omop_cdm_v54_spec.md). The encoding principle is documented in the spec's "Encoding Principles" preamble; this log handles the cases where the principle's application is genuinely ambiguous — cases where two contributors could reasonably reach different `Domain` or `FK` decisions reading the same OHDSI evidence, including cases where the finding is borderline because it sits in a category the preamble's named examples did not directly anticipate.

## How to use this log

When you encounter a spec row where two contributors could reasonably reach different `Domain` or `FK` decisions under the encoding principle, add a decision entry here with the full structured fields used below. If the borderline is the result of a new category the preamble's examples don't name (rather than ambiguity within an already-named category), say so explicitly in the entry's `Reasoning` field. A single new-category occurrence belongs in this log; the preamble's examples list earns a new named case once a second instance of the same shape appears in a future audit.

Do not litigate existing entries; if you believe a past decision was wrong, file a finding and triage it through the normal review process.

## Decisions

### note.note_class_concept_id

- **Column:** `note.note_class_concept_id`
- **OHDSI reference:** [cdm54.html#note](https://ohdsi.github.io/CommonDataModel/cdm54.html#note)
- **OHDSI table cell:** `integer / Yes / No / Yes / CONCEPT` — `FK Domain` cell BLANK.
- **OHDSI narrative:** "A Standard Concept Id representing the HL7 LOINC Document Type Vocabulary classification of the note." Narrative embeds an Athena URL with parameters `&conceptClass=Doc+Kind&conceptClass=Doc+Role&conceptClass=Doc+Setting&conceptClass=Doc+Subject+Matter&conceptClass=Doc+Type+of+Service&domain=Meas+Value`. Narrative also offers an alternative path: "concepts with the relationship 'Kind of (LOINC)' to 706391 (Note)."
- **Tension:** The Athena URL parameter `domain=Meas+Value` is a single-Domain anchor — but it appears only inside the URL, never in the prose narrative as a Domain string. The prose itself names "HL7 LOINC Document Type Vocabulary classification" (a vocabulary classification, not a Domain string). Two reasonable contributors could weight the URL evidence vs the prose evidence differently under the encoding principle, and reach different `Domain` decisions.
- **Decision:** `Domain` blank.
- **Reasoning:** This is a new category not directly named by the principle preamble's examples — the preamble's named cases cover narrative-passing-mention, polymorphic-FK, and explicit-table-cell patterns; it does not currently anticipate "Domain string appears only as a parameter inside a narrative-embedded URL." The principle's blank-because-ambiguous *spirit* applies: the URL parameter `domain=Meas+Value` is embedded inside an Athena query URL within the narrative rather than asserted as a Domain string in prose, and treating URL parameters as prose-equivalent is a stretch the preamble does not authorize. The conservative reading (blank) stays inside what OHDSI explicitly says in prose. Encoding `Meas Value` would also be defensible if URL parameters are treated as prose-equivalent — that is the borderline. If similar URL-parameter-without-prose-Domain-mention findings surface in future audits, the preamble's examples list may need a fifth named case ("Domain string appears only as a parameter inside a narrative-embedded Athena URL"); a single occurrence is not yet evidence for that broadening.
