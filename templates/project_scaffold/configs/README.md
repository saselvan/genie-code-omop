# configs/

This directory holds per-table OMOP YAML configs. The skill's agent generates configs here as you build out OMOP tables — one YAML file per table you build.

Examples:
- `person.yaml` (after scaffolding the Person table)
- `visit_occurrence.yaml` (after scaffolding Visit Occurrence)
- `condition_occurrence.yaml` (after scaffolding Condition Occurrence)

Empty until the agent writes the first config. The schema for each YAML is defined in `_schema.yaml` (sibling file).
