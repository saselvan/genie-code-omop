#!/usr/bin/env python3
"""Standalone OMOP YAML config validator (Pydantic schema only).

This script embeds a copy of the Pydantic models from ``src/config_loader.py``
so it can be run from anywhere (Genie Code Agent, local CLI, CI) without
requiring the host repo on ``sys.path`` or pyspark to be installed.

Two surfaces:
  1. CLI:    python validate_yaml_schema.py <path-to-config.yaml>
             Exit 0 on valid; exit 1 with formatted errors on invalid.
  2. Python: from validate_yaml_schema import validate
             validate(path) -> None on valid; raises pydantic.ValidationError on invalid.
             validate_text(yaml_text) -> None for in-memory YAML strings.

Drift contract: tests/test_validate_yaml_schema.py runs both this validator AND
``src.config_loader.load_config`` against the same fixtures and asserts identical
pass/fail outcomes per resolution strategy. If you change ``src/config_loader.py``,
you MUST update the embedded copy below and re-run the drift test.

Dependencies: pydantic, pyyaml. NO pyspark — that's intentional; this validator
must run in environments where pyspark isn't installed (e.g. agent sandboxes).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


# ---------------------------------------------------------------------------
# Pydantic schema — DRIFT-SENSITIVE COPY of src/config_loader.py.
# Keep these classes byte-for-byte (modulo imports) in sync with the host repo.
# tests/test_validate_yaml_schema.py is the regression boundary.
# ---------------------------------------------------------------------------


class Source(BaseModel):
    """Bronze table source definition."""

    model_config = ConfigDict(extra="forbid")

    alias: str
    table: str


class Join(BaseModel):
    """Join between two source aliases; ``condition`` is a SQL boolean expression using those aliases.

    Note: the YAML key is ``condition`` (not ``on``) because YAML 1.1 parses bare ``on`` as boolean True.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    left: str
    right: str
    type: str = Field(
        default="inner",
        json_schema_extra={"description": "Spark join type, e.g. left, inner, right, full, left_outer."},
    )
    condition: str = Field(
        validation_alias="condition",
        json_schema_extra={"description": "SQL boolean expression for the join (use source aliases)."},
    )


class VocabularyLookup(BaseModel):
    """Concept resolution strategy.

    Field requirements depend on ``resolution``:
      - ``source_to_concept_map`` requires ``source_vocabulary_id``.
      - ``concept_table`` requires ``vocabulary_id`` and ``domain_id``.
      - ``concept_table_mapped`` requires (``vocabulary_id`` or ``source_vocabulary_id``) AND ``domain_id``.
    """

    model_config = ConfigDict(extra="forbid")

    source_alias: str
    source_column: str
    target_column: str
    resolution: Literal["source_to_concept_map", "concept_table", "concept_table_mapped"] = Field(
        default="source_to_concept_map",
        json_schema_extra={
            "description": "source_to_concept_map: join reference.source_to_concept_map; "
            "concept_table: join reference.concept on vocabulary_id + domain_id; "
            "concept_table_mapped: concept_table + relationship crosswalk to standard concept."
        },
    )
    source_vocabulary_id: Optional[str] = Field(
        default=None,
        json_schema_extra={"description": "Required when resolution=source_to_concept_map."},
    )
    vocabulary_id: Optional[str] = Field(
        default=None,
        json_schema_extra={"description": "Required when resolution=concept_table or concept_table_mapped."},
    )
    domain_id: Optional[str] = Field(
        default=None,
        json_schema_extra={"description": "Required when resolution=concept_table. Used as preferred domain for one-to-many dedup in concept_table_mapped."},
    )
    relationship_id: str = Field(
        default="Maps to",
        json_schema_extra={"description": "Relationship to traverse in concept_table_mapped. Default 'Maps to'. Use 'Maps to value' or 'Maps to unit' for measurement."},
    )
    standard_only: bool = Field(
        default=True,
        json_schema_extra={"description": "If true, only return standard concepts (standard_concept='S'). Set false for *_source_concept_id columns."},
    )
    fallback: int = Field(default=0, json_schema_extra={"description": "concept_id when no match"})

    @model_validator(mode="after")
    def _check_required_fields_per_resolution(self) -> "VocabularyLookup":
        if self.resolution == "source_to_concept_map":
            if not self.source_vocabulary_id:
                raise ValueError(
                    f"VocabularyLookup target={self.target_column}: "
                    "resolution=source_to_concept_map requires source_vocabulary_id."
                )
        elif self.resolution == "concept_table":
            if not self.vocabulary_id or not self.domain_id:
                raise ValueError(
                    f"VocabularyLookup target={self.target_column}: "
                    "resolution=concept_table requires both vocabulary_id and domain_id."
                )
        elif self.resolution == "concept_table_mapped":
            if not (self.vocabulary_id or self.source_vocabulary_id):
                raise ValueError(
                    f"VocabularyLookup target={self.target_column}: "
                    "resolution=concept_table_mapped requires vocabulary_id or source_vocabulary_id."
                )
            if not self.domain_id:
                raise ValueError(
                    f"VocabularyLookup target={self.target_column}: "
                    "resolution=concept_table_mapped requires domain_id to filter one-to-many "
                    "mappings to the correct OMOP table domain."
                )
        return self


class SourceVocabulary(BaseModel):
    """Generator-input metadata: which OHDSI vocabulary a customer's source codes belong to.

    Drives the v2.0.7+ source-to-concept mapping draft generator
    (``scripts/generate_source_concept_map.py``). Distinct from ``VocabularyLookup`` —
    that class governs the runtime resolver's behavior; this class tells the offline
    draft generator the source vocabulary per (source_alias, source_column).

    Customer configs may have a column appear in both ``vocabulary_lookups`` and
    ``source_vocabulary`` for the same (source_alias, source_column). Values typically
    match (``vocabulary_lookups[].source_vocabulary_id`` == ``source_vocabulary[].vocabulary_id``)
    but the two sections are semantically separate; equality is not enforced.

    The section is optional; pre-v2.0.7 configs without it validate clean.
    """

    model_config = ConfigDict(extra="forbid")

    source_alias: str = Field(
        json_schema_extra={"description": "Source row the codes live on (matches sources[].alias)."},
    )
    source_column: str = Field(
        json_schema_extra={"description": "Column on that source carrying the raw codes."},
    )
    vocabulary_id: str = Field(
        json_schema_extra={
            "description": "OHDSI vocabulary the codes belong to (e.g. ICD10CM, SNOMED, RxNorm, "
            "LOINC, CPT4, HCPCS, NDC). Permissive string; the generator's coverage report "
            "surfaces unrecognized vocabularies."
        },
    )


class ColumnMapping(BaseModel):
    """Single target column from a SQL expression."""

    model_config = ConfigDict(extra="forbid")

    target: str
    expr: str


class Expectation(BaseModel):
    """Named SDP expectation rule (SQL predicate)."""

    model_config = ConfigDict(extra="forbid")

    name: str
    expr: str


class Expectations(BaseModel):
    """Grouped expectations for fail / drop / warn per SDP API."""

    model_config = ConfigDict(extra="forbid")

    fail: list[Expectation] = Field(default_factory=list)
    drop: list[Expectation] = Field(default_factory=list)
    warn: list[Expectation] = Field(default_factory=list)


class OMOPConfig(BaseModel):
    """Full OMOP table transform config matching configs/*.yaml schema."""

    model_config = ConfigDict(extra="forbid")

    table_name: str
    target_schema: str
    description: str
    sources: list[Source]
    joins: list[Join] = Field(default_factory=list)
    vocabulary_lookups: list[VocabularyLookup] = Field(default_factory=list)
    source_vocabulary: list[SourceVocabulary] = Field(default_factory=list)
    column_mappings: list[ColumnMapping]
    expectations: Expectations = Field(default_factory=Expectations)


# ---------------------------------------------------------------------------
# Public surfaces.
# ---------------------------------------------------------------------------


def validate_text(yaml_text: str) -> OMOPConfig:
    """Parse YAML text and validate against the OMOP config schema.

    Raises pydantic.ValidationError on invalid input; returns the parsed
    OMOPConfig on success.
    """
    raw = yaml.safe_load(yaml_text) or {}
    return OMOPConfig.model_validate(raw)


def validate(path: str | Path) -> OMOPConfig:
    """Validate a YAML config at ``path`` against the OMOP schema.

    Use this from Genie Code Agent's executeCode python kernel:
        import sys
        sys.path.insert(0, "/Workspace/.assistant/skills/omop-pipeline-builder/scripts")
        from validate_yaml_schema import validate
        validate("/Workspace/Users/<you>/configs/person.yaml")
    """
    p = Path(path).expanduser()
    if not p.exists():
        raise FileNotFoundError(f"YAML config not found: {p}")
    return validate_text(p.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------


def _format_errors(err: ValidationError) -> str:
    """Render a ValidationError as a numbered list, one error per line."""
    lines: list[str] = []
    for i, e in enumerate(err.errors(), start=1):
        loc = ".".join(str(x) for x in e.get("loc", ()))
        msg = e.get("msg", "")
        typ = e.get("type", "")
        lines.append(f"  {i}. [{typ}] {loc}: {msg}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate an OMOP YAML config against the embedded Pydantic schema."
    )
    parser.add_argument("path", help="Path to the YAML config file")
    args = parser.parse_args()

    try:
        cfg = validate(args.path)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    except ValidationError as e:
        print(
            f"INVALID: {args.path}\n"
            f"{len(e.errors())} validation error(s):\n{_format_errors(e)}",
            file=sys.stderr,
        )
        print(
            "\nSee docs/omop-runbook.md Section 8 'Config Validation Errors "
            "(Pydantic)' for common fixes per error type.",
            file=sys.stderr,
        )
        return 1
    except Exception as e:  # pragma: no cover — unexpected
        print(f"UNEXPECTED ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 3

    print(
        f"OK: {args.path} validated as {cfg.table_name} "
        f"(sources={len(cfg.sources)} joins={len(cfg.joins)} "
        f"lookups={len(cfg.vocabulary_lookups)} source_vocabs={len(cfg.source_vocabulary)} "
        f"columns={len(cfg.column_mappings)})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
