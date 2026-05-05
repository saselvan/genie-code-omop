"""YAML-driven OMOP transform configuration: Pydantic models and Spark source/join helpers."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr as sql_expr

logger = logging.getLogger(__name__)


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
      - ``source_to_concept_map`` requires ``source_vocabulary_id`` (filters reference.source_to_concept_map).
      - ``concept_table`` requires ``vocabulary_id`` and ``domain_id`` (filters reference.concept).
      - ``concept_table_mapped`` requires ``vocabulary_id`` or ``source_vocabulary_id`` — matches
        source concept_code then traverses a relationship (default "Maps to") to return the
        standard concept_id. Handles one-to-many by preferring the target domain and lowest
        concept_id. Filters out deprecated/invalid concepts.
        Use for non-standard vocabs (ICD10CM, CPT4, NDC, ICD10PCS) → standard (SNOMED, RxNorm).
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


def _read_yaml_text(yaml_path: str | Path) -> str:
    """Read YAML file contents, handling both local paths and UC Volume paths.

    On Databricks, UC Volume paths (/Volumes/...) are accessible via FUSE mount.
    If FUSE fails (e.g., SDP serverless without FUSE), falls back to dbutils.fs.head().
    """
    path_str = str(yaml_path)

    # Try pathlib first (works locally and on FUSE-enabled clusters).
    try:
        with Path(path_str).open("r", encoding="utf-8") as f:
            return f.read()
    except (FileNotFoundError, OSError):
        pass

    # Fallback: dbutils.fs.head() for UC Volume paths in SDP serverless.
    try:
        # dbutils is injected by Databricks runtime, not importable.
        return dbutils.fs.head(path_str, 1_000_000)  # type: ignore[name-defined]  # noqa: F821
    except Exception:
        pass

    raise FileNotFoundError(
        f"Cannot read YAML config at '{path_str}'. "
        f"Tried pathlib (FUSE) and dbutils.fs.head(). "
        f"Ensure the file exists in the UC Volume or workspace path."
    )


def load_config(yaml_path: str | Path) -> OMOPConfig:
    """Load and validate an OMOP YAML config from disk or UC Volume.

    Args:
        yaml_path: Path to a config YAML file (local path or /Volumes/... UC path).

    Returns:
        Validated ``OMOPConfig`` instance.
    """
    logger.info("Loading OMOP config from %s", yaml_path)
    print(f"[config_loader] Loading OMOP config from {yaml_path}", flush=True)
    text = _read_yaml_text(yaml_path)
    raw = yaml.safe_load(text)
    config = OMOPConfig.model_validate(raw)
    logger.info(
        "Validated config table_name=%s sources=%d joins=%d lookups=%d source_vocabs=%d",
        config.table_name,
        len(config.sources),
        len(config.joins),
        len(config.vocabulary_lookups),
        len(config.source_vocabulary),
    )
    print(
        f"[config_loader] Validated table_name={config.table_name} "
        f"sources={len(config.sources)} joins={len(config.joins)} "
        f"lookups={len(config.vocabulary_lookups)} source_vocabs={len(config.source_vocabulary)}",
        flush=True,
    )
    return config


def _normalize_join_type(join_type: str) -> str:
    """Map YAML join type strings to PySpark ``how`` values."""
    t = join_type.strip().lower()
    mapping = {
        "inner": "inner",
        "left": "left",
        "left_outer": "left",
        "right": "right",
        "right_outer": "right",
        "full": "outer",
        "full_outer": "outer",
        "outer": "outer",
        "cross": "cross",
    }
    if t not in mapping:
        raise ValueError(f"Unsupported join type: {join_type!r}; use one of {sorted(set(mapping.keys()))}")
    return mapping[t]


def resolve_sources(
    config: OMOPConfig,
    spark: SparkSession,
    catalog: str,
    bronze_schema: str,
) -> dict[str, DataFrame]:
    """Read each configured bronze source as a DataFrame.

    Table paths are formatted with ``catalog`` and ``bronze_schema`` placeholders.

    Args:
        config: Parsed OMOP config.
        spark: Active Spark session.
        catalog: Unity Catalog name.
        bronze_schema: Bronze schema name.

    Returns:
        Mapping of source alias to DataFrame.
    """
    out: dict[str, DataFrame] = {}
    for src in config.sources:
        fq = src.table.format(catalog=catalog, bronze_schema=bronze_schema)
        logger.info("Reading source alias=%s table=%s", src.alias, fq)
        print(f"[config_loader] resolve_sources: {src.alias} <- {fq}", flush=True)
        out[src.alias] = spark.read.table(fq)
    return out


def apply_joins(source_dfs: dict[str, DataFrame], joins: list[Join]) -> DataFrame:
    """Left-fold joins across sources using aliased DataFrames for SQL ``on`` expressions.

    If ``joins`` is empty, returns the single source DataFrame (exactly one source required).

    Args:
        source_dfs: Alias -> DataFrame for each source.
        joins: Ordered join specifications.

    Returns:
        Joined DataFrame.
    """
    if not joins:
        if len(source_dfs) != 1:
            raise ValueError("With no joins, exactly one source is required.")
        only = next(iter(source_dfs.values()))
        logger.info("No joins; returning single source DataFrame.")
        print("[config_loader] apply_joins: no joins, single source.", flush=True)
        return only

    first = joins[0]
    if first.left not in source_dfs or first.right not in source_dfs:
        raise KeyError(f"Join references unknown alias: {first.left}, {first.right}")
    how = _normalize_join_type(first.type)
    left_a = source_dfs[first.left].alias(first.left)
    right_a = source_dfs[first.right].alias(first.right)
    logger.info("Initial join %s %s %s ON %s", first.left, how, first.right, first.condition)
    print(
        f"[config_loader] apply_joins: initial {first.left} {how} JOIN {first.right} ON {first.condition}",
        flush=True,
    )

    working = left_a.join(right_a, on=sql_expr(first.condition), how=how)

    for j in joins[1:]:
        if j.right not in source_dfs:
            raise KeyError(f"Join references unknown right alias: {j.right}")
        how = _normalize_join_type(j.type)
        right_a = source_dfs[j.right].alias(j.right)
        logger.info("Extending join %s %s ON %s", how, j.right, j.condition)
        print(f"[config_loader] apply_joins: extend {how} JOIN {j.right} ON {j.condition}", flush=True)
        working = working.join(right_a, on=sql_expr(j.condition), how=how)

    return working
