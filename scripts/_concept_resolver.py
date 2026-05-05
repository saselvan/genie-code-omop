"""Source-to-concept mapping draft generator core (v2.0.7).

Pure-logic module for the new ``generate_source_concept_map.py`` CLI.
Sibling to ``generate_source_mappings.py`` (single-vocab CLI bootstrap);
decoupled from CLI entry so the coverage report module can import the
same resolver and coverage data structures directly.

Reads per-table YAML configs to discover ``(source_alias, source_column,
vocabulary_id)`` tuples (via the ``source_vocabulary[]`` schema
extension). For each tuple:

  1. Queries the customer's bronze table for distinct codes in
     ``source_column``.
  2. Looks up each distinct code in ``reference.concept`` filtered by
     ``vocabulary_id`` (one batched query per source vocabulary, IN-list
     chunked at 500 codes per query).
  3. For non-standard matches (``standard_concept`` not equal ``'S'``),
     queries ``reference.concept_relationship`` for ``relationship_id =
     'Maps to'`` to obtain the standard ``concept_id``.
  4. Emits ``source_to_concept_map`` rows in OHDSI STCM shape.
  5. Records coverage data (per-vocabulary metrics, sample unmapped
     codes) for the report module.

Approach 1 only: direct Maps-to single-hop lookup. Approach 2 (fuzzy /
LLM-assisted matching) explicitly out of scope for v2.0.7.

Design decisions (resolved during cycle preparation):

  Q1 (Maps-to chain depth): SINGLE-HOP. OHDSI's ``Maps to`` is
    conceptually single-hop; multi-hop chains in real data surface as
    ``unresolved_ambiguous`` in coverage. v2.0.8+ may revisit if
    customer data shows multi-hop matters.

  Q2 (Round-trip count): BATCH. One concept-lookup query per source
    vocabulary (IN-list chunked at 500 codes per query); one
    ``concept_relationship`` Maps-to query per source vocabulary for
    non-standard concept_ids found. Total round-trips:
    ``O(vocabularies x 2 x ceil(codes/500))`` plus one distinct-codes
    query per ``(source_alias, source_column)`` tuple.

  Q3 (Multi-vocab-per-table): PER-COLUMN ITERATION. The
    ``source_vocabulary[]`` schema is per-column; this resolver iterates
    each entry independently. Tests cover the case where one table has
    columns from multiple source vocabularies.

SQL safety (known SQL safety concerns; future work will address):

  This module does NOT introduce new f-string SQL with unvalidated
  customer-provided identifiers. Identifier interpolation goes through
  ``_safe_identifier()`` (regex-validated then backtick-quoted); all
  literal values use parameterized queries via the SDK's
  ``StatementParameterListItem`` mechanism. Future SQL safety work
  will retrofit ``generate_source_mappings.py`` and ``validate_omop.py``
  ``_sql`` helpers to share this pattern; this module is the exemplar.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Callable

# ``SqlFn`` matches the dependency-injection shape used by
# ``_omop_validator.py``: a callable that takes ``statement`` (str) and
# optional ``parameters`` (list of dicts ready for SDK's
# ``StatementParameterListItem.from_dict``) and returns rows as a list of
# lists. Tests inject a mock; production wires through to the SDK's
# ``execute_statement`` via the CLI orchestrator.
SqlFn = Callable[..., list[list[Any]]]


# OHDSI STCM column order; matches ``STCM_FIELDS`` in
# ``generate_source_mappings.py`` and the existing
# ``seed_data/source_to_concept_map_custom.csv`` shape. Verified during
# Pre-flight verification against ``01_load_vocabulary.py`` which creates the
# UC ``source_to_concept_map`` Delta table with these exact columns.
STCM_FIELDS = (
    "source_code",
    "source_concept_id",
    "source_vocabulary_id",
    "source_code_description",
    "target_concept_id",
    "target_vocabulary_id",
    "valid_start_date",
    "valid_end_date",
    "invalid_reason",
)

# IN-list batching cap. Spark SQL accepts large IN-lists but planning
# cost grows; 500 is a conservative slice that keeps each query under
# the SDK's 50s wait_timeout for typical vocabulary sizes.
DEFAULT_CODE_CHUNK_SIZE = 500

# Identifier validator: SQL-safe table/column names only. Customer YAML
# configs declare these so they are user-provided input. Reject any
# identifier that isn't a basic alphanumeric+underscore token before
# backtick-quoting and interpolating.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_identifier(name: str, *, kind: str = "identifier") -> str:
    """Validate a customer-supplied identifier and return a backtick-quoted form.

    Raises ``ValueError`` for anything that isn't a basic alphanumeric +
    underscore token. The backtick quoting is defense in depth: even
    valid identifiers get quoted so reserved words don't break queries.
    """
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid {kind}: {name!r}. Expected pattern {_IDENTIFIER_RE.pattern!r}."
        )
    return f"`{name}`"


def _safe_fqn(fqn: str) -> str:
    """Validate and backtick-quote a 3-part ``catalog.schema.table`` FQN.

    The YAML ``Source.table`` field carries ``{catalog}.{bronze_schema}.<TABLE>``
    placeholders that the CLI orchestrator pre-substitutes via
    ``str.format``. By the time this resolver receives a FQN, all
    placeholders are resolved; we re-validate the three parts before
    interpolating into a query.
    """
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Invalid table FQN: {fqn!r}. Expected catalog.schema.table."
        )
    return ".".join(_safe_identifier(p, kind="fqn part") for p in parts)


def _chunk(items: list[str], size: int) -> Iterable[list[str]]:
    """Yield successive ``size``-length chunks from ``items``."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


@dataclass(frozen=True)
class SourceCodeRequest:
    """One ``(source_alias, source_column, vocabulary_id, source_table_fqn)`` tuple.

    Built by the CLI orchestrator from each per-table YAML config's
    ``source_vocabulary[]`` entries cross-referenced with ``sources[]``
    for the table FQN. Source table FQN has its placeholders already
    substituted (``{catalog}`` / ``{bronze_schema}`` resolved).

    ``config_path`` and ``table_name`` carry provenance for coverage
    reporting; the resolver doesn't use them for query construction.
    """

    source_alias: str
    source_column: str
    vocabulary_id: str
    source_table_fqn: str
    config_path: str = ""
    table_name: str = ""


@dataclass
class STCMRow:
    """One drafted ``source_to_concept_map`` row.

    Field order matches ``STCM_FIELDS``; the CLI writes these to the
    CSV via ``csv.DictWriter(fieldnames=STCM_FIELDS)``.
    """

    source_code: str
    source_concept_id: int
    source_vocabulary_id: str
    source_code_description: str
    target_concept_id: int
    target_vocabulary_id: str
    valid_start_date: str
    valid_end_date: str
    invalid_reason: str

    def as_dict(self) -> dict[str, str]:
        """Render row as a string-only dict for ``csv.DictWriter``."""
        return {
            "source_code": self.source_code,
            "source_concept_id": str(self.source_concept_id),
            "source_vocabulary_id": self.source_vocabulary_id,
            "source_code_description": self.source_code_description,
            "target_concept_id": str(self.target_concept_id),
            "target_vocabulary_id": self.target_vocabulary_id,
            "valid_start_date": self.valid_start_date,
            "valid_end_date": self.valid_end_date,
            "invalid_reason": self.invalid_reason,
        }


@dataclass
class VocabularyCoverage:
    """Per-source-vocabulary coverage metrics consumed by the report module.

    Counts represent distinct source codes (de-duplicated across the
    columns that share the same source vocabulary). The five mutually
    exclusive resolution buckets sum to ``total_distinct_codes``:

      resolved_direct          source code matched a STANDARD concept
                               directly (no Maps-to needed)
      resolved_via_maps_to     source code matched a non-standard concept
                               whose Maps-to chain reached a standard
                               concept
      unresolved_no_concept    source code not present in
                               ``reference.concept`` for this vocabulary
      unresolved_no_maps_to    source code matched a non-standard concept
                               that has no ``Maps to`` relationship to a
                               standard concept (orphan / deprecated)
      unresolved_ambiguous     source code's Maps-to target is itself
                               non-standard (multi-hop case; v2.0.7
                               surfaces but doesn't auto-traverse)
    """

    vocabulary_id: str
    total_distinct_codes: int = 0
    resolved_direct: int = 0
    resolved_via_maps_to: int = 0
    unresolved_no_concept: int = 0
    unresolved_no_maps_to: int = 0
    unresolved_ambiguous: int = 0


@dataclass
class ColumnCoverage:
    """Per-``(source_alias, source_column)`` coverage metrics.

    The report consumes these to attribute coverage gaps to the
    per-table YAML configs that introduced each column. Counts represent
    distinct codes seen on this column (NOT de-duplicated against other
    columns sharing the same vocabulary).
    """

    source_alias: str
    source_column: str
    vocabulary_id: str
    config_path: str = ""
    table_name: str = ""
    distinct_codes: int = 0
    resolved: int = 0
    unresolved: int = 0


@dataclass
class GenerationMetadata:
    """Top-level metadata for a single generator run.

    ``valid_start_date`` is the run timestamp; the resolver writes this
    to every drafted STCM row's ``valid_start_date`` so customers can
    audit which generation produced which rows. ``valid_end_date`` is a
    sentinel matching the existing ``generate_source_mappings.py``
    convention (``20991231``).
    """

    valid_start_date: str = "19700101"
    valid_end_date: str = "20991231"
    configs_read: list[str] = field(default_factory=list)
    code_chunk_size: int = DEFAULT_CODE_CHUNK_SIZE


@dataclass
class CoverageData:
    """Top-level coverage data structure consumed by the report module.

    The coverage report module imports this dataclass directly
    from ``_concept_resolver``; the contract is the field names and
    types here, not the printed CSV. Fields:

      per_vocabulary       vocabulary_id -> aggregated metrics
      per_column           (source_alias, source_column) -> per-column metrics
      sample_unmapped      vocabulary_id -> list of first N unmapped codes
                           (capped at ``max_sample_unmapped_per_vocab``)
      generation_metadata  run-level metadata (timestamp, configs read)
    """

    per_vocabulary: dict[str, VocabularyCoverage] = field(default_factory=dict)
    per_column: dict[tuple[str, str], ColumnCoverage] = field(default_factory=dict)
    sample_unmapped: dict[str, list[str]] = field(default_factory=dict)
    generation_metadata: GenerationMetadata = field(default_factory=GenerationMetadata)
    max_sample_unmapped_per_vocab: int = 20


def collect_distinct_codes(
    sql_fn: SqlFn,
    requests: list[SourceCodeRequest],
) -> dict[tuple[str, str], list[str]]:
    """Issue one DISTINCT query per (source_table_fqn, source_column) tuple.

    Returns a mapping from ``(source_alias, source_column)`` to the
    sorted list of non-null distinct string values found in that
    column. The same source column appearing in multiple requests is
    queried once.

    Identifiers (FQN parts, column names) go through ``_safe_identifier``
    / ``_safe_fqn``; null values and non-string values are stringified.
    """
    seen_columns: dict[tuple[str, str], list[str]] = {}
    queried: set[tuple[str, str]] = set()
    for req in requests:
        per_column_key = (req.source_alias, req.source_column)
        if per_column_key in queried:
            continue
        queried.add(per_column_key)

        fqn = _safe_fqn(req.source_table_fqn)
        col = _safe_identifier(req.source_column, kind="column")
        statement = (
            f"SELECT DISTINCT {col} AS c FROM {fqn} WHERE {col} IS NOT NULL"
        )
        rows = sql_fn(statement=statement)
        codes = sorted(
            {str(r[0]) for r in rows if r and r[0] is not None}
        )
        seen_columns[per_column_key] = codes
    return seen_columns


def _bucket_codes_by_vocabulary(
    requests: list[SourceCodeRequest],
    distinct_by_column: dict[tuple[str, str], list[str]],
) -> dict[str, set[str]]:
    """Aggregate distinct codes per source vocabulary across columns.

    A single vocabulary may appear on multiple ``(table, column)``
    tuples (e.g. ICD10CM on primary_diagnosis and secondary_diagnosis
    columns). Per Q2 batch design, we de-duplicate codes across columns
    so the concept-lookup query runs once per vocabulary.
    """
    by_vocab: dict[str, set[str]] = {}
    for req in requests:
        codes = distinct_by_column.get((req.source_alias, req.source_column), [])
        by_vocab.setdefault(req.vocabulary_id, set()).update(codes)
    return by_vocab


@dataclass
class _ConceptHit:
    """One row from the ``concept`` lookup query."""

    concept_id: int
    concept_name: str
    vocabulary_id: str
    standard_concept: str | None
    concept_code: str


def lookup_concepts(
    sql_fn: SqlFn,
    *,
    catalog: str,
    ref_schema: str,
    codes_by_vocabulary: dict[str, set[str]],
    chunk_size: int = DEFAULT_CODE_CHUNK_SIZE,
) -> dict[str, dict[str, _ConceptHit]]:
    """Query ``reference.concept`` for the given codes per vocabulary.

    Returns a nested mapping: ``vocabulary_id -> concept_code -> _ConceptHit``.
    Codes that don't match are simply absent from the inner dict; the
    caller compares against the requested set to find unresolved codes.

    Issues one query per vocabulary per chunk (chunk size 500). Each
    query uses parameterized values for both ``vocabulary_id`` and the
    code IN-list, so no customer code text reaches the SQL string
    unescaped.
    """
    catalog_q = _safe_identifier(catalog, kind="catalog")
    ref_schema_q = _safe_identifier(ref_schema, kind="ref_schema")
    concept_fqn = f"{catalog_q}.{ref_schema_q}.`concept`"

    out: dict[str, dict[str, _ConceptHit]] = {}
    for vocab, codes in codes_by_vocabulary.items():
        per_vocab: dict[str, _ConceptHit] = {}
        if not codes:
            out[vocab] = per_vocab
            continue
        sorted_codes = sorted(codes)
        for chunk in _chunk(sorted_codes, chunk_size):
            param_names = [f"c{i}" for i in range(len(chunk))]
            in_list = ", ".join(f":{n}" for n in param_names)
            statement = (
                "SELECT concept_id, concept_name, vocabulary_id, "
                "standard_concept, concept_code "
                f"FROM {concept_fqn} "
                "WHERE vocabulary_id = :vocab "
                f"  AND concept_code IN ({in_list})"
            )
            parameters = [{"name": "vocab", "type": "STRING", "value": vocab}]
            for n, c in zip(param_names, chunk):
                parameters.append({"name": n, "type": "STRING", "value": c})
            rows = sql_fn(statement=statement, parameters=parameters)
            for row in rows:
                if not row or row[0] is None:
                    continue
                hit = _ConceptHit(
                    concept_id=int(row[0]),
                    concept_name=str(row[1]) if row[1] is not None else "",
                    vocabulary_id=str(row[2]) if len(row) > 2 and row[2] is not None else vocab,
                    standard_concept=(
                        str(row[3]) if len(row) > 3 and row[3] is not None else None
                    ),
                    concept_code=str(row[4]) if len(row) > 4 and row[4] is not None else "",
                )
                per_vocab[hit.concept_code] = hit
        out[vocab] = per_vocab
    return out


def lookup_maps_to(
    sql_fn: SqlFn,
    *,
    catalog: str,
    ref_schema: str,
    non_standard_concept_ids: set[int],
    chunk_size: int = DEFAULT_CODE_CHUNK_SIZE,
) -> dict[int, list[_ConceptHit]]:
    """Query ``concept_relationship`` then ``concept`` for Maps-to targets.

    Returns ``concept_id_1 -> [_ConceptHit, ...]`` where each hit is the
    standard target concept reached via ``relationship_id = 'Maps to'``.
    Multiple hits per source concept indicate ambiguity (rare; surfaced
    by the caller as ``unresolved_ambiguous``).

    Two passes:

      1. ``concept_relationship`` query produces ``concept_id_1 -> [concept_id_2, ...]``.
      2. ``concept`` query looks up the standard concepts to populate
         vocabulary, name, and standard flag for STCM row construction.
    """
    if not non_standard_concept_ids:
        return {}

    catalog_q = _safe_identifier(catalog, kind="catalog")
    ref_schema_q = _safe_identifier(ref_schema, kind="ref_schema")
    cr_fqn = f"{catalog_q}.{ref_schema_q}.`concept_relationship`"
    concept_fqn = f"{catalog_q}.{ref_schema_q}.`concept`"

    targets_by_source: dict[int, list[int]] = {}
    sorted_ids = sorted(non_standard_concept_ids)
    for chunk in _chunk([str(x) for x in sorted_ids], chunk_size):
        param_names = [f"i{i}" for i in range(len(chunk))]
        in_list = ", ".join(f":{n}" for n in param_names)
        statement = (
            "SELECT concept_id_1, concept_id_2 "
            f"FROM {cr_fqn} "
            "WHERE relationship_id = :rel "
            f"  AND concept_id_1 IN ({in_list}) "
            "  AND (invalid_reason IS NULL OR invalid_reason = '')"
        )
        parameters = [{"name": "rel", "type": "STRING", "value": "Maps to"}]
        for n, c in zip(param_names, chunk):
            parameters.append({"name": n, "type": "INT", "value": c})
        rows = sql_fn(statement=statement, parameters=parameters)
        for row in rows:
            if not row or row[0] is None or row[1] is None:
                continue
            src = int(row[0])
            tgt = int(row[1])
            targets_by_source.setdefault(src, []).append(tgt)

    all_targets = {tgt for tgts in targets_by_source.values() for tgt in tgts}
    targets_by_id: dict[int, _ConceptHit] = {}
    for chunk in _chunk([str(x) for x in sorted(all_targets)], chunk_size):
        param_names = [f"i{i}" for i in range(len(chunk))]
        in_list = ", ".join(f":{n}" for n in param_names)
        statement = (
            "SELECT concept_id, concept_name, vocabulary_id, "
            "standard_concept, concept_code "
            f"FROM {concept_fqn} "
            f"WHERE concept_id IN ({in_list})"
        )
        parameters = []
        for n, c in zip(param_names, chunk):
            parameters.append({"name": n, "type": "INT", "value": c})
        rows = sql_fn(statement=statement, parameters=parameters)
        for row in rows:
            if not row or row[0] is None:
                continue
            hit = _ConceptHit(
                concept_id=int(row[0]),
                concept_name=str(row[1]) if row[1] is not None else "",
                vocabulary_id=str(row[2]) if len(row) > 2 and row[2] is not None else "",
                standard_concept=(
                    str(row[3]) if len(row) > 3 and row[3] is not None else None
                ),
                concept_code=str(row[4]) if len(row) > 4 and row[4] is not None else "",
            )
            targets_by_id[hit.concept_id] = hit

    out: dict[int, list[_ConceptHit]] = {}
    for src, tgt_ids in targets_by_source.items():
        hits = [targets_by_id[t] for t in tgt_ids if t in targets_by_id]
        if hits:
            out[src] = hits
    return out


def _is_standard(concept: _ConceptHit) -> bool:
    """OHDSI ``standard_concept`` semantics: ``'S'`` means standard."""
    return concept.standard_concept == "S"


def build_stcm_rows(
    requests: list[SourceCodeRequest],
    distinct_by_column: dict[tuple[str, str], list[str]],
    concepts_by_vocab: dict[str, dict[str, _ConceptHit]],
    maps_to_by_concept: dict[int, list[_ConceptHit]],
    metadata: GenerationMetadata,
) -> tuple[list[STCMRow], CoverageData]:
    """Assemble STCM rows + coverage data from resolution results.

    Returns ``(rows, coverage)``. Rows are deduplicated across columns
    that share the same ``(source_code, source_vocabulary_id)`` pair —
    OHDSI STCM's MERGE key is exactly this pair, so producing duplicate
    rows would be ambiguous (the loader would arbitrarily pick one).

    Each unique source code produces exactly one STCM row in the output.
    The row's ``target_concept_id`` reflects the resolution outcome:

      - direct standard match -> matched concept_id
      - Maps-to single-hop to standard -> standard concept_id
      - Maps-to to non-standard (ambiguous / multi-hop) -> 0 with
        description noting ambiguity
      - matched but no Maps-to -> 0 with description noting orphan
      - no concept match -> 0 with description ``UNRESOLVED``

    Rows with ``target_concept_id = 0`` are still emitted so customers
    can see every distinct code in their data; manual mapping replaces
    the 0 in the CSV before the loader's MERGE.
    """
    coverage = CoverageData(generation_metadata=metadata)

    codes_by_vocab_seen: dict[str, set[str]] = {}
    for req in requests:
        codes = distinct_by_column.get((req.source_alias, req.source_column), [])
        codes_by_vocab_seen.setdefault(req.vocabulary_id, set()).update(codes)
        col_key = (req.source_alias, req.source_column)
        col = coverage.per_column.setdefault(
            col_key,
            ColumnCoverage(
                source_alias=req.source_alias,
                source_column=req.source_column,
                vocabulary_id=req.vocabulary_id,
                config_path=req.config_path,
                table_name=req.table_name,
            ),
        )
        col.distinct_codes = len(codes)

    rows: list[STCMRow] = []
    emitted_keys: set[tuple[str, str]] = set()

    for vocab, codes in codes_by_vocab_seen.items():
        vc = coverage.per_vocabulary.setdefault(
            vocab, VocabularyCoverage(vocabulary_id=vocab)
        )
        vc.total_distinct_codes = len(codes)
        per_vocab_concepts = concepts_by_vocab.get(vocab, {})
        unmapped_samples: list[str] = []

        for code in sorted(codes):
            key = (code, vocab)
            if key in emitted_keys:
                continue
            emitted_keys.add(key)

            hit = per_vocab_concepts.get(code)
            if hit is None:
                vc.unresolved_no_concept += 1
                if len(unmapped_samples) < coverage.max_sample_unmapped_per_vocab:
                    unmapped_samples.append(code)
                rows.append(
                    STCMRow(
                        source_code=code,
                        source_concept_id=0,
                        source_vocabulary_id=vocab,
                        source_code_description=(
                            "UNRESOLVED - no concept in OHDSI vocabulary; manual mapping required"
                        ),
                        target_concept_id=0,
                        target_vocabulary_id="",
                        valid_start_date=metadata.valid_start_date,
                        valid_end_date=metadata.valid_end_date,
                        invalid_reason="",
                    )
                )
                continue

            if _is_standard(hit):
                vc.resolved_direct += 1
                rows.append(
                    STCMRow(
                        source_code=code,
                        source_concept_id=hit.concept_id,
                        source_vocabulary_id=vocab,
                        source_code_description=hit.concept_name,
                        target_concept_id=hit.concept_id,
                        target_vocabulary_id=hit.vocabulary_id,
                        valid_start_date=metadata.valid_start_date,
                        valid_end_date=metadata.valid_end_date,
                        invalid_reason="",
                    )
                )
                continue

            mt_hits = maps_to_by_concept.get(hit.concept_id, [])
            standard_hits = [h for h in mt_hits if _is_standard(h)]

            if not mt_hits:
                vc.unresolved_no_maps_to += 1
                if len(unmapped_samples) < coverage.max_sample_unmapped_per_vocab:
                    unmapped_samples.append(code)
                rows.append(
                    STCMRow(
                        source_code=code,
                        source_concept_id=hit.concept_id,
                        source_vocabulary_id=vocab,
                        source_code_description=(
                            f"{hit.concept_name} - non-standard with no 'Maps to' relationship; "
                            "manual mapping required"
                        ),
                        target_concept_id=0,
                        target_vocabulary_id="",
                        valid_start_date=metadata.valid_start_date,
                        valid_end_date=metadata.valid_end_date,
                        invalid_reason="",
                    )
                )
                continue

            if not standard_hits:
                vc.unresolved_ambiguous += 1
                if len(unmapped_samples) < coverage.max_sample_unmapped_per_vocab:
                    unmapped_samples.append(code)
                rows.append(
                    STCMRow(
                        source_code=code,
                        source_concept_id=hit.concept_id,
                        source_vocabulary_id=vocab,
                        source_code_description=(
                            f"{hit.concept_name} - 'Maps to' targets are non-standard "
                            "(possible multi-hop); manual mapping required"
                        ),
                        target_concept_id=0,
                        target_vocabulary_id="",
                        valid_start_date=metadata.valid_start_date,
                        valid_end_date=metadata.valid_end_date,
                        invalid_reason="",
                    )
                )
                continue

            chosen = sorted(standard_hits, key=lambda h: h.concept_id)[0]
            if len(standard_hits) > 1:
                vc.unresolved_ambiguous += 1
                if len(unmapped_samples) < coverage.max_sample_unmapped_per_vocab:
                    unmapped_samples.append(code)
                description = (
                    f"{hit.concept_name} - multiple 'Maps to' standard targets "
                    f"({len(standard_hits)}); chose lowest concept_id; manual review recommended"
                )
                target_id = 0
                target_vocab = ""
            else:
                vc.resolved_via_maps_to += 1
                description = hit.concept_name
                target_id = chosen.concept_id
                target_vocab = chosen.vocabulary_id

            rows.append(
                STCMRow(
                    source_code=code,
                    source_concept_id=hit.concept_id,
                    source_vocabulary_id=vocab,
                    source_code_description=description,
                    target_concept_id=target_id,
                    target_vocabulary_id=target_vocab,
                    valid_start_date=metadata.valid_start_date,
                    valid_end_date=metadata.valid_end_date,
                    invalid_reason="",
                )
            )

        if unmapped_samples:
            coverage.sample_unmapped[vocab] = unmapped_samples

    for col_key, col in coverage.per_column.items():
        per_vocab_concepts = concepts_by_vocab.get(col.vocabulary_id, {})
        codes = distinct_by_column.get(col_key, [])
        resolved_count = 0
        for code in codes:
            hit = per_vocab_concepts.get(code)
            if hit is None:
                continue
            if _is_standard(hit):
                resolved_count += 1
                continue
            standard_hits = [
                h
                for h in maps_to_by_concept.get(hit.concept_id, [])
                if _is_standard(h)
            ]
            if len(standard_hits) == 1:
                resolved_count += 1
        col.resolved = resolved_count
        col.unresolved = col.distinct_codes - resolved_count

    return rows, coverage


def resolve(
    sql_fn: SqlFn,
    *,
    catalog: str,
    ref_schema: str,
    requests: list[SourceCodeRequest],
    chunk_size: int = DEFAULT_CODE_CHUNK_SIZE,
    metadata: GenerationMetadata | None = None,
) -> tuple[list[STCMRow], CoverageData]:
    """End-to-end: distinct codes -> concept lookup -> Maps-to -> STCM rows + coverage.

    Single entry point for the CLI orchestrator and for the
    coverage report module. ``sql_fn`` is the dependency-injected SQL
    execution callable; tests pass a mock, the CLI passes a real-SDK
    wrapper.
    """
    if metadata is None:
        metadata = GenerationMetadata()
    if requests:
        configs_seen = []
        for req in requests:
            if req.config_path and req.config_path not in configs_seen:
                configs_seen.append(req.config_path)
        metadata.configs_read = configs_seen

    distinct_by_column = collect_distinct_codes(sql_fn, requests)
    codes_by_vocab = _bucket_codes_by_vocabulary(requests, distinct_by_column)
    concepts_by_vocab = lookup_concepts(
        sql_fn,
        catalog=catalog,
        ref_schema=ref_schema,
        codes_by_vocabulary=codes_by_vocab,
        chunk_size=chunk_size,
    )

    non_standard_ids: set[int] = set()
    for vocab, per_vocab in concepts_by_vocab.items():
        for hit in per_vocab.values():
            if not _is_standard(hit):
                non_standard_ids.add(hit.concept_id)

    maps_to_by_concept = lookup_maps_to(
        sql_fn,
        catalog=catalog,
        ref_schema=ref_schema,
        non_standard_concept_ids=non_standard_ids,
        chunk_size=chunk_size,
    )

    return build_stcm_rows(
        requests,
        distinct_by_column,
        concepts_by_vocab,
        maps_to_by_concept,
        metadata,
    )


def build_requests_from_configs(
    configs: list[tuple[str, Any]],
    *,
    catalog: str,
    bronze_schema: str,
) -> list[SourceCodeRequest]:
    """Cross-reference each config's ``source_vocabulary[]`` with ``sources[]``.

    Inputs:
      configs: list of ``(config_path, OMOPConfig)`` pairs (path retained
        for provenance tracking in coverage data).
      catalog / bronze_schema: substituted into ``Source.table``
        placeholders via ``str.format``.

    Returns: list of ``SourceCodeRequest`` with FQN placeholders resolved.
    Skips configs whose ``source_vocabulary`` is empty (back-compat: pre-v2.0.7
    configs without the section are unaffected). Raises ``ValueError``
    if a ``source_vocabulary`` entry references an undeclared
    ``source_alias`` (catches typos before SQL is issued).
    """
    out: list[SourceCodeRequest] = []
    for cfg_path, config in configs:
        if not getattr(config, "source_vocabulary", None):
            continue
        sources_by_alias = {s.alias: s for s in config.sources}
        for entry in config.source_vocabulary:
            src = sources_by_alias.get(entry.source_alias)
            if src is None:
                raise ValueError(
                    f"Config {cfg_path}: source_vocabulary entry references "
                    f"unknown source_alias={entry.source_alias!r}; declared aliases: "
                    f"{sorted(sources_by_alias.keys())}"
                )
            try:
                fqn = src.table.format(catalog=catalog, bronze_schema=bronze_schema)
            except KeyError as e:
                raise ValueError(
                    f"Config {cfg_path}: source {entry.source_alias!r} table "
                    f"placeholder {e!s} not provided. Expected {{catalog}} and "
                    "{bronze_schema} placeholders only."
                ) from e
            out.append(
                SourceCodeRequest(
                    source_alias=entry.source_alias,
                    source_column=entry.source_column,
                    vocabulary_id=entry.vocabulary_id,
                    source_table_fqn=fqn,
                    config_path=cfg_path,
                    table_name=config.table_name,
                )
            )
    return out
