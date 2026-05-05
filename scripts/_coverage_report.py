"""Source-to-concept mapping coverage report renderer.

Pure-rendering module that consumes the resolver's
``_concept_resolver.CoverageData`` and produces a markdown report at
``reports/source_mapping_coverage_<timestamp>.md``.

Per the documented design decisions: markdown output, not
stdout-only, not Lakehouse table for v2.0.7.

Design questions resolved (
message):

  Q1 (execution modes): POST-GENERATION ONLY for v2.0.7. Standalone
    audit mode (running against existing per-table configs without
    re-generating mappings) defers to v2.0.8 if customer pull warrants.
    Reasons: avoids splitting the v2.0.7 release message; avoids
    a parse-existing-CSV reconstruction path; keeps initial-release scope
    crisp.

  Q2 (output format): MARKDOWN ONLY for v2.0.7. HTML "share-out"
    flavor defers to v2.0.8 if customer pull warrants. Reasons:
    markdown renders well in VS Code, GitHub, and most modern
    document tools; HTML adds template engine choice + CSS + escaping
    test surface without immediate customer pull.

  Q3 (timestamp format): ISO 8601 with seconds, filesystem-safe
    variant — ``YYYY-MM-DDTHH-MM-SS`` (colons replaced with hyphens
    for Windows / cross-filesystem compatibility). Sortable in
    directory listings; readable; no collisions even at sub-minute
    cadence.

The report is a customer-facing artifact (committed alongside customer
configs to track mapping evolution over time). Treat the markdown
template as customer-facing documentation — wording, recommendations,
and references should be checkable against authoritative semantics
(per BACKLOG process-discipline convention 13: lens prompts must source
authoritative platform context; analogously, customer-facing prose must
source authoritative semantics for what the report claims).

Stdlib only — no markdown library dependency. Template-fill is
sufficient for the report's scope.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from _concept_resolver import CoverageData

# Report file basename pattern. ``YYYY-MM-DDTHH-MM-SS`` is filesystem-safe
# (no colons that Windows rejects); collation in directory listings sorts
# chronologically; second-level resolution avoids same-minute collisions.
_REPORT_FILENAME_PATTERN = "source_mapping_coverage_{timestamp}.md"

# Markdown special characters that can break table-cell rendering.
# ``|`` collides with table-column separators; ``\`` and the markdown
# emphasis characters render unexpectedly inside cells. We escape with
# leading backslash, which the CommonMark spec recognizes.
_MD_TABLE_CELL_ESCAPE_RE = re.compile(r"([\\|`*_{}\[\]()#+\-.!])")


def _format_iso_timestamp_safe(now: datetime | None = None) -> str:
    """Return ``YYYY-MM-DDTHH-MM-SS`` for ``now`` (default: ``datetime.now()``).

    Filesystem-safe (no colons). Tests pass an explicit ``now`` to
    pin the output for deterministic assertions.
    """
    dt = now if now is not None else datetime.now()
    return dt.strftime("%Y-%m-%dT%H-%M-%S")


def _md_escape_cell(text: str | int | float | None) -> str:
    r"""Escape markdown table-cell text so customer-supplied identifiers don't break tables.

    Customer table names, column names, vocabulary IDs, and source codes
    are arbitrary strings. Many customer EHRs use codes containing
    ``|`` (pipe), ``*`` (asterisk), ``_`` (underscore), or ``\`` (backslash).
    Without escaping, a cell containing ``|`` breaks the column boundary;
    asterisks render as italics; underscores render as emphasis.

    ``None`` renders as empty string; numbers are stringified.
    """
    if text is None:
        return ""
    if isinstance(text, (int, float)):
        return str(text)
    s = str(text)
    return _MD_TABLE_CELL_ESCAPE_RE.sub(r"\\\1", s)


def _resolved_total(vc) -> int:
    return vc.resolved_direct + vc.resolved_via_maps_to


def _unresolved_total(vc) -> int:
    return (
        vc.unresolved_no_concept
        + vc.unresolved_no_maps_to
        + vc.unresolved_ambiguous
    )


def _resolution_pct(vc) -> str:
    """Return ``NN.N%`` or ``n/a`` if total_distinct_codes is 0."""
    total = vc.total_distinct_codes
    if total == 0:
        return "n/a"
    pct = 100.0 * _resolved_total(vc) / total
    return f"{pct:.1f}%"


def _render_overview_section(coverage: CoverageData) -> str:
    """Top-level overview: aggregate counts across all vocabularies."""
    total_codes = sum(
        vc.total_distinct_codes for vc in coverage.per_vocabulary.values()
    )
    total_resolved = sum(
        _resolved_total(vc) for vc in coverage.per_vocabulary.values()
    )
    total_unresolved = sum(
        _unresolved_total(vc) for vc in coverage.per_vocabulary.values()
    )
    overall_pct = (
        f"{100.0 * total_resolved / total_codes:.1f}%" if total_codes else "n/a"
    )
    n_vocabs = len(coverage.per_vocabulary)
    n_cols = len(coverage.per_column)

    lines = [
        "## Overview",
        "",
        f"- Vocabularies analyzed: **{n_vocabs}**",
        f"- (source_alias, source_column) tuples covered: **{n_cols}**",
        f"- Distinct source codes seen: **{total_codes}**",
        f"- Resolved (standard concept reached): **{total_resolved}** ({overall_pct})",
        f"- Unresolved (manual mapping required): **{total_unresolved}**",
        "",
    ]
    return "\n".join(lines)


def _render_per_vocabulary_section(coverage: CoverageData) -> str:
    """Per-source-vocabulary five-bucket breakdown."""
    if not coverage.per_vocabulary:
        return (
            "## Per-vocabulary coverage\n"
            "\n"
            "_No source vocabularies processed in this run._\n"
            "\n"
        )
    lines = [
        "## Per-vocabulary coverage",
        "",
        "Five mutually exclusive resolution buckets per source vocabulary. "
        "`resolved_direct` + `resolved_via_maps_to` is the resolved-to-standard-concept "
        "count; the three `unresolved_*` columns identify gaps that need manual mapping.",
        "",
        "| vocabulary_id | total | resolved | direct | via Maps-to | unresolved | no concept | no Maps-to | ambiguous | resolution % |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for vocab in sorted(coverage.per_vocabulary.keys()):
        vc = coverage.per_vocabulary[vocab]
        lines.append(
            "| {vocab} | {total} | {resolved} | {direct} | {mapsto} | "
            "{unresolved} | {no_c} | {no_mt} | {amb} | {pct} |".format(
                vocab=_md_escape_cell(vocab),
                total=vc.total_distinct_codes,
                resolved=_resolved_total(vc),
                direct=vc.resolved_direct,
                mapsto=vc.resolved_via_maps_to,
                unresolved=_unresolved_total(vc),
                no_c=vc.unresolved_no_concept,
                no_mt=vc.unresolved_no_maps_to,
                amb=vc.unresolved_ambiguous,
                pct=_resolution_pct(vc),
            )
        )
    lines.append("")
    return "\n".join(lines)


def _render_per_column_section(coverage: CoverageData) -> str:
    """Per-(table, column) attribution table.

    The report attributes coverage gaps to the per-table YAML configs that
    introduced each column so customers can prioritize manual mapping
    work by config.
    """
    if not coverage.per_column:
        return (
            "## Per-column coverage attribution\n"
            "\n"
            "_No (table, column) tuples processed in this run._\n"
            "\n"
        )
    lines = [
        "## Per-column coverage attribution",
        "",
        "Distinct codes seen per `(source_alias, source_column)` tuple, with "
        "the originating per-table YAML config for traceability.",
        "",
        "| config | table | source_alias | source_column | vocabulary_id | distinct codes | resolved | unresolved |",
        "|---|---|---|---|---|---:|---:|---:|",
    ]
    sorted_keys = sorted(
        coverage.per_column.keys(),
        key=lambda k: (
            coverage.per_column[k].config_path,
            coverage.per_column[k].table_name,
            k[0],
            k[1],
        ),
    )
    for key in sorted_keys:
        col = coverage.per_column[key]
        lines.append(
            "| {cfg} | {tbl} | {alias} | {col} | {vocab} | {n} | {ok} | {bad} |".format(
                cfg=_md_escape_cell(col.config_path) or "_(unspecified)_",
                tbl=_md_escape_cell(col.table_name) or "_(unspecified)_",
                alias=_md_escape_cell(col.source_alias),
                col=_md_escape_cell(col.source_column),
                vocab=_md_escape_cell(col.vocabulary_id),
                n=col.distinct_codes,
                ok=col.resolved,
                bad=col.unresolved,
            )
        )
    lines.append("")
    return "\n".join(lines)


def _render_unmapped_samples_section(coverage: CoverageData) -> str:
    """Sample unmapped codes per vocabulary for triage focus."""
    if not coverage.sample_unmapped or not any(
        coverage.sample_unmapped.values()
    ):
        return (
            "## Sample unmapped codes\n"
            "\n"
            "_No unmapped codes in this run; all distinct source codes resolved "
            "to a standard concept._\n"
            "\n"
        )
    cap = coverage.max_sample_unmapped_per_vocab
    lines = [
        "## Sample unmapped codes",
        "",
        f"Up to **{cap}** unmapped codes per vocabulary, in lexicographic order. "
        "Use these as the starting point for manual mapping work; codes appearing "
        "here either have no concept in the OHDSI vocabulary, or have a non-standard "
        "concept with no `Maps to` traversal to a standard concept.",
        "",
    ]
    for vocab in sorted(coverage.sample_unmapped.keys()):
        codes = coverage.sample_unmapped[vocab][:cap]
        if not codes:
            continue
        lines.append(f"### {_md_escape_cell(vocab)}")
        lines.append("")
        lines.append(
            "```\n"
            + "\n".join(str(c) for c in codes)
            + "\n```"
        )
        lines.append("")
    return "\n".join(lines)


def _render_recommendations_section() -> str:
    """Static guidance for next steps after reviewing the report.

    Customer-facing prose. References AD-002 (skill-drafts /
    customer-reviews handoff pattern) so customers understand the
    skill's role boundary.
    """
    return (
        "## Recommended next steps\n"
        "\n"
        "1. **Resolved rows**: trust the drafted `target_concept_id` values for "
        "codes in the `resolved_direct` and `resolved_via_maps_to` buckets. "
        "These reached a standard concept via OHDSI's `concept` table or via "
        "a single-hop `Maps to` traversal in `concept_relationship`.\n"
        "2. **Unresolved rows (`target_concept_id = 0`)**: manual mapping is "
        "required before `01_load_vocabulary.py` MERGEs the CSV into the UC "
        "`source_to_concept_map` Delta table. The skill drafts mappings; "
        "domain-expert review owns final assignment (per AD-002 — skill-drafts "
        "/ customer-reviews handoff).\n"
        "   - `unresolved_no_concept`: source code does not exist in the OHDSI "
        "vocabulary. Likely a custom EHR code or a deprecated code; manual "
        "mapping to the closest standard concept is required.\n"
        "   - `unresolved_no_maps_to`: source code matches a non-standard "
        "concept that has no `Maps to` relationship in the loaded vocabulary "
        "version. Check vocabulary refresh status and the OHDSI Athena "
        "release notes; if expected behavior, mark with manual standard "
        "mapping.\n"
        "   - `unresolved_ambiguous`: source code's `Maps to` chain is "
        "non-standard (possible multi-hop) OR has multiple standard targets. "
        "Manual review is required to choose the correct standard concept; "
        "v2.0.7 surfaces but does not auto-resolve.\n"
        "3. **Per-column attribution**: use the per-column table to prioritize "
        "manual mapping work by per-table YAML config — configs with the "
        "largest `unresolved` columns warrant the most attention.\n"
        "4. **Vocabulary coverage**: vocabularies where `total_distinct_codes` "
        "is large but `resolution %` is low may indicate a vocabulary version "
        "mismatch, a CPT4 license-gating issue, or a custom code system "
        "incorrectly tagged with an OHDSI vocabulary_id.\n"
        "\n"
    )


def _render_metadata_footer(coverage: CoverageData) -> str:
    md = coverage.generation_metadata
    if md.configs_read:
        configs_lines = [f"  - `{_md_escape_cell(p)}`" for p in md.configs_read]
        configs_block = "\n".join(configs_lines)
    else:
        configs_block = "  - _(no configs recorded)_"
    return (
        "## Generation metadata\n"
        "\n"
        f"- Generator: `scripts/generate_source_concept_map.py` (v2.0.7+)\n"
        f"- Resolver: `scripts/_concept_resolver.py` (Approach 1, single-hop Maps-to)\n"
        f"- valid_start_date written to STCM rows: `{md.valid_start_date}`\n"
        f"- valid_end_date written to STCM rows: `{md.valid_end_date}`\n"
        f"- Concept-lookup IN-list chunk size: `{md.code_chunk_size}`\n"
        f"- Configs read:\n{configs_block}\n"
        "\n"
        "_Coverage data structure source of truth: "
        "`scripts/_concept_resolver.py::CoverageData`. This report "
        "renderer is downstream of that contract; if the contract evolves, "
        "The `TestCoverageDataShape` regression tests must coordinate-update._\n"
    )


def render_markdown_text(
    coverage: CoverageData,
    *,
    timestamp: str | None = None,
) -> str:
    """Render the full markdown report as a string.

    Separated from ``render_markdown`` (which writes to disk) so tests
    can assert on the text directly without a tmp_path round-trip and
    Documentation can include rendered samples without
    rerunning the generator.

    ``timestamp`` is the human-readable timestamp embedded in the
    report header. Tests pin this for deterministic assertions; the
    file-writing entry point computes it from ``datetime.now()`` and
    threads it through.
    """
    ts = timestamp if timestamp is not None else _format_iso_timestamp_safe()
    header = (
        "# Source-to-concept mapping coverage report\n"
        "\n"
        f"_Generated: `{ts}`_\n"
        "\n"
        "Drafted source_to_concept_map rows from the mapping generator. "
        "This report is the post-generation triage artifact for the v2.0.7+ "
        "draft generator (`scripts/generate_source_concept_map.py`). Use it to "
        "scope manual mapping work for the unresolved buckets before running "
        "`01_load_vocabulary.py` to MERGE the CSV into the UC "
        "`source_to_concept_map` Delta table.\n"
        "\n"
    )
    sections = [
        header,
        _render_overview_section(coverage),
        _render_per_vocabulary_section(coverage),
        _render_per_column_section(coverage),
        _render_unmapped_samples_section(coverage),
        _render_recommendations_section(),
        _render_metadata_footer(coverage),
    ]
    return "".join(sections)


def render_markdown(
    coverage: CoverageData,
    output_dir: str | Path,
    *,
    now: datetime | None = None,
) -> Path:
    """Render the report and write it to ``<output_dir>/source_mapping_coverage_<ts>.md``.

    Returns the resolved path of the written file. Creates ``output_dir``
    (and parents) idempotently — running twice does not fail.

    ``now`` is exposed for deterministic tests; production callers omit
    it to use ``datetime.now()``.
    """
    ts = _format_iso_timestamp_safe(now)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = _REPORT_FILENAME_PATTERN.format(timestamp=ts)
    out_path = out_dir / filename
    text = render_markdown_text(coverage, timestamp=ts)
    out_path.write_text(text, encoding="utf-8")
    return out_path.resolve()
