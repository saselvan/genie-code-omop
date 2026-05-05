#!/usr/bin/env python3
"""Source-to-concept mapping draft generator (v2.0.7).

CLI orchestrator for the per-table-config-driven, multi-vocabulary STCM
draft generator. Reads per-table YAML configs (
``source_vocabulary[]`` schema), queries OHDSI ``concept`` and
``concept_relationship`` (Maps-to single-hop) via a serverless SQL
warehouse, and writes drafted ``source_to_concept_map`` rows to a CSV
matching the ``seed_data/source_to_concept_map_custom.csv`` shape that
``templates/.../src/01_load_vocabulary.py`` MERGEs into the UC
``{catalog}.{ref_schema}.source_to_concept_map`` Delta table on the
next vocabulary load.

Sibling utility to ``generate_source_mappings.py`` (single-vocab CLI
bootstrap that takes ``--source-vocabulary-id``, ``--source-table``,
``--source-code-column`` flags). Both ship; pick by use case:

  - One column, one vocabulary, one-off bootstrap     ->  generate_source_mappings.py
  - Many configs, many vocabularies, repeatable run   ->  generate_source_concept_map.py (this script)

Substantive correctness work (single-hop Maps-to traversal across
``concept_relationship``; five-bucket coverage classification) lives in
``_concept_resolver.py``; this script is glue (argparse + SDK wiring +
CSV writer + coverage summary printer).

Output rows beyond direct standard matches:

  - resolved_via_maps_to    target_concept_id = standard concept reached
                            via single-hop Maps-to from a non-standard
                            source concept
  - unresolved_no_concept   target_concept_id = 0; description names the
                            gap (no concept in OHDSI vocabulary)
  - unresolved_no_maps_to   target_concept_id = 0; description notes the
                            non-standard source has no Maps-to
                            relationship (orphan / deprecated)
  - unresolved_ambiguous    target_concept_id = 0; description notes
                            multiple Maps-to standard targets OR the
                            Maps-to target is itself non-standard
                            (possible multi-hop case)

Customers must replace target_concept_id = 0 with manual mapping before
running ``01_load_vocabulary.py`` (which MERGEs the rows into the UC
``source_to_concept_map`` Delta table the runtime resolver consumes).

Auth is handled by Databricks runtime when invoked from Genie Code
Agent. ``--profile`` only applies for local development against
``~/.databrickscfg``.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue, NotFound
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

from _concept_resolver import (
    DEFAULT_CODE_CHUNK_SIZE,
    STCM_FIELDS,
    GenerationMetadata,
    SqlFn,
    build_requests_from_configs,
    resolve,
)
from _coverage_report import render_markdown
from _warehouse import resolve_warehouse_id


def _sql(
    w: WorkspaceClient,
    *,
    warehouse_id: str,
    statement: str,
    parameters: list[dict] | None = None,
    catalog: str | None = None,
    schema: str | None = None,
) -> list[list[Any]]:
    """Execute a SQL statement with parameterized literals.

    Modeled on ``validate_omop.py``'s SDK-exception-wrapped ``_sql``;
    ``parameters`` accepts the resolver's dict-shaped parameter
    descriptors (``{"name": ..., "type": ..., "value": ...}``) and
    converts them to ``StatementParameterListItem`` for the SDK call.

    This is the exemplar SQL safety
    pattern. Future work will unify ``_sql`` implementations
    across ``generate_source_mappings.py``, ``validate_omop.py``, and
    this script.
    """
    kwargs: dict[str, Any] = {
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "50s",
    }
    if catalog:
        kwargs["catalog"] = catalog
    if schema:
        kwargs["schema"] = schema
    if parameters:
        kwargs["parameters"] = [
            StatementParameterListItem(
                name=p["name"], type=p.get("type", "STRING"), value=p["value"]
            )
            for p in parameters
        ]
    try:
        resp = w.statement_execution.execute_statement(**kwargs)
    except (InvalidParameterValue, NotFound) as e:
        msg = str(e).strip()
        low = msg.lower()
        if "endpoint" in low or "warehouse" in low:
            raise SystemExit(
                f"Invalid SQL warehouse: {msg} "
                "Verify with `databricks warehouses list`, pass --warehouse-id, "
                "or set DATABRICKS_WAREHOUSE_ID."
            ) from e
        raise
    if resp.status.state != StatementState.SUCCEEDED:
        err = getattr(resp.status, "error", None)
        msg = getattr(err, "message", None) or str(resp.status.state)
        raise RuntimeError(f"Statement failed: {msg}")
    res = getattr(resp, "result", None)
    if res is None:
        return []
    rows = getattr(res, "data_array", None)
    if rows is None and hasattr(res, "data"):
        rows = res.data
    return rows or []


def _load_configs(config_paths: list[str]):
    """Load each per-table YAML config via the host Pydantic validator.

    Returns ``[(config_path, OMOPConfig), ...]``. Surfaces validation
    errors with a SystemExit naming the offending file so the user
    doesn't have to read a stack trace.
    """
    from src.config_loader import load_config

    out = []
    for p in config_paths:
        path = Path(p)
        if not path.exists():
            raise SystemExit(f"Config not found: {p}")
        try:
            config = load_config(path)
        except Exception as e:
            raise SystemExit(f"Config validation failed for {p}: {e}") from e
        out.append((str(path), config))
    return out


def _print_coverage_summary(coverage, total_rows: int) -> None:
    """Print a one-screen coverage summary (the coverage report module produces a richer report)."""
    print("")
    print("=" * 70)
    print(f"Generated {total_rows} draft source_to_concept_map rows")
    print("=" * 70)
    print("")
    print("Per-vocabulary coverage:")
    for vocab in sorted(coverage.per_vocabulary.keys()):
        vc = coverage.per_vocabulary[vocab]
        resolved = vc.resolved_direct + vc.resolved_via_maps_to
        unresolved = (
            vc.unresolved_no_concept
            + vc.unresolved_no_maps_to
            + vc.unresolved_ambiguous
        )
        print(
            f"  {vocab}: total={vc.total_distinct_codes} "
            f"resolved={resolved} (direct={vc.resolved_direct}, "
            f"maps_to={vc.resolved_via_maps_to}) "
            f"unresolved={unresolved} (no_concept={vc.unresolved_no_concept}, "
            f"no_maps_to={vc.unresolved_no_maps_to}, "
            f"ambiguous={vc.unresolved_ambiguous})"
        )
    if coverage.sample_unmapped:
        print("")
        print("Sample unmapped codes (first 10 per vocabulary):")
        for vocab in sorted(coverage.sample_unmapped.keys()):
            sample = coverage.sample_unmapped[vocab][:10]
            print(f"  {vocab}: {', '.join(sample)}")
    print("")


def _write_csv(rows, output_path: Path) -> None:
    """Write STCM rows to CSV with the canonical 9-column header."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(STCM_FIELDS))
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_dict())


def _run(args: argparse.Namespace, sql_fn: SqlFn) -> int:
    """Testable orchestration: configs -> requests -> resolve -> CSV + summary.

    Separated from ``main`` so tests can inject a mock ``sql_fn`` without
    standing up a WorkspaceClient. Returns process exit code.
    """
    if not args.configs:
        print("ERROR: --configs requires at least one path", file=sys.stderr)
        return 1

    configs = _load_configs(args.configs)
    requests = build_requests_from_configs(
        configs, catalog=args.catalog, bronze_schema=args.bronze_schema
    )
    if not requests:
        print(
            "ERROR: no source_vocabulary entries found across the provided configs. "
            "Add a source_vocabulary[] section to at least one per-table YAML "
            "(see configs/_schema.yaml v2.0.7+ schema extension).",
            file=sys.stderr,
        )
        return 1

    metadata = GenerationMetadata()
    rows, coverage = resolve(
        sql_fn,
        catalog=args.catalog,
        ref_schema=args.ref_schema,
        requests=requests,
        chunk_size=args.chunk_size,
        metadata=metadata,
    )

    output_path = Path(args.output)
    _write_csv(rows, output_path)
    print(f"Wrote: {output_path.resolve()}")

    if not args.no_report:
        report_path = render_markdown(coverage, args.output_report_dir)
        print(f"Wrote coverage report: {report_path}")

    _print_coverage_summary(coverage, len(rows))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Draft source_to_concept_map rows from per-table YAML configs by "
            "resolving customer source codes through OHDSI's concept and "
            "concept_relationship (Maps-to single-hop) reference tables. Output "
            "is a CSV in OHDSI STCM shape; review unresolved rows (target_concept_id = 0) "
            "and replace with manual mappings before 01_load_vocabulary.py MERGEs "
            "into the UC source_to_concept_map Delta table."
        )
    )
    parser.add_argument(
        "--configs",
        nargs="+",
        required=True,
        help="One or more per-table YAML config paths (e.g. configs/condition_occurrence.yaml).",
    )
    parser.add_argument(
        "--catalog",
        required=True,
        help="UC catalog (substituted into Source.table placeholders and used for the OHDSI reference schema).",
    )
    parser.add_argument(
        "--bronze-schema",
        required=True,
        help="UC bronze schema (substituted into Source.table placeholders).",
    )
    parser.add_argument(
        "--ref-schema",
        default="reference",
        help="UC reference schema holding the OHDSI vocabulary tables (default: reference).",
    )
    parser.add_argument(
        "--output",
        default="seed_data/source_to_concept_map_custom.csv",
        help="Output CSV path (default: seed_data/source_to_concept_map_custom.csv).",
    )
    parser.add_argument("--warehouse-id", default=None)
    parser.add_argument("--profile", default=None)
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CODE_CHUNK_SIZE,
        help=f"IN-list chunk size for batched concept lookups (default: {DEFAULT_CODE_CHUNK_SIZE}).",
    )
    parser.add_argument(
        "--output-report-dir",
        default="reports",
        help=(
            "Directory for the post-generation markdown coverage report "
            "(default: reports/). The report file basename is "
            "source_mapping_coverage_<timestamp>.md per the documented design "
            "decision Q3 (ISO 8601 with seconds, filesystem-safe variant)."
        ),
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help=(
            "Skip post-generation markdown coverage report emission. CSV output "
            "is unaffected. Useful for one-off runs and CI pipelines that don't "
            "need the report artifact."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    warehouse_id = resolve_warehouse_id(args.warehouse_id, profile=args.profile, client=w)

    def sql_fn(*, statement: str, parameters: list[dict] | None = None) -> list[list[Any]]:
        return _sql(
            w,
            warehouse_id=warehouse_id,
            statement=statement,
            parameters=parameters,
            catalog=args.catalog,
            schema=args.ref_schema,
        )

    return _run(args, sql_fn)


if __name__ == "__main__":
    sys.exit(main())
