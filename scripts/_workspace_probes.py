#!/usr/bin/env python3
"""Shared Databricks workspace probes used by scaffolder + bundle-state reader.

Probes are best-effort: each returns ``(value, skip_reason)`` where ``skip_reason``
is ``None`` on success or a short human-readable string on failure. Callers
never see the underlying exception type; the contract is opaque so probes can
evolve their auth strategy (SDK, Spark fallback, etc.) without breaking
downstream code.

Currently exposes:

- ``_probe_existing_tables(core_target, profile=None)`` — list tables in a
  ``catalog.schema`` target. Lifted unchanged from ``scaffold_omop_project.py``
  in Phase 1 Step 1; Step 2 hardens it with an SDK-then-Spark fallback.

Auth is handled by Databricks runtime when invoked from Genie Code Agent.
``profile`` only applies for local development against ``~/.databrickscfg``.
"""

from __future__ import annotations

from databricks.sdk import WorkspaceClient


def _probe_existing_tables(
    core_target: str, profile: str | None = None
) -> tuple[list[str], str | None]:
    """Best-effort probe. Returns ([], reason) on any failure."""
    parts = core_target.split(".")
    if len(parts) != 2:
        return [], f"core_target must be catalog.schema, got: {core_target}"
    catalog, schema = parts

    try:
        w = WorkspaceClient(profile=profile)
    except Exception as e:
        return [], f"SDK auth failed: {type(e).__name__}: {e}"

    try:
        tables = list(w.tables.list(catalog_name=catalog, schema_name=schema))
        return [t.name for t in tables if t.name], None
    except Exception as e:
        return (
            [],
            f"Could not list tables in {core_target}: {type(e).__name__}: {e}",
        )
