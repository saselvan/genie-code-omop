#!/usr/bin/env python3
"""Shared Databricks workspace probes used by scaffolder + bundle-state reader.

Probes are best-effort: each returns ``(value, skip_reason)`` where ``skip_reason``
is ``None`` on success or a short human-readable string on failure. Callers
never see the underlying exception type; the contract is opaque so probes can
evolve their auth strategy (SDK, Spark fallback, etc.) without breaking
downstream code.

Currently exposes:

- ``_probe_existing_tables(core_target, profile=None)`` — list tables in a
  ``catalog.schema`` target. Used by the scaffolder Step 0 flow.
- ``_probe_silver_tables(core_target, profile=None)`` — list tables in a
  ``catalog.schema`` target. Used by ``read_bundle_state``. Same
  semantics as ``_probe_existing_tables`` but accepts ``core_target=None``
  (returns ``([], None)``).

Both probes share ``_list_tables_with_fallback`` which tries the Databricks
SDK's ``WorkspaceClient.tables.list()`` first, then falls back to Spark SQL's
``SHOW TABLES IN <schema>`` if the SDK fails with an auth-flavored error and
a SparkSession is available. This hardens against Genie Code Agent mode's
serverless runtime, where ``WorkspaceClient()`` lacks explicit auth and the
agent has historically had to bypass via Spark SQL.

Auth is handled by Databricks runtime when invoked from Genie Code Agent.
``profile`` only applies for local development against ``~/.databrickscfg``.
"""

from __future__ import annotations

from databricks.sdk import WorkspaceClient

# Class names that indicate the SDK call failed for auth/permissions reasons
# rather than network or schema-not-found. Trigger Spark fallback.
_AUTH_ERROR_CLASS_NAMES: frozenset[str] = frozenset(
    {"PermissionDenied", "Unauthenticated", "Forbidden"}
)

# Substrings in the exception message that indicate auth-flavored failure.
# Matched case-insensitively against ``str(exc)``. "permission" intentionally
# omitted: a generic ``Exception("permission denied")`` (no proper SDK class)
# is treated as a real listing error per scaffolder regression tests; only
# typed PermissionDenied triggers fallback.
_AUTH_ERROR_PHRASES: tuple[str, ...] = (
    "authentication",
    "credentials",
    "unauthorized",
    "unauthenticated",
)


def _is_auth_error(exc: BaseException) -> bool:
    """Heuristic: does this exception look like an auth/permission failure?

    Checks the exception class name first (catches typed SDK errors like
    ``PermissionDenied`` even across SDK versions), then falls back to
    case-insensitive substring search on the message (catches custom-raised
    or wrapped errors).
    """
    if type(exc).__name__ in _AUTH_ERROR_CLASS_NAMES:
        return True
    msg = str(exc).lower()
    return any(phrase in msg for phrase in _AUTH_ERROR_PHRASES)


def _try_spark_fallback(
    target: str, sdk_reason: str
) -> tuple[list[str], str | None]:
    """Last-resort fallback when SDK fails auth-flavored.

    Tries ``SparkSession.getActiveSession()``; if no session is active or
    PySpark isn't importable, returns the SDK reason augmented with the
    fallback skip reason. If SHOW TABLES succeeds, returns the table list
    cleanly (no skip_reason).
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return [], f"{sdk_reason}; PySpark not importable for fallback"

    spark = SparkSession.getActiveSession()
    if spark is None:
        return [], f"{sdk_reason}; no SparkSession available for fallback"

    try:
        rows = spark.sql(f"SHOW TABLES IN {target}").collect()
    except Exception as e:  # pragma: no cover  (covered via mock)
        return (
            [],
            f"{sdk_reason}; Spark fallback failed: {type(e).__name__}: {e}",
        )

    names = [r["tableName"] for r in rows if r["tableName"]]
    return names, None


def _list_tables_with_fallback(
    target: str, profile: str | None
) -> tuple[list[str], str | None]:
    """Two-part-name list-tables probe with SDK-then-Spark fallback.

    Validates the target is ``catalog.schema`` (no SDK call on malformed
    input). Tries WorkspaceClient first; on auth-flavored failure, falls
    back to Spark SQL when a SparkSession is available.

    Public contract is opaque: callers see ``([], reason)`` or
    ``(names, None)`` and never know which path produced the answer.
    """
    parts = target.split(".")
    if len(parts) != 2:
        return [], f"target must be catalog.schema, got: {target}"
    catalog, schema = parts

    try:
        w = WorkspaceClient(profile=profile)
    except Exception as e:
        sdk_reason = f"SDK auth failed: {type(e).__name__}: {e}"
        return _try_spark_fallback(target, sdk_reason)

    try:
        tables = list(w.tables.list(catalog_name=catalog, schema_name=schema))
        return [t.name for t in tables if t.name], None
    except Exception as e:
        listing_reason = (
            f"Could not list tables in {target}: {type(e).__name__}: {e}"
        )
        if not _is_auth_error(e):
            return [], listing_reason
        return _try_spark_fallback(target, listing_reason)


def _probe_existing_tables(
    core_target: str, profile: str | None = None
) -> tuple[list[str], str | None]:
    """List tables in <core_target>. Best-effort, opaque skip-reason on failure.

    Used by the scaffolder Step 0 flow to surface existing OMOP tables. Lifted
    from ``scaffold_omop_project.py`` and hardened to use the SDK-then-Spark
    fallback shared with ``_probe_silver_tables``.
    """
    return _list_tables_with_fallback(core_target, profile)


def _probe_silver_tables(
    core_target: str | None, profile: str | None = None
) -> tuple[list[str], str | None]:
    """List tables in ``<core_target>``. Best-effort.

    ``core_target=None`` short-circuits to ``([], None)`` — no probe attempted,
    no error. Otherwise delegates to the shared SDK-then-Spark fallback.

    Hardens against Genie Code Agent mode's serverless runtime where
    ``WorkspaceClient()`` lacks explicit auth and the agent must bypass via
    ``SHOW TABLES IN <schema>``.
    """
    if core_target is None:
        return [], None
    return _list_tables_with_fallback(core_target, profile)
