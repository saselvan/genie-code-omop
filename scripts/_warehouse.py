"""Shared warehouse ID resolution for skill scripts.

Strict superset of the pre-deletion ``_resolve_warehouse_id`` helper from
``validate_omop.py`` (see ``e20c834~1``). Same behavior, broader signature:
adds ``profile`` (for local dev fresh-client construction) and ``client``
(for client injection in tests and to avoid double-constructing
``WorkspaceClient`` when the caller already has one).
"""

from __future__ import annotations

import os

from databricks.sdk import WorkspaceClient


def resolve_warehouse_id(
    explicit: str | None = None,
    *,
    profile: str | None = None,
    client: WorkspaceClient | None = None,
) -> str:
    """Resolve a SQL warehouse ID via explicit arg, env var, or auto-discovery.

    Priority order:
      1. ``explicit`` arg (typically from ``--warehouse-id``).
      2. ``DATABRICKS_WAREHOUSE_ID`` env var.
      3. SDK auto-discovery: first warehouse that is BOTH running AND
         serverless. No fallback to non-serverless warehouses — see the
         "behavior contract" note below.
      4. ``SystemExit`` with a user-actionable message naming all three
         resolution paths.

    Args:
        explicit: Caller-provided override. When non-None and non-empty,
            returned as-is without further resolution.
        profile: Databricks CLI profile for local dev. Ignored when
            ``client`` is provided. When ``client`` is None and
            auto-discovery is needed, used to construct
            ``WorkspaceClient(profile=profile)``.
        client: Pre-constructed ``WorkspaceClient`` for SDK calls. When
            provided, ``profile`` is ignored. Use this to inject mocks
            in tests, or to reuse an existing client and avoid
            constructing a second one.

    Returns:
        The resolved SQL warehouse ID.

    Raises:
        SystemExit: No warehouse could be resolved via any of the three
            paths. Message names ``--warehouse-id``,
            ``DATABRICKS_WAREHOUSE_ID``, and the start-a-warehouse
            remediation so the caller can act without consulting docs.
            Message text is preserved verbatim from the pre-deletion
            inline helper because ``recommended_ci_config.md`` and the
            v2.0.3 rehearsal acceptance matrix both expect to see this
            exact wording.

    Behavior contract — why no "any running" fallback:
        The pre-deletion helper required running-AND-serverless and
        raised otherwise. The earlier ``_warehouse.py`` (zero callers
        before v2.0.3) had a "fall back to any running warehouse"
        path. We preserve the pre-deletion semantics, not the earlier
        ``_warehouse.py`` semantics, because:
          1. The ``SystemExit`` message names "serverless" — silently
             returning a non-serverless warehouse would make that
             message misleading in the failure case.
          2. ``recommended_ci_config.md`` documents the validator as
             serverless-warehouse-driven; falling back to non-serverless
             would be an undocumented behavior expansion.
          3. ``_warehouse.py`` had zero callers before v2.0.3, so
             tightening to pre-deletion semantics breaks no current
             caller.
    """
    if explicit:
        return explicit
    env = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if env:
        return env
    w = client or (WorkspaceClient(profile=profile) if profile else WorkspaceClient())
    for wh in w.warehouses.list():
        state = getattr(wh, "state", None)
        wtype = getattr(wh, "warehouse_type", None)
        is_running = str(state).upper().endswith("RUNNING")
        is_serverless = "SERVERLESS" in str(wtype).upper() or bool(
            getattr(wh, "enable_serverless_compute", False)
        )
        if is_running and is_serverless and wh.id:
            return wh.id
    raise SystemExit(
        "No running serverless warehouse found. Pass --warehouse-id, "
        "set DATABRICKS_WAREHOUSE_ID, or start a warehouse in the workspace."
    )
