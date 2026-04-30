"""Shared warehouse ID resolution for skill scripts."""

def resolve_warehouse_id(profile: str | None = None) -> str:
    """Auto-discover a SQL warehouse ID from the workspace.

    Prefers serverless warehouses. Falls back to any running warehouse.
    Uses DATABRICKS_WAREHOUSE_ID env var if set.
    """
    import os
    wid = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if wid:
        return wid
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    for wh in w.warehouses.list():
        wtype = getattr(wh, "warehouse_type", None)
        serverless = getattr(wh, "enable_serverless_compute", False)
        if ("SERVERLESS" in str(wtype).upper()) or serverless:
            return wh.id
    for wh in w.warehouses.list():
        if str(getattr(wh, "state", "")).upper() == "RUNNING":
            return wh.id
    raise RuntimeError("No SQL warehouse found. Set DATABRICKS_WAREHOUSE_ID or start a warehouse.")
