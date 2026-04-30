#!/usr/bin/env python3
"""Start a Databricks Lakeflow / Delta Pipeline update and poll to completion."""

from __future__ import annotations

import argparse
import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import UpdateInfoState


def _resolve_pipeline_id(w: WorkspaceClient, *, pipeline_id: str | None, name: str | None) -> str:
    if pipeline_id:
        return pipeline_id
    if not name:
        raise SystemExit("Provide --pipeline-id or --pipeline-name")
    pipelines = list(w.pipelines.list_pipelines())
    exact = [p for p in pipelines if p.name == name]
    if len(exact) == 1:
        return exact[0].pipeline_id
    if len(exact) > 1:
        raise SystemExit(
            f"Multiple pipelines exactly named '{name}': {[p.pipeline_id for p in exact]}. "
            "Use --pipeline-id to disambiguate."
        )
    # Fall back to word-boundary contains match — useful under DAB dev mode where
    # names are wrapped like '[dev user] omop_person_target'.
    contains = [p for p in pipelines if name in (p.name or "")]
    if len(contains) == 1:
        return contains[0].pipeline_id
    if len(contains) > 1:
        hits = [f"{p.name} ({p.pipeline_id})" for p in contains]
        raise SystemExit(
            f"Ambiguous pipeline name '{name}' — matched {len(contains)} pipelines: {hits}. "
            "Use --pipeline-id to disambiguate."
        )
    raise SystemExit(f"Pipeline named '{name}' not found")


def _state_str(state: Any) -> str:
    if state is None:
        return "UNKNOWN"
    if isinstance(state, UpdateInfoState):
        return state.value
    return str(state)


def _print_update_snapshot(update: Any) -> None:
    state = _state_str(getattr(update, "state", None))
    print(f"  state={state!r} update_id={getattr(update, 'update_id', None)!r}")
    cause = getattr(update, "cause", None)
    if cause is not None:
        print(f"  cause={cause!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Start and poll a Databricks pipeline update.")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--pipeline-id", help="Pipeline UUID")
    g.add_argument("--pipeline-name", help="Exact pipeline name in workspace")
    parser.add_argument(
        "--table",
        default=None,
        help="OMOP target table name passed as pipeline parameters.table_name",
    )
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--profile", default=None, help="Databricks CLI auth profile")
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=10,
        help="Polling interval (default 10)",
    )
    parser.add_argument(
        "--max-wait-seconds",
        type=int,
        default=1800,
        help="Max wait time (default 1800 = 30 min)",
    )
    args = parser.parse_args()

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    pid = _resolve_pipeline_id(w, pipeline_id=args.pipeline_id, name=args.pipeline_name)

    params: dict[str, str] | None = None
    if args.table:
        params = {"table_name": args.table}

    print(f"Starting update for pipeline {pid} (full_refresh={args.full_refresh})")
    if params:
        print(f"  parameters={params}")

    start = w.pipelines.start_update(
        pipeline_id=pid,
        full_refresh=args.full_refresh,
        parameters=params,
    )
    update_id = getattr(start, "update_id", None) or getattr(start, "id", None)
    if not update_id:
        print(f"Start response: {start!r}")
        raise SystemExit("Could not read update_id from start_update response")

    print(f"update_id={update_id!r} — polling every {args.poll_seconds}s ...")
    deadline = time.time() + args.max_wait_seconds
    last_state = None
    while time.time() < deadline:
        resp = w.pipelines.get_update(pipeline_id=pid, update_id=update_id)
        info = getattr(resp, "update", None)
        if info is None:
            print(f"Unexpected get_update payload: {resp!r}")
            time.sleep(max(1, args.poll_seconds))
            continue
        state = _state_str(getattr(info, "state", None))
        if state != last_state:
            _print_update_snapshot(info)
            last_state = state
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            print(f"Final state: {state!r} update_id={update_id!r}")
            raise SystemExit(0 if state == "COMPLETED" else 1)
        time.sleep(max(1, args.poll_seconds))

    print("Timed out waiting for pipeline update.")
    raise SystemExit(2)


if __name__ == "__main__":
    main()
