#!/usr/bin/env python3
"""Read the current state of an OMOP project bundle from disk.

State-aware workflow: before generating new configs or wiring tasks,
the agent reads what already exists and branches accordingly. This
module is the read-only state probe that produces a structured
`BundleState` snapshot.

Decision 7: re-read on every invocation. No manifest, no caching.
Decision 10: Git-backed bundles are the recommended default; Git status is
part of state. Decision 12: this module does not deploy.

CLI is a developer-loop tool. Programmatic callers (`classify_request`,
the update workflow) should use ``read_bundle_state()`` directly.

Auth is handled by Databricks runtime when invoked from Genie Code Agent.
``--profile`` only applies for local development against ``~/.databrickscfg``.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from _omop_dag import DAG, topological_sort, transitive_predecessors
from _workspace_probes import _probe_silver_tables as _probe_silver_via_helper

_MARKER_FILENAME = ".omop-skill-version"

# Strict canonical form for OMOP table names. Request-classification
# helpers reject any input that does not match this regex with ValueError,
# both for safety (path traversal, weird unicode) and for consistency
# (configs ship as lowercase snake_case, never PascalCase).
_TARGET_TABLE_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# Per-call timeout for individual git subprocess invocations. Generous
# enough for slow filesystems (UC Volume mounts) while still bounding the
# bundle-state read to a few seconds in the worst case.
_GIT_TIMEOUT_SECONDS = 10

_WIRED_TASK_RE = re.compile(r"^\s*-\s*task_key:\s+([A-Za-z0-9_]+)")
_COMMENTED_TASK_RE = re.compile(r"^\s*#\s*-\s*task_key:\s+([A-Za-z0-9_]+)")


@dataclass
class GitStatus:
    """Snapshot of `git status`-derived facts about the project tree."""

    is_git_repo: bool
    branch: str | None = None
    has_uncommitted_changes: bool = False
    untracked_count: int = 0
    modified_count: int = 0


@dataclass
class BundleState:
    """Structured view of an OMOP project's on-disk state.

    All list fields default to empty so partial reads (e.g. a missing
    ``configs/`` directory) compose cleanly. ``*_skip_reason`` fields are
    populated when a probe couldn't run; consumers should treat populated
    skip reasons as "this dimension is unknown" rather than "empty."
    """

    project_path: str
    scaffold_version: str | None = None
    configs_present: list[str] = field(default_factory=list)
    ambiguous_configs: list[str] = field(default_factory=list)
    tasks_wired: list[str] = field(default_factory=list)
    tasks_commented: list[str] = field(default_factory=list)
    silver_tables: list[str] = field(default_factory=list)
    silver_skip_reason: str | None = None
    materialization_diff: list[str] | None = None
    git_status: GitStatus = field(
        default_factory=lambda: GitStatus(is_git_repo=False)
    )
    git_skip_reason: str | None = None


@dataclass
class RequestClassification:
    """Per-table classification of a build request against current bundle state.

    Produced by :func:`classify_request`. Documents what the agent should do
    when the engineer asks to build (or rebuild, or update) ``target_table``.

    ``suggested_action`` is a single-word verdict the agent's prompt
    template branches on; ``requires_branching`` is the boolean shortcut
    for "must offer the three sub-paths (update/replace/different)" before
    proceeding. A True ``requires_branching`` is exactly the
    ``suggested_action='branch'`` case.
    """

    target_table: str
    config_exists: bool
    config_path: str | None
    table_materialized: bool
    task_wired: bool
    requires_branching: bool
    suggested_action: Literal["generate", "branch", "not_scaffolded"]


@dataclass
class BatchClassification:
    """Multi-table classification with topological build order + gap analysis.

    Produced by :func:`classify_batch`. ``build_order`` is the requested
    target list re-sorted to OMOP DAG order. ``unsatisfied_predecessors`` is
    the list of (table, level) tuples for predecessors of the requested
    targets that are NOT in the batch and NOT already materialized at L3 —
    the agent must surface these as a gap before proceeding.

    ``conflicts`` collects the per-table classifications for any requested
    table whose ``suggested_action`` is not ``'generate'`` (i.e., needs
    branching, or the project isn't scaffolded). Consumers can introspect
    ``conflicts[i].suggested_action`` to differentiate; the batch-level
    ``suggested_action`` collapses to one of the three documented verdicts.
    """

    build_order: list[str]
    unsatisfied_predecessors: list[tuple[str, int]]
    conflicts: list[RequestClassification]
    suggested_action: Literal["proceed", "refuse_predecessor", "branch"]


def _list_yaml_configs(configs_dir: Path) -> tuple[list[str], list[str]]:
    """Return (configs_present, ambiguous_configs) for a configs/ directory.

    ``configs_present`` is the sorted list of YAML filenames (basenames with
    extension). ``ambiguous_configs`` is the sorted list of stems that have
    BOTH ``.yaml`` and ``.yml`` siblings — a hard signal during request
    classification that the agent must refuse to process the table until
    the engineer resolves the extension conflict.

    Missing directories return ``([], [])`` — never raises.
    """
    if not configs_dir.exists() or not configs_dir.is_dir():
        return [], []

    yaml_names: list[str] = []
    stems: dict[str, set[str]] = {}
    for entry in configs_dir.iterdir():
        if not entry.is_file():
            continue
        if entry.suffix not in (".yaml", ".yml"):
            continue
        yaml_names.append(entry.name)
        stems.setdefault(entry.stem, set()).add(entry.suffix)

    ambiguous = sorted(
        stem for stem, suffixes in stems.items() if len(suffixes) >= 2
    )
    return sorted(yaml_names), ambiguous


def _parse_jobs_yml_tasks(jobs_yml_path: Path) -> tuple[list[str], list[str]]:
    """Return (wired, commented) task_keys from a jobs.yml.

    Source-of-truth parser is regex-based (not YAML) because commented
    `task_key:` entries are NOT in the parsed YAML. Both wired and commented
    forms tolerate variable whitespace.

    Missing or unreadable file → ``([], [])``. Returns lists in the order
    they appear in the file (DAG ordering already comes from `_omop_dag.py`,
    not from jobs.yml's textual ordering).

    A task that appears both wired AND commented (mid-edit state) is
    returned in BOTH lists. Request-classification consumers treat the
    wired entry as authoritative when both are present; this parser does
    not editorialize.
    """
    if not jobs_yml_path.exists() or not jobs_yml_path.is_file():
        return [], []

    try:
        text = jobs_yml_path.read_text(encoding="utf-8")
    except OSError:
        return [], []

    wired: list[str] = []
    commented: list[str] = []
    for line in text.splitlines():
        m = _COMMENTED_TASK_RE.match(line)
        if m:
            commented.append(m.group(1))
            continue
        m = _WIRED_TASK_RE.match(line)
        if m:
            wired.append(m.group(1))
    return wired, commented


def _read_skill_version(project_path: Path) -> str | None:
    """Return the contents of ``<project>/.omop-skill-version`` or None.

    Strips surrounding whitespace (handles trailing newline + CRLF). Returns
    ``None`` if the marker file is missing OR the file is empty after
    stripping (treat blank marker as absent).
    """
    marker = project_path / _MARKER_FILENAME
    if not marker.exists() or not marker.is_file():
        return None
    try:
        raw = marker.read_text(encoding="utf-8")
    except OSError:
        return None
    stripped = raw.strip()
    return stripped or None


def _probe_silver_tables(
    core_target: str | None, profile: str | None = None
) -> tuple[list[str], str | None]:
    """List tables in ``<core_target>``. Best-effort.

    Thin re-export of the shared probe in ``_workspace_probes``. The
    indirection is intentional: ``read_bundle_state`` patches this name on
    the ``bundle_state`` module in tests, while production code uses the
    same SDK-then-Spark fallback as the scaffolder probe.
    """
    return _probe_silver_via_helper(core_target, profile)


def _run_git(project_path: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run ``git -C <project_path> <args...>`` with text capture and timeout.

    Caller wraps in try/except for ``FileNotFoundError`` (no git installed)
    and ``subprocess.TimeoutExpired``. Other failures (non-zero exit) are
    surfaced via ``CompletedProcess.returncode``; the helper does not raise
    on non-zero exit so the caller can branch deterministically.
    """
    return subprocess.run(  # noqa: S603,S607  (intentional, args are static)
        ["git", "-C", project_path, *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=_GIT_TIMEOUT_SECONDS,
    )


def _probe_git_status(project_path: str) -> tuple[GitStatus, str | None]:
    """Best-effort Git status probe. Three subprocess calls, never raises.

    Steps:
      1. ``git rev-parse --is-inside-work-tree`` — am I in a repo?
      2. ``git rev-parse --abbrev-ref HEAD`` — what branch? (returns
         literal ``HEAD`` for detached-HEAD state)
      3. ``git status --porcelain`` — count untracked vs modified

    On success returns ``(GitStatus(...), None)``. On any failure (no git
    binary, non-repo path, timeout, etc.) returns ``(GitStatus(...defaults),
    reason)`` so the caller can include the skip reason in the bundle
    state without losing the structured shape.
    """
    try:
        is_repo_proc = _run_git(project_path, "rev-parse", "--is-inside-work-tree")
    except FileNotFoundError:
        return GitStatus(is_git_repo=False), "git command not found"
    except subprocess.TimeoutExpired:
        return GitStatus(is_git_repo=False), "git command timed out"
    except OSError as e:
        return GitStatus(is_git_repo=False), f"git invocation failed: {type(e).__name__}: {e}"

    if is_repo_proc.returncode != 0:
        return GitStatus(is_git_repo=False), "Not a git repository"

    # Second + third git calls. Mirror the breadth of error handling from
    # the first call so the documented "never raises" contract holds even
    # if the git binary disappears between calls or the OS surfaces a
    # transient PermissionError on /. (PE-review fix.)
    try:
        branch_proc = _run_git(project_path, "rev-parse", "--abbrev-ref", "HEAD")
        status_proc = _run_git(project_path, "status", "--porcelain")
    except FileNotFoundError:
        return GitStatus(is_git_repo=True), "git command disappeared mid-probe"
    except subprocess.TimeoutExpired:
        return GitStatus(is_git_repo=True), "git rev-parse/status timed out"
    except OSError as e:
        return (
            GitStatus(is_git_repo=True),
            f"git invocation failed: {type(e).__name__}: {e}",
        )

    if branch_proc.returncode != 0:
        return (
            GitStatus(is_git_repo=True),
            f"git rev-parse failed: {branch_proc.stderr.strip()}",
        )

    branch = branch_proc.stdout.strip() or None

    if status_proc.returncode != 0:
        return (
            GitStatus(is_git_repo=True, branch=branch),
            f"git status failed: {status_proc.stderr.strip()}",
        )

    untracked, modified = _count_porcelain_lines(status_proc.stdout)
    has_changes = (untracked + modified) > 0

    return (
        GitStatus(
            is_git_repo=True,
            branch=branch,
            has_uncommitted_changes=has_changes,
            untracked_count=untracked,
            modified_count=modified,
        ),
        None,
    )


def _count_porcelain_lines(porcelain: str) -> tuple[int, int]:
    """Count untracked vs modified lines from ``git status --porcelain`` output.

    Format: each non-empty line is two columns of status code followed by a
    space and the path. ``??`` = untracked; anything else (``M``, `` M``,
    ``A``, ``MM``, ``R``, ``C``, etc.) is counted as modified. Don't
    over-parse — counts are sufficient for the agent's "is this tree
    clean?" decision.
    """
    untracked = 0
    modified = 0
    for line in porcelain.splitlines():
        if not line:
            continue
        if line.startswith("??"):
            untracked += 1
        else:
            modified += 1
    return untracked, modified


def read_bundle_state(
    project_path: str,
    core_target: str | None = None,
    profile: str | None = None,
    probe_silver: bool = True,
    probe_git: bool = True,
    previous_silver_tables: list[str] | None = None,
) -> BundleState:
    """Read the current state of an OMOP project bundle from disk.

    Returns structured state describing what configs exist, what tasks
    are wired into ``jobs.yml``, what tables exist in ``core_target``, and
    what Git status the project tree is in. Reads ``.omop-skill-version``
    if present.

    If ``previous_silver_tables`` is provided, populates
    ``materialization_diff`` with newly-appearing tables (tables in the
    current probe result that were not in the previous list).

    Best-effort: any probe (silver, git) that fails returns a populated
    skip_reason in the result; the function never raises for missing
    data. Raises ``ValueError`` only for ``project_path`` that doesn't
    exist or isn't a directory.

    Decision 7: re-reads from disk on every call. No caching.
    """
    project = Path(project_path)
    if not project.exists():
        raise ValueError(f"project_path does not exist: {project_path}")
    if not project.is_dir():
        raise ValueError(f"project_path is not a directory: {project_path}")

    scaffold_version = _read_skill_version(project)
    configs_present, ambiguous_configs = _list_yaml_configs(project / "configs")
    tasks_wired, tasks_commented = _parse_jobs_yml_tasks(
        project / "resources" / "jobs.yml"
    )

    silver_tables, silver_skip_reason = _read_silver_section(
        core_target=core_target,
        profile=profile,
        probe_silver=probe_silver,
    )

    materialization_diff = _compute_materialization_diff(
        current=silver_tables,
        previous=previous_silver_tables,
        silver_skip_reason=silver_skip_reason,
    )

    git_status, git_skip_reason = _read_git_section(
        project_path=str(project),
        probe_git=probe_git,
    )

    return BundleState(
        project_path=str(project),
        scaffold_version=scaffold_version,
        configs_present=configs_present,
        ambiguous_configs=ambiguous_configs,
        tasks_wired=tasks_wired,
        tasks_commented=tasks_commented,
        silver_tables=silver_tables,
        silver_skip_reason=silver_skip_reason,
        materialization_diff=materialization_diff,
        git_status=git_status,
        git_skip_reason=git_skip_reason,
    )


def _validate_canonical_table(target_table: str, ambiguous: list[str]) -> None:
    """Raise ValueError if `target_table` is non-canonical or ambiguous.

    Shared gate for ``classify_request`` and ``classify_batch``. Two
    independent failure modes are checked, in order:

    1. **Regex** (``^[a-z][a-z0-9_]*$``) — rejects PascalCase, unicode,
       slashes, dots, leading digits, and obvious traversal attempts like
       ``../../etc/passwd``.
    2. **Ambiguity** — refuses if the project has both ``<stem>.yaml`` and
       ``<stem>.yml`` for this stem. The caller (the agent) must ask the
       engineer to delete one before any classification is meaningful.
    """
    if not _TARGET_TABLE_RE.match(target_table):
        raise ValueError(
            f"target_table must be lowercase snake_case matching "
            f"^[a-z][a-z0-9_]*$, got: {target_table!r}"
        )
    if target_table in ambiguous:
        raise ValueError(
            f"target_table {target_table!r} is ambiguous: both "
            f"{target_table}.yaml and {target_table}.yml exist in configs/. "
            "Delete or rename one before proceeding."
        )


def _resolve_config_path(
    project_path: str, target_table: str, configs_present: list[str]
) -> tuple[bool, str | None]:
    """Return (config_exists, config_path) for `target_table` in this project.

    Prefers ``.yaml`` when only one extension is present (and the ambiguity
    case has already been excluded by ``_validate_canonical_table``).
    """
    yaml_name = f"{target_table}.yaml"
    yml_name = f"{target_table}.yml"
    if yaml_name in configs_present:
        return True, str(Path(project_path) / "configs" / yaml_name)
    if yml_name in configs_present:
        return True, str(Path(project_path) / "configs" / yml_name)
    return False, None


def classify_request(
    state: BundleState, target_table: str
) -> RequestClassification:
    """Classify a single-table build request against the current bundle state.

    The agent calls this at the top of the per-table workflow (Step 2 of
    the v2.0 SKILL.md). It is the contract that drives the three-sub-path
    branching — update / replace / different table — when an existing
    config is detected, and it is the gate that surfaces "scaffold first"
    when the project hasn't been initialized.

    Args:
        state: Current bundle state from :func:`read_bundle_state`.
        target_table: Lowercase canonical OMOP table name (e.g. ``"person"``).

    Returns:
        A populated :class:`RequestClassification`.

    Raises:
        ValueError: ``target_table`` is non-canonical (regex reject) or
            ambiguous (both ``.yaml`` and ``.yml`` present in configs/).
    """
    _validate_canonical_table(target_table, state.ambiguous_configs)

    config_exists, config_path = _resolve_config_path(
        state.project_path, target_table, state.configs_present
    )
    table_materialized = target_table in state.silver_tables
    task_wired = target_table in state.tasks_wired

    if state.scaffold_version is None:
        suggested: Literal["generate", "branch", "not_scaffolded"] = "not_scaffolded"
    elif config_exists:
        suggested = "branch"
    else:
        suggested = "generate"

    return RequestClassification(
        target_table=target_table,
        config_exists=config_exists,
        config_path=config_path,
        table_materialized=table_materialized,
        task_wired=task_wired,
        requires_branching=(suggested == "branch"),
        suggested_action=suggested,
    )


def _table_state_level(state: BundleState, table: str) -> int:
    """Return the L0-L3 state level for ``table`` in the given bundle state.

    Levels (per the request-classification spec):

    - **L0** — no config locally. (Includes the anomaly where a table
      appears in ``silver_tables`` with no local config: the skill
      cannot manage it through this workflow, so the predecessor is
      reported as L0 and surfaces in ``unsatisfied_predecessors``.)
    - **L1** — config exists; task is commented, absent, or only
      present in ``tasks_commented``; table not yet materialized — OR,
      the silver-only anomaly: config exists, task is NOT wired, but
      the table happens to appear in ``silver_tables``. We still return
      L1 in this case because the skill-managed gap (an unwired task)
      is the actionable signal, regardless of how the table got
      materialized. The agent should treat L1 as "needs further work
      before this is a batch dependency it can rely on."
    - **L2** — config exists, task wired, table not yet materialized.
    - **L3** — config exists, task wired, table materialized. The
      "done" state — predecessors at L3 are satisfied without needing
      inclusion in a downstream batch.
    """
    has_config = (
        f"{table}.yaml" in state.configs_present
        or f"{table}.yml" in state.configs_present
    )
    if not has_config:
        return 0
    has_wired_task = table in state.tasks_wired
    has_table = table in state.silver_tables
    if has_wired_task and has_table:
        return 3
    if has_wired_task:
        return 2
    return 1


def classify_batch(
    state: BundleState, target_tables: list[str]
) -> BatchClassification:
    """Classify a multi-table build request.

    Given a list of target tables, returns a topologically-ordered build
    plan, identifies missing predecessors not in the batch, and collects
    per-table conflicts (existing configs needing branching, OR projects
    that aren't scaffolded). The agent's batch response template branches
    on ``suggested_action``:

    - ``'proceed'`` — all targets are at L0 with predecessors satisfied;
      generate them in ``build_order``.
    - ``'refuse_predecessor'`` — at least one predecessor is not in the
      batch and not at L3; surface the gap and ask the engineer to add it.
    - ``'branch'`` — at least one requested table needs the agent to talk
      to the engineer before proceeding. This collapses two distinct
      situations into one verdict: an existing config that needs
      update/replace/different-table branching, OR a project that isn't
      scaffolded yet. Consumers must introspect ``conflicts[i].suggested_action``
      to differentiate ``'branch'`` (resolve config) from
      ``'not_scaffolded'`` (run scaffolder first). When ``'branch'``
      fires, ``unsatisfied_predecessors`` may also be populated; the
      verdict simply reflects that branching dominates as the next
      conversational step, not that predecessors are satisfied.

    Args:
        state: Current bundle state.
        target_tables: Requested target tables. Order is irrelevant — the
            helper sorts to OMOP DAG order.

    Returns:
        A populated :class:`BatchClassification`.

    Raises:
        ValueError: any element of ``target_tables`` is non-canonical or
            ambiguous (re-uses :func:`classify_request`'s gates).
        KeyError: any element of ``target_tables`` is not a known OMOP
            table in :data:`_omop_dag.DAG`.
    """
    # Pre-validate every input first so canonical-form violations surface as
    # ValueError before topological_sort sees the input and turns them into
    # KeyError instead. KeyError is reserved for "regex-clean name not in DAG."
    for t in target_tables:
        _validate_canonical_table(t, state.ambiguous_configs)

    targets_set = set(target_tables)
    build_order = topological_sort(targets_set) if targets_set else []

    per_table = [classify_request(state, t) for t in build_order]
    conflicts = [c for c in per_table if c.suggested_action != "generate"]

    unsatisfied: list[tuple[str, int]] = []
    seen: set[str] = set()
    for table in build_order:
        for predecessor in transitive_predecessors(table):
            if predecessor in targets_set:
                continue
            if predecessor in seen:
                continue
            seen.add(predecessor)
            level = _table_state_level(state, predecessor)
            if level < 3:
                unsatisfied.append((predecessor, level))
    unsatisfied.sort()

    if conflicts:
        suggested: Literal["proceed", "refuse_predecessor", "branch"] = "branch"
    elif unsatisfied:
        suggested = "refuse_predecessor"
    else:
        suggested = "proceed"

    return BatchClassification(
        build_order=build_order,
        unsatisfied_predecessors=unsatisfied,
        conflicts=conflicts,
        suggested_action=suggested,
    )


def _read_silver_section(
    *, core_target: str | None, profile: str | None, probe_silver: bool
) -> tuple[list[str], str | None]:
    """Resolve the silver-probe section of a bundle-state read.

    Precedence:
      1. ``probe_silver=False`` → return ``([], "skipped: probe_silver=False")``
         regardless of ``core_target``.
      2. ``core_target=None`` → return ``([], "skipped: no core_target provided")``
         (no SDK call attempted).
      3. Otherwise call the shared probe and surface its result verbatim.
    """
    if not probe_silver:
        return [], "skipped: probe_silver=False"
    if core_target is None:
        return [], "skipped: no core_target provided"
    return _probe_silver_tables(core_target, profile)


def _read_git_section(
    *, project_path: str, probe_git: bool
) -> tuple[GitStatus, str | None]:
    """Resolve the git-probe section of a bundle-state read."""
    if not probe_git:
        return GitStatus(is_git_repo=False), "skipped: probe_git=False"
    return _probe_git_status(project_path)


def _compute_materialization_diff(
    *,
    current: list[str],
    previous: list[str] | None,
    silver_skip_reason: str | None,
) -> list[str] | None:
    """Compute newly-materialized tables since a previous probe.

    Returns ``None`` when ``previous`` is None (caller didn't supply a
    baseline) or when the silver probe was skipped/failed (current view
    is unreliable, so a diff would be misleading). Otherwise returns the
    sorted set difference ``current - previous``.
    """
    if previous is None:
        return None
    if silver_skip_reason is not None:
        return None
    return sorted(set(current) - set(previous))


def _format_state(state: BundleState) -> str:
    """Render a BundleState as a human-readable text block for the CLI.

    Not a stable interface — programmatic callers should use the dataclass
    directly. Intended for developer-loop debugging output.
    """
    lines: list[str] = []
    lines.append(f"OMOP bundle state: {state.project_path}")
    lines.append(
        f"  scaffold_version: {state.scaffold_version or '<missing>'}"
    )
    lines.append(f"  configs ({len(state.configs_present)}):")
    for name in state.configs_present:
        lines.append(f"    - {name}")
    if state.ambiguous_configs:
        lines.append(
            f"  ambiguous configs ({len(state.ambiguous_configs)}) — "
            "the agent will refuse these until resolved:"
        )
        for stem in state.ambiguous_configs:
            lines.append(f"    ! {stem} (both .yaml and .yml present)")
    lines.append(
        f"  tasks wired ({len(state.tasks_wired)}): "
        f"{', '.join(state.tasks_wired) or '<none>'}"
    )
    lines.append(
        f"  tasks commented ({len(state.tasks_commented)}): "
        f"{', '.join(state.tasks_commented) or '<none>'}"
    )

    if state.silver_skip_reason:
        lines.append(f"  silver tables: <skipped> {state.silver_skip_reason}")
    else:
        lines.append(f"  silver tables ({len(state.silver_tables)}):")
        for name in state.silver_tables:
            lines.append(f"    - {name}")

    if state.materialization_diff is not None:
        lines.append(
            f"  newly materialized vs previous "
            f"({len(state.materialization_diff)}): "
            f"{', '.join(state.materialization_diff) or '<none>'}"
        )

    if state.git_skip_reason:
        lines.append(f"  git: <skipped> {state.git_skip_reason}")
    elif state.git_status.is_git_repo:
        gs = state.git_status
        lines.append(
            f"  git: branch={gs.branch or '<unknown>'} "
            f"dirty={gs.has_uncommitted_changes} "
            f"untracked={gs.untracked_count} modified={gs.modified_count}"
        )
    else:
        lines.append("  git: not a git repository")
    return "\n".join(lines)


def _cli(argv: list[str] | None = None) -> int:
    """Developer-loop debugging CLI. Not a customer-facing interface.

    Programmatic callers (``classify_request``, the update workflow)
    should call ``read_bundle_state()`` directly.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Read the current state of an OMOP project bundle. Developer-"
            "loop tool; programmatic callers should use read_bundle_state()."
        )
    )
    parser.add_argument(
        "--project-path",
        required=True,
        help="Filesystem path of the OMOP project tree.",
    )
    parser.add_argument(
        "--core-target",
        default=None,
        help="Two-part UC name catalog.schema. Required to probe silver tables.",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile for SDK auth (local development only).",
    )
    parser.add_argument(
        "--no-probe-silver",
        dest="probe_silver",
        action="store_false",
        default=True,
        help="Skip the silver tables probe (no SDK call).",
    )
    parser.add_argument(
        "--no-probe-git",
        dest="probe_git",
        action="store_false",
        default=True,
        help="Skip the git status probe (no subprocess call).",
    )

    args = parser.parse_args(argv)

    try:
        state = read_bundle_state(
            project_path=args.project_path,
            core_target=args.core_target,
            profile=args.profile,
            probe_silver=args.probe_silver,
            probe_git=args.probe_git,
        )
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    print(_format_state(state))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
