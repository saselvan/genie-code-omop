#!/usr/bin/env python3
"""Read the current state of an OMOP project bundle from disk.

Phase 1 of omop-pipeline-builder v2.0 introduces state-aware workflow:
before generating new configs or wiring tasks, the agent reads what already
exists and branches accordingly. This module is the read-only state probe
that produces a structured `BundleState` snapshot.

Decision 7: re-read on every invocation. No manifest, no caching.
Decision 10: Git-backed bundles are the recommended default; Git status is
part of state. Decision 12: this module does not deploy.

CLI is a developer-loop tool. Programmatic callers (Phase 2's
`classify_request`, Phase 3's update workflow) should use
``read_bundle_state()`` directly.

Auth is handled by Databricks runtime when invoked from Genie Code Agent.
``--profile`` only applies for local development against ``~/.databrickscfg``.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

_MARKER_FILENAME = ".omop-skill-version"

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


def _list_yaml_configs(configs_dir: Path) -> tuple[list[str], list[str]]:
    """Return (configs_present, ambiguous_configs) for a configs/ directory.

    ``configs_present`` is the sorted list of YAML filenames (basenames with
    extension). ``ambiguous_configs`` is the sorted list of stems that have
    BOTH ``.yaml`` and ``.yml`` siblings — a Phase 2 hard signal that the
    agent must refuse to process the table until the engineer resolves the
    extension conflict.

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
    returned in BOTH lists. Phase 2 consumers treat the wired entry as
    authoritative when both are present; this parser does not editorialize.
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
    """STUB — implemented in Phase 1 Step 2.

    Will list tables in ``<core_target>`` using SDK, falling back to Spark
    SQL when the SDK auth fails. Same opaque ``([], skip_reason)`` contract
    as ``_probe_existing_tables``.
    """
    raise NotImplementedError("step 2: _probe_silver_tables")


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

    try:
        branch_proc = _run_git(project_path, "rev-parse", "--abbrev-ref", "HEAD")
        status_proc = _run_git(project_path, "status", "--porcelain")
    except subprocess.TimeoutExpired:
        return (
            GitStatus(is_git_repo=True),
            "git rev-parse/status timed out",
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
    """STUB — implemented in Phase 1 Step 4."""
    raise NotImplementedError("step 4: read_bundle_state")
