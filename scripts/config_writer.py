#!/usr/bin/env python3
"""Write OMOP YAML configs to a scaffolded project's configs/ directory.

The shared write surface for the Update, Replace, and Generate sub-paths.
When the engineer chooses "Update" from Step 2's three sub-paths, the
agent regenerates the whole config and writes the new version through
this module. ``write_config`` is the single shared write surface for the
Update and Replace sub-paths; the Generate sub-path (greenfield) writes
through this same surface with ``overwrite=False``.

Decision 9: writes the WHOLE regenerated config — never a textual diff.
Decision 10: VCS-agnostic. The writer issues only **read-only** git
probes (``git rev-parse`` / ``git status``) for surfacing
``WriteResult.git_warning``; it never mutates the repository state
and never blocks the write on git results. The engineer commits
through their own workflow.
Decision 11: agent writes, engineer commits. The writer does not
``git add``, ``git commit``, or any other git mutation.

This file ships the Step 1 + 2 + 3 + 4 surface:
  - greenfield write (file does not exist)
  - bytes-for-bytes preservation
  - overwrite handling (FileExistsError when ``overwrite=False``,
    atomic replace when ``overwrite=True``)
  - atomic write via tempfile + ``os.replace`` (no partial-write
    corruption; original preserved on any failure)
  - Git status surfacing in ``WriteResult.git_warning``: ``None`` for
    git'd projects, honest Decision-10 text for non-Git'd projects,
    "version-control state unknown" for probe failures
  - **Stable-mode** ``expected_mtime`` concurrency check: when the
    caller passes ``expected_mtime``, the writer raises
    :class:`MtimeMismatchError` if the on-disk mtime differs (or the
    target file is gone). Stable mode is the option (b) branch from
    the FUSE-mtime stability spike; the unstable-mode informational-
    warning branch is not shipped because the spike concluded the
    Volume FUSE mtime is stable enough to block on.
  - ``WriteResult.mtime_warning`` stays ``None`` in stable mode and
    exists only as a forward-compatible field for future builds that
    might run against an unstable FUSE mount.

Auth boilerplate not applicable (pure filesystem; no SDK calls).
"""

from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

# Same-skill internal import: bundle_state houses the Git probe (it
# lives there rather than in _workspace_probes because Git status is a
# local-filesystem concern, not a workspace/SDK concern). We import the
# private symbol on purpose — both modules ship together in this skill
# and the boundary is ours to draw.
from bundle_state import _probe_git_status

# Canonical lowercase snake_case regex — matches
# bundle_state._TARGET_TABLE_RE. Duplicated rather than imported to keep
# config_writer.py independent of bundle_state's import graph (which
# pulls _workspace_probes and the Databricks SDK transitively). The two
# regexes must stay in lockstep; a future module-level constant that
# both consume could deduplicate, but for now a one-line duplication is
# the lowest-risk choice.
_TARGET_TABLE_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# Decision-10 honest warning text for non-Git'd projects. Verbatim from
# the Update sub-path spec — names cloud-side storage versioning as the
# alternative because the skill itself ships no snapshot mechanism.
# The text is informational only; the writer never blocks on this.
_GIT_WARNING_NOT_A_REPO = (
    "Project is not under git version control. The skill does not snapshot "
    "configs — without git or cloud-side storage versioning, this overwrite "
    "is not recoverable. Recommended: connect this project to git before "
    "further updates. Alternative: ask your platform team to enable "
    "versioning on the storage account backing this Volume."
)


# Mtime comparison tolerance, in seconds. Real concurrent writes
# advance mtime by milliseconds-to-seconds; this tolerance only
# absorbs the float-precision loss that happens when the caller
# round-trips ``expected_mtime`` through JSON or a database column
# whose precision is lower than ``os.stat().st_mtime``'s nanosecond
# representation. 1 microsecond is well below any realistic write
# spacing on a Volume FUSE mount per the FUSE-mtime stability spike.
_MTIME_TOLERANCE_SECONDS = 1e-6


class MtimeMismatchError(Exception):
    """Raised when the on-disk mtime differs from the caller's expectation.

    Stable-mode concurrency guard. The caller (typically the agent
    after reading the bundle state) passes the mtime they observed
    for the target config. ``write_config`` re-stats the file just
    before writing; if the mtime advanced (someone else wrote to the
    file) or the file is gone (someone deleted it), the writer
    refuses to overwrite and raises this exception.

    The exception carries structured fields the agent's prompt
    template can use to compose a "concurrent edit detected" message
    without parsing the string representation:

      - ``target_path``: the absolute path of the contested file
      - ``expected_mtime``: what the caller passed in
      - ``actual_mtime``: what the file currently shows, or ``None``
        if the target was deleted between read and write

    The write itself never starts when this is raised — the file on
    disk remains byte-for-byte unchanged.
    """

    def __init__(
        self,
        target_path: str,
        expected_mtime: float,
        actual_mtime: float | None,
    ) -> None:
        self.target_path = target_path
        self.expected_mtime = expected_mtime
        self.actual_mtime = actual_mtime
        if actual_mtime is None:
            actual_repr = "file does not exist"
        else:
            actual_repr = f"{actual_mtime:.9f}"
        message = (
            f"Mtime mismatch on {target_path}: expected "
            f"{expected_mtime:.9f}, got {actual_repr}. "
            "Another writer modified or removed the config since you "
            "read it. Re-read the bundle state before retrying."
        )
        super().__init__(message)


@dataclass
class WriteResult:
    """Result of a successful :func:`write_config` invocation.

    Returned for every successful write — the writer never raises on
    informational concerns (Git status). Step 4 in stable mode raises
    :class:`MtimeMismatchError` instead of populating
    ``mtime_warning``; the warning field is preserved for forward
    compatibility with a future unstable-mode build.

    Fields:
        config_path: Absolute path of the file that was written.
        bytes_written: Number of bytes the new file contains.
        overwrote_existing: True if a pre-existing file was replaced.
            False for greenfield writes.
        git_warning: Informational text surfaced to the agent's response
            template when the project isn't under git version control.
            Never blocks the write. Populated by Step 3.
        mtime_warning: Forward-compatibility field for future
            unstable-mode builds. Always ``None`` in this build per
            the FUSE-mtime stability spike's conclusion; mtime
            mismatches in stable mode raise :class:`MtimeMismatchError`.
    """

    config_path: str
    bytes_written: int
    overwrote_existing: bool
    git_warning: str | None = None
    mtime_warning: str | None = None


def write_config(
    project_path: str,
    target_table: str,
    yaml_content: str,
    *,
    overwrite: bool = False,
    expected_mtime: float | None = None,
) -> WriteResult:
    """Write a YAML config to ``<project_path>/configs/<target_table>.yaml``.

    **Step 1 + 2 + 3 surface:**

    - Validates ``target_table`` against the canonical lowercase regex
      ``^[a-z][a-z0-9_]*$``; raises :class:`ValueError` on mismatch.
      Same gate ``classify_request`` enforces, repeated here so the
      writer is safe to call directly without a prior classification
      pass. The validation gate fires BEFORE any
      filesystem side effects, so a malicious target name cannot leave
      a stray ``configs/`` behind.
    - Creates ``<project_path>/configs/`` if it doesn't exist (defensive
      against partial scaffolds — the scaffolder always creates it, but
      a customer who deleted the directory should not see an opaque
      ``FileNotFoundError`` from this writer).
    - **Overwrite handling.** If the target file already exists and
      ``overwrite=False`` (the default), raises :class:`FileExistsError`
      and leaves the original byte-for-byte unchanged. With
      ``overwrite=True`` the existing file is replaced atomically.
    - **Atomic write.** The new content is written to a hidden temp
      file in the SAME directory as the target (so ``os.replace``
      stays cross-filesystem-safe), then ``os.replace`` swaps the
      temp file into the target name. ``os.replace`` is atomic on
      POSIX — partial-write failures cannot corrupt the original.
      On any exception during temp-write or replace, the temp file
      is best-effort removed so no orphan ``.tmp`` remains on disk.
    - **Stable-mode concurrency guard.** When the caller passes
      ``expected_mtime``, the writer stats the target file BEFORE
      any side effect; if the actual mtime differs from
      ``expected_mtime`` (modulo a 1 microsecond float-precision
      tolerance), or the target file is gone, raises
      :class:`MtimeMismatchError` and leaves the disk byte-for-byte
      unchanged. ``expected_mtime=None`` (the default) skips the
      check — used by Generate (greenfield) and Replace
      (regenerate-from-scratch) sub-paths. The Update sub-path
      passes the mtime it read from :func:`bundle_state.read_bundle_state`.
    - **Git status surfacing.** AFTER the write succeeds, runs a
      best-effort ``git status`` probe. If the project is under git,
      ``WriteResult.git_warning`` stays ``None`` — the freshly-written
      file showing up as uncommitted is the expected state. If the
      project is NOT under git, sets ``git_warning`` to the
      Decision-10 honest text naming cloud-side storage versioning as
      the alternative. If the probe itself fails (no git binary,
      timeout, etc.), sets a "version-control state unknown" message
      that names the underlying reason.

    **Platform notes:**

    - ``os.replace`` is documented atomic on POSIX (Databricks runtime
      on Linux); on macOS dev environments it is atomic for same-
      filesystem replaces but the OS may surface different
      semantics if the target file is held open by an editor at the
      moment of replace. The writer does not coordinate with editors.
    - File permissions on the new file are the OS default (umask).
      OMOP configs are not permission-sensitive.

    Args:
        project_path: Filesystem path of the scaffolded project tree.
        target_table: Lowercase canonical OMOP table name (e.g. ``"person"``).
        yaml_content: The YAML document to write, as a string. Empty
            content is allowed — the writer does not validate YAML
            syntax; that's the agent's responsibility (the agent runs
            Pydantic validation BEFORE calling this function).
        overwrite: When True, replaces an existing target file
            atomically. When False (default), an existing target file
            causes :class:`FileExistsError`.
        expected_mtime: Concurrency guard. When non-None, the writer
            stats the target file before writing and raises
            :class:`MtimeMismatchError` if the on-disk mtime differs
            (or the target is gone). When None (default), skips the
            check — used by Generate (greenfield) and Replace
            (regenerate-from-scratch) sub-paths. Compare uses a
            1 microsecond float-precision tolerance to absorb JSON /
            DB round-trip loss.

    Returns:
        :class:`WriteResult` describing the write. ``overwrote_existing``
        is True iff ``overwrite=True`` and the target file existed
        before the write. ``git_warning`` is populated per the rules in
        :func:`_resolve_git_warning`. ``mtime_warning`` is always None
        in this build (stable-mode mtime drift raises instead).

    Raises:
        ValueError: ``target_table`` does not match the canonical regex.
        FileExistsError: target file exists and ``overwrite=False``.
        MtimeMismatchError: ``expected_mtime`` is set AND the on-disk
            mtime differs by more than 1 microsecond, OR the target
            file no longer exists. Disk is unchanged when this raises.
    """
    if not _TARGET_TABLE_RE.match(target_table):
        raise ValueError(
            f"target_table must be lowercase snake_case matching "
            f"^[a-z][a-z0-9_]*$, got: {target_table!r}"
        )

    configs_dir = Path(project_path) / "configs"
    target_path = configs_dir / f"{target_table}.yaml"

    # Stable-mode concurrency guard. Runs BEFORE any filesystem side
    # effect (no mkdir, no tempfile) so a failed check leaves the
    # caller's tree exactly as it was. The on-disk stat happens just
    # in time to minimize the race window between the check and the
    # subsequent atomic write — the Update sub-path spec accepts a
    # millisecond-scale window because the failure modes this protects
    # against (multi-engineer / multi-tab) operate at second+ scales.
    if expected_mtime is not None:
        _enforce_expected_mtime(target_path, expected_mtime)

    configs_dir.mkdir(parents=True, exist_ok=True)
    pre_existing = target_path.exists()

    if pre_existing and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing config: {target_path}. "
            "Pass overwrite=True for the Replace or Update sub-paths."
        )

    encoded = yaml_content.encode("utf-8")
    _atomic_write_bytes(configs_dir, target_path, encoded, target_table)

    git_warning = _resolve_git_warning(project_path)

    return WriteResult(
        config_path=str(target_path),
        bytes_written=len(encoded),
        overwrote_existing=pre_existing,
        git_warning=git_warning,
        mtime_warning=None,
    )


def _enforce_expected_mtime(target_path: Path, expected_mtime: float) -> None:
    """Raise :class:`MtimeMismatchError` if the on-disk mtime drifted.

    Stable-mode concurrency guard. Three outcomes:

    1. Target file exists and ``actual_mtime`` is within tolerance of
       ``expected_mtime`` — returns silently; the caller proceeds to
       write.
    2. Target file exists but ``actual_mtime`` is outside tolerance —
       raises with both expected and actual on the exception. Disk is
       not yet touched (configs/ may also not yet exist).
    3. Target file does NOT exist — raises with ``actual_mtime=None``.
       This catches the "engineer rm'd the file between read and
       write" scenario as a mismatch rather than silently succeeding
       as a greenfield write, which would defeat the concurrency
       guard's purpose.

    A separate helper rather than inlined logic so test fixtures can
    patch it cleanly when simulating the rare-but-possible
    ``stat()``-raises case (e.g., permission flap on a Volume FUSE
    mount). The helper does NOT swallow stat exceptions other than
    ``FileNotFoundError``: an EPERM or ENOTDIR genuinely is a "we
    can't tell what state the file is in" condition, and the writer
    should surface it rather than risk a silent overwrite.
    """
    try:
        actual_mtime = target_path.stat().st_mtime
    except FileNotFoundError:
        raise MtimeMismatchError(
            target_path=str(target_path),
            expected_mtime=expected_mtime,
            actual_mtime=None,
        ) from None

    if abs(actual_mtime - expected_mtime) > _MTIME_TOLERANCE_SECONDS:
        raise MtimeMismatchError(
            target_path=str(target_path),
            expected_mtime=expected_mtime,
            actual_mtime=actual_mtime,
        )


def _resolve_git_warning(project_path: str) -> str | None:
    """Map the bundle_state git probe result to a write-side warning string.

    Three outcomes:

    1. The project IS a git repo (regardless of clean / dirty state) —
       returns ``None``. The freshly-written file showing up as
       uncommitted is the expected state; we don't editorialize.

    2. The project is NOT a git repo (probe returned
       ``"Not a git repository"``) — returns the verbatim
       Decision-10 honest warning text naming cloud-side storage
       versioning as the alternative.

    3. The probe itself failed (no git binary, timeout, OS error,
       unexpected non-zero return code) — returns an honest
       "version-control state is unknown" message that names the
       underlying reason. The write itself succeeded; we surface the
       skip reason so the agent can decide whether to escalate.
    """
    git_status, skip_reason = _probe_git_status(project_path)

    if git_status.is_git_repo:
        return None

    if skip_reason == "Not a git repository":
        return _GIT_WARNING_NOT_A_REPO

    # The probe reported is_git_repo=False AND the reason is something
    # other than "not a repo" — meaning the probe couldn't determine
    # repo status (no git binary, timeout, OS error). Be honest with
    # the agent about that.
    reason = skip_reason or "unknown reason"
    return (
        f"Git status check failed: {reason}. "
        "Write succeeded but version-control state is unknown."
    )


def _atomic_write_bytes(
    configs_dir: Path,
    target_path: Path,
    encoded: bytes,
    target_table: str,
) -> None:
    """Atomically write ``encoded`` to ``target_path`` via tempfile + os.replace.

    The tempfile is created in the SAME directory as the target so
    ``os.replace`` stays on the same filesystem (the cross-filesystem
    case is undefined behavior on some platforms). On any exception
    during write or replace, the tempfile is best-effort removed so no
    orphan ``.tmp`` remains.

    Note: not exposed publicly — only used by :func:`write_config`. The
    helper exists so test fixtures can patch it cleanly to simulate
    mid-write failures (e.g., simulating a disk-full ``OSError`` from
    ``os.replace`` to verify the original file remains intact).
    """
    fd, tmp_name = tempfile.mkstemp(
        dir=str(configs_dir),
        prefix=f".{target_table}.yaml.",
        suffix=".tmp",
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(encoded)
        os.replace(str(tmp_path), str(target_path))
    except BaseException:
        # BaseException so KeyboardInterrupt / SystemExit also clean up.
        # We don't swallow the exception — just remove the orphan and
        # re-raise the original.
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
