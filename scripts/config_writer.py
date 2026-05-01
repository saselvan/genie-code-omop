#!/usr/bin/env python3
"""Write OMOP YAML configs to a scaffolded project's configs/ directory.

Phase 3 of omop-pipeline-builder v2.0 introduces the Update sub-path: when
the engineer chooses "Update" from Step 2's three sub-paths, the agent
regenerates the whole config and writes the new version through this
module. ``write_config`` is the single shared write surface for the
Update and Replace sub-paths; the Generate sub-path (greenfield) writes
through this same surface with ``overwrite=False``.

Decision 9: writes the WHOLE regenerated config — never a textual diff.
Decision 10: VCS-agnostic. The writer never invokes git; the engineer
commits through their own workflow. A non-Git'd project surfaces an
informational ``git_warning`` in :class:`WriteResult` (added in Phase 3
Step 3) but never blocks the write.
Decision 11: agent writes, engineer commits. The writer does not
``git add`` or ``git commit``.

This file ships the Step-1 happy-path slice only:
  - greenfield write (file does not exist)
  - bytes-for-bytes preservation
  - ``WriteResult`` populated with placeholders for git_warning /
    mtime_warning (Steps 3 + 4 fill these in)

Atomic-write semantics (Step 2), overwrite handling (Step 2), Git
integration (Step 3), and ``expected_mtime`` concurrency (Step 4) are
added by subsequent commits in this phase.

Auth boilerplate not applicable (pure filesystem; no SDK calls).
"""

from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

# Canonical lowercase snake_case regex — matches Phase 2's
# bundle_state._TARGET_TABLE_RE. Duplicated rather than imported to keep
# config_writer.py independent of bundle_state's import graph (which
# pulls _workspace_probes and the Databricks SDK transitively). The two
# regexes must stay in lockstep; a future module-level constant that
# both consume could deduplicate, but for now a one-line duplication is
# the lowest-risk choice.
_TARGET_TABLE_RE = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass
class WriteResult:
    """Result of a successful :func:`write_config` invocation.

    Returned for every successful write — the writer never raises on
    informational concerns (Git status, mtime drift in unstable mode).
    Phase 3 Steps 3 + 4 populate the warning fields; Step 1 leaves them
    as ``None``.

    Fields:
        config_path: Absolute path of the file that was written.
        bytes_written: Number of bytes the new file contains.
        overwrote_existing: True if a pre-existing file was replaced.
            False for greenfield writes. Step 1 always returns False
            (overwrite handling lands in Step 2).
        git_warning: Informational text surfaced to the agent's response
            template when the project isn't under git version control.
            Never blocks the write. Populated by Step 3.
        mtime_warning: Informational text surfaced when ``expected_mtime``
            was provided AND the Phase 0a FUSE mtime spike concluded
            mtime is unstable. In stable mode (which this build ships
            per Phase 0a's SESSION-STATE entry), mtime mismatches raise
            :class:`MtimeMismatchError` instead. Populated by Step 4.
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

    **Step 1 + 2 surface:**

    - Validates ``target_table`` against the canonical lowercase regex
      ``^[a-z][a-z0-9_]*$``; raises :class:`ValueError` on mismatch.
      Same gate Phase 2's ``classify_request`` enforces, repeated here
      so the writer is safe to call directly without a prior
      classification pass. The validation gate fires BEFORE any
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
    - **Concurrency model: last-write-wins.** The writer does not
      coordinate with concurrent writers. Phase 3 Step 4 adds
      optional ``expected_mtime`` checking that turns a concurrent
      modification into a :class:`MtimeMismatchError` instead of a
      silent overwrite — but only when the caller opts in.

    **Future-step surface (NOT IMPLEMENTED IN STEP 2):**

    - ``expected_mtime``: Step 4 — gated on Phase 0a's FUSE spike
      result (this build ships in **STABLE** mode per Phase 0a's
      SESSION-STATE entry). Will raise :class:`MtimeMismatchError` on
      drift. Currently accepted but unused.
    - ``git_warning`` field of the return value: Step 3 — informational
      text surfaced when the project isn't under git. Currently always
      ``None``.

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
        expected_mtime: Reserved for Step 4. Currently unused.

    Returns:
        :class:`WriteResult` describing the write. ``overwrote_existing``
        is True iff ``overwrite=True`` and the target file existed
        before the write.

    Raises:
        ValueError: ``target_table`` does not match the canonical regex.
        FileExistsError: target file exists and ``overwrite=False``.
    """
    if not _TARGET_TABLE_RE.match(target_table):
        raise ValueError(
            f"target_table must be lowercase snake_case matching "
            f"^[a-z][a-z0-9_]*$, got: {target_table!r}"
        )

    configs_dir = Path(project_path) / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)

    target_path = configs_dir / f"{target_table}.yaml"
    pre_existing = target_path.exists()

    if pre_existing and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing config: {target_path}. "
            "Pass overwrite=True for the Replace or Update sub-paths."
        )

    encoded = yaml_content.encode("utf-8")
    _atomic_write_bytes(configs_dir, target_path, encoded, target_table)

    return WriteResult(
        config_path=str(target_path),
        bytes_written=len(encoded),
        overwrote_existing=pre_existing,
        git_warning=None,
        mtime_warning=None,
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
