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

import re
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

    **Step 1 surface (happy path only):**

    - Validates ``target_table`` against the canonical lowercase regex
      ``^[a-z][a-z0-9_]*$``; raises :class:`ValueError` on mismatch.
      Same gate Phase 2's ``classify_request`` enforces, repeated here
      so the writer is safe to call directly without a prior
      classification pass.
    - Creates ``<project_path>/configs/`` if it doesn't exist (defensive
      against partial scaffolds — the scaffolder always creates it, but
      a customer who deleted the directory should not see an opaque
      ``FileNotFoundError`` from this writer).
    - Writes ``yaml_content`` to ``<configs>/<target_table>.yaml`` as
      UTF-8. The write is direct (not yet atomic — Step 2 adds the
      tempfile + ``os.replace`` dance).
    - Returns a :class:`WriteResult` populated with the actual path,
      byte count, ``overwrote_existing=False``, and ``None`` for both
      warning fields. Subsequent steps populate the warnings.

    **Future-step surface (NOT IMPLEMENTED IN STEP 1):**

    - ``overwrite``: Step 2 — when False against an existing file,
      raises ``FileExistsError``; when True, replaces atomically.
      Currently the parameter is accepted but, since Step 1 doesn't
      implement overwrite or atomic write, an existing file will be
      directly replaced via the underlying ``Path.write_text`` call.
      Step 2 lands the proper semantics; Step 1 tests do not exercise
      the overwrite path.
    - ``expected_mtime``: Step 4 — gated on Phase 0a's FUSE spike
      result. Stable-mode raises :class:`MtimeMismatchError` on drift;
      unstable-mode populates ``WriteResult.mtime_warning``. Currently
      accepted but unused.

    Args:
        project_path: Filesystem path of the scaffolded project tree.
        target_table: Lowercase canonical OMOP table name (e.g. ``"person"``).
        yaml_content: The YAML document to write, as a string. Empty
            content is allowed — the writer does not validate YAML
            syntax; that's the agent's responsibility (the agent runs
            Pydantic validation BEFORE calling this function).
        overwrite: Reserved for Step 2. Currently does not enforce
            overwrite semantics.
        expected_mtime: Reserved for Step 4. Currently unused.

    Returns:
        :class:`WriteResult` describing the write.

    Raises:
        ValueError: ``target_table`` does not match the canonical regex.
    """
    if not _TARGET_TABLE_RE.match(target_table):
        raise ValueError(
            f"target_table must be lowercase snake_case matching "
            f"^[a-z][a-z0-9_]*$, got: {target_table!r}"
        )

    configs_dir = Path(project_path) / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)

    target_path = configs_dir / f"{target_table}.yaml"
    target_path.write_text(yaml_content, encoding="utf-8")

    return WriteResult(
        config_path=str(target_path),
        bytes_written=len(yaml_content.encode("utf-8")),
        overwrote_existing=False,
        git_warning=None,
        mtime_warning=None,
    )
