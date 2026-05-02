#!/usr/bin/env python3
"""Scaffold a new OMOP project skeleton in a customer-chosen path.

Generates a working DAB-shaped OMOP build project: bundle config, jobs DAG with
all 14 OMOP tables as commented placeholders, src/ boilerplate copied from this
skill's reference implementation, empty configs/ folder, seed_data template, and
a quickstart README.

Called from Step 1 of the omop-pipeline-builder workflow. The agent collects
parameters conversationally and invokes scaffold_project() directly; this module
also exposes a thin argparse CLI for engineer-driven re-scaffolds.

The scaffolder verifies the target UC Volume exists before writing. If the
Volume is missing, it raises VolumeNotFoundError; the agent surfaces this to
the customer and asks them to create the Volume via UC governance, then resumes.

The scaffolder probes the core_target schema for existing OMOP tables and
surfaces them in the generated README so the engineer can decide per table:
keep-as-is or rebuild via the skill. It does NOT auto-generate stub configs for
existing tables. It does NOT create catalogs, schemas, or Volumes.

Auth is handled by Databricks runtime when invoked from Genie Code Agent.
--profile only applies for local development against ~/.databrickscfg.
"""

from __future__ import annotations

import argparse
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

from databricks.sdk import WorkspaceClient

from _workspace_probes import _probe_existing_tables

_log = logging.getLogger(__name__)

TEMPLATES_DIR = (
    Path(__file__).resolve().parent.parent / "templates" / "project_scaffold"
)

# Shared validator module copied into the customer's <target>/src/ alongside
# the notebook (see _copy_shared_module). Single-source-of-truth: the
# validator lives here in scripts/ exactly once; v2.0.4c Commit 2's notebook
# rewrite imports `from _omop_validator import ...`, which only resolves at
# customer runtime if the scaffolder put a copy of this file next to the
# notebook in the customer's deployed src/.
SHARED_VALIDATOR_PATH = Path(__file__).resolve().parent / "_omop_validator.py"

# Skill version stamped into a `.omop-skill-version` marker file at scaffold
# time. The marker has two orthogonal uses:
#   1. Version detection (informational): bundle_state reads it as a
#      forward-looking breadcrumb when classifying requests; no scaffolder
#      behavior branches on the *value* of the version string.
#   2. Completion detection (behavioral): the scaffolder writes the marker
#      LAST, after every other artifact. The marker's *presence* therefore
#      means "the previous scaffold ran to completion." A project tree that
#      has `databricks.yml` and `src/` but no marker is a crashed scaffold,
#      and the scaffolder allows retry-with-overwrite. See
#      `scaffold_project`'s refuse guard.
_CURRENT_SKILL_VERSION = "2.0.3"


class VolumeNotFoundError(Exception):
    """Raised when the target UC Volume doesn't exist or isn't accessible.

    The scaffolder does not create UC objects. The agent catches this and
    asks the customer to create the Volume through standard UC governance
    (Catalog Explorer, SQL CREATE VOLUME, or their platform team).
    """

    def __init__(self, volume_target: str, underlying_reason: str):
        self.volume_target = volume_target
        self.underlying_reason = underlying_reason
        super().__init__(
            f"UC Volume '{volume_target}' not found or not accessible.\n"
            f"  Reason: {underlying_reason}\n"
            f"  Action: Ask your UC admin to create the Volume, or run:\n"
            f"    CREATE VOLUME IF NOT EXISTS {volume_target};\n"
            f"  Then re-run the scaffolder."
        )


@dataclass
class ScaffoldResult:
    """What the scaffolder reports back to the agent."""

    project_path: str
    volume_target: str
    core_target: str
    bronze_target: str
    existing_tables: list[str]
    detection_skipped_reason: str | None
    files_written: int


def _default_core_target(volume_target: str) -> str:
    parts = volume_target.split(".")
    if len(parts) < 2:
        raise ValueError(
            "volume_target must be at least catalog.schema to derive a default "
            f"core_target, got: {volume_target}"
        )
    return f"{parts[0]}.core_omop"


def _default_bronze_target(volume_target: str) -> str:
    """Derive a `<CHANGEME>`-flagged default bronze_target from volume_target.

    Unlike `_default_core_target`, there is no safe inferable bronze schema —
    bronze schemas come from the customer's EHR landing zone (Caboodle,
    Clarity, Lakeflow Connect, etc.) and the scaffolder cannot guess it.

    Failure mode if the customer ships the placeholder unchanged:
        - `databricks bundle validate -t production` returns "Validation OK!"
          (DAB validate checks structure, not variable values).
        - The pipeline run fails when `${bronze_schema}` substitutes into a
          source identifier like `cat.<CHANGEME — your bronze schema>.patient`,
          which is invalid SQL. The runtime error contains the literal
          `<CHANGEME>` string, making the cause obvious.

    The placeholder is therefore loud at pipeline-time, but quiet at
    validate-time. Customers should override before deploying — either by
    passing `bronze_target` to the scaffolder or by editing
    `databricks.yml`'s `bronze_schema.default` post-scaffold.
    """
    parts = volume_target.split(".")
    if len(parts) < 1 or not parts[0]:
        raise ValueError(
            "volume_target must include a catalog to derive a default "
            f"bronze_target, got: {volume_target}"
        )
    return f"{parts[0]}.<CHANGEME — your bronze schema>"


def _validate_volume_target(volume_target: str) -> None:
    """Raises ValueError if volume_target isn't three-part catalog.schema.volume.

    Pulled out of `_verify_volume_exists` so the orchestrator can format-check
    volume_target up front (before _assert_consistent_catalogs, before any SDK
    call) without coupling the format check to the SDK roundtrip.
    """
    parts = volume_target.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"volume_target must be three-part catalog.schema.volume, "
            f"got: {volume_target}"
        )


def _validate_core_target(core_target: str) -> None:
    """Raises ValueError if core_target isn't exactly two parts (catalog.schema).

    Defensive guard against malformed --core-target inputs that would otherwise
    crash later inside _render_databricks_yml's `core_target.split('.')[1]`
    with an IndexError, or silently render the wrong core_schema if 3+ parts
    were supplied.
    """
    parts = core_target.split(".")
    if len(parts) != 2:
        raise ValueError(
            f"core_target must be two-part catalog.schema, got: {core_target}"
        )


def _validate_bronze_target(bronze_target: str) -> None:
    """Raises ValueError if bronze_target isn't exactly two parts (catalog.schema).

    Mirrors `_validate_core_target`. The bronze_schema portion may be a
    `<CHANGEME>` placeholder (default path) or a real schema name (explicit
    path); either is two-part valid here. Pipeline-time use is what catches
    an unreplaced placeholder.
    """
    parts = bronze_target.split(".")
    if len(parts) != 2:
        raise ValueError(
            f"bronze_target must be two-part catalog.schema, got: {bronze_target}"
        )


def _assert_consistent_catalogs(
    volume_target: str, core_target: str, bronze_target: str
) -> None:
    """Refuse scaffolds whose three targets span more than one Unity Catalog.

    Cross-catalog OMOP builds are a real but unusual pattern (engineering and
    clinical catalogs separated for governance). The default scaffold assumes
    a single-catalog layout, which is the dominant case. If a customer needs
    cross-catalog, they ask explicitly and we add an `--allow-cross-catalog`
    flag — until then, refusing loudly here surfaces unintentional drift
    (e.g., a typo'd catalog in bronze_target) before any files are written.

    Fires before any disk writes or SDK calls so the failure is atomic. The
    error names all three values so the customer can see which one is the
    odd one out without re-reading their command line.
    """
    volume_catalog = volume_target.split(".")[0]
    core_catalog = core_target.split(".")[0]
    bronze_catalog = bronze_target.split(".")[0]
    catalogs = {volume_catalog, core_catalog, bronze_catalog}
    if len(catalogs) > 1:
        raise ValueError(
            "All three targets must share a single Unity Catalog. Got:\n"
            f"  volume_target={volume_target} (catalog={volume_catalog})\n"
            f"  core_target={core_target} (catalog={core_catalog})\n"
            f"  bronze_target={bronze_target} (catalog={bronze_catalog})\n"
            "Cross-catalog OMOP builds aren't supported by the default "
            "scaffold; ask the maintainers for `--allow-cross-catalog` if "
            "your governance model actually needs separate catalogs."
        )


def _render_databricks_yml(
    volume_target: str,
    core_target: str,
    bronze_target: str,
    project_name: str,
) -> str:
    catalog = volume_target.split(".")[0]
    config_volume = volume_target.split(".")[2]
    core_schema = core_target.split(".")[1]
    bronze_schema = bronze_target.split(".")[1]
    return f"""bundle:
  name: {project_name}

variables:
  catalog:
    description: Unity Catalog name for OMOP build
    default: {catalog}
  bronze_schema:
    description: Bronze schema with EHR source tables
    default: {bronze_schema}
  core_schema:
    description: Schema where OMOP core tables materialize (silver layer)
    default: {core_schema}
  ref_schema:
    description: OHDSI vocabulary reference schema
    default: reference
  config_volume:
    description: UC Volume holding YAML configs for the SDP pipelines
    default: {config_volume}
  notification_email:
    description: Email for pipeline failure notifications (replace with your team)
    default: <CHANGEME — your-team-email@example.com>

include:
  - resources/*.yml

targets:
  production:
    mode: production
    workspace:
      host: <CHANGEME — replace with your workspace URL>
      root_path: /Workspace/Users/${{workspace.current_user.userName}}/.bundle/${{bundle.name}}/${{bundle.target}}
"""


def _render_existing_tables_section(
    core_target: str, tables: list[str], skip_reason: str | None
) -> str:
    if skip_reason:
        return f"""## Existing OMOP tables in `{core_target}`

Detection skipped: {skip_reason}

Run `databricks tables list {core_target}` after fixing the issue, then
update this section manually before deciding which tables to rebuild via
the skill.
"""

    if not tables:
        return f"""## Existing OMOP tables in `{core_target}`

None detected. This is a greenfield build — every table will be authored
through the skill from scratch.
"""

    table_list = "\n".join(f"- `{t}`" for t in tables)
    core_schema_name = core_target.split(".")[1]
    return f"""## Existing OMOP tables in `{core_target}`

{table_list}

These tables were built outside this skill and have not been ratified through
OMOP fidelity review. You have two paths for each:

1. **Keep as-is.** Leave the existing table in place. Don't scaffold a config
   for it. Document in your team's wiki that this table predates the
   skill-driven flow. The skill cannot tell whether it's correct; your team
   makes that call.

2. **Rebuild via the skill.** Generate a config through the per-table workflow.
   The pipeline will materialize the new version into a side-by-side schema
   (set `core_schema` in `databricks.yml` to something like `omop_skill_built`
   while rebuilding). Validate the new version against the existing one before
   cutting over by changing `core_schema` back to `{core_schema_name}`.

The skill will not auto-generate stubs for these tables. You decide per table.
"""


def _render_readme(
    project_name: str,
    volume_target: str,
    core_target: str,
    bronze_target: str,
    existing_tables: list[str],
    detection_skipped_reason: str | None,
) -> str:
    existing_section = _render_existing_tables_section(
        core_target, existing_tables, detection_skipped_reason
    )
    bronze_schema = bronze_target.split(".")[1]
    bronze_changeme_block = (
        f"\n> **`<CHANGEME>` placeholder — override before deploying.** "
        f"`bronze_target` defaulted to `{bronze_target}`. Replace "
        "`<CHANGEME — your bronze schema>` in `databricks.yml`'s "
        "`bronze_schema.default` with the actual schema where your EHR "
        "landing-zone tables live (e.g., `bronze_caboodle`, `bronze_clarity`, "
        "`bronze_lakeflow`). **`databricks bundle validate` will NOT catch "
        "this** — it checks YAML structure, not variable values. The "
        "pipeline run will fail with the literal `<CHANGEME>` string in the "
        "error message.\n"
        if "<CHANGEME" in bronze_schema
        else ""
    )
    return f"""# {project_name}

OMOP CDM v5.4 build project, scaffolded by `omop-pipeline-builder`.

## What's here

- `databricks.yml` — bundle root with catalog/schema variables
- `resources/jobs.yml` — DAG with `person` uncommented as the first task; the
  other 13 OMOP CDM v5.4 tables ship as commented placeholders
- `resources/pipeline_generic.yml` — parameterized SDP pipeline definition
- `src/` — pipeline code (config_loader, vocab_resolver, transform pipeline, validators)
- `configs/` — empty; YAML configs land here as you build each table
- `seed_data/` — STCM template for source-to-concept mappings
- `tests/` — Pydantic schema tests
- `docs/omop-runbook.md` — quickstart guide

## Volume target

Bundle config Volume: `{volume_target}`

## Bronze schema

EHR source tables read from: `{bronze_target}`
{bronze_changeme_block}
## Core schema

OMOP core tables materialize in: `{core_target}`

{existing_section}

## Next steps

1. Replace the `<CHANGEME>` placeholder in `databricks.yml` with your workspace URL.
2. Validate the scaffold: `databricks bundle validate -t production`
   (the scaffold ships with the `person` task uncommented in `resources/jobs.yml`
   so this validation succeeds on a fresh project. Uncomment additional table
   tasks as their configs are added.)
3. Connect this project tree to your team's Git repo. The skill works without
   Git, but recovery and audit are much easier with version control.
4. Pick the OMOP table you want to build first. Most teams start with Person —
   the scaffold's pre-uncommented task points at this build.
5. In Genie Code, ask the agent: "Draft the Person config." It will run the
   per-table workflow and produce a draft `configs/person.yaml` for your review.
6. Review and ratify the draft, then commit through your team's normal Git/CI flow.
7. After deploy, ask the agent: "Validate the Person table." It will run the
   5-layer OMOP fidelity validator.

## Production deploy

This scaffold ships with per-user deploy paths:

```yaml
workspace:
  root_path: /Workspace/Users/${{workspace.current_user.userName}}/.bundle/${{bundle.name}}/${{bundle.target}}
```

Per-user paths are safe for local development and solo work — each contributor's deploy lives under their own workspace path. **For team CI/CD deploys, override the production target's `root_path` to a shared location** so all deploys converge on one canonical artifact:

```yaml
targets:
  production:
    workspace:
      root_path: /Workspace/Shared/.bundle/${{bundle.name}}/${{bundle.target}}
```

Use a service principal path (`/Workspace/Service Principals/<sp-id>/...`) if your CI runs as a service principal and you want deploys isolated from human users.

## Workflow notes

- The skill drafts configs into this project tree but does not commit them. After
  the agent produces a draft, you review, ratify, and commit it yourself.
- The skill does not deploy the bundle. Deploy is owned by your team's CI/CD
  pipeline (`databricks bundle deploy -t production`); your CI typically runs
  this step, not you directly. The agent's responsibility ends at producing
  validated drafts for your review.
"""


def scaffold_project(
    project_path: str,
    volume_target: str,
    core_target: str | None = None,
    bronze_target: str | None = None,
    project_name: str = "omop-build",
    profile: str | None = None,
) -> ScaffoldResult:
    """Scaffold a new OMOP project at project_path.

    Args:
        project_path: Filesystem path where the project tree is written.
            Customer-chosen. Typically a UC Volume mount path
            (/Volumes/<catalog>/<schema>/<volume>/), but local paths and
            Workspace paths also work.
        volume_target: Three-part UC name where the bundle deploys:
            catalog.schema.volume. The Volume MUST exist; scaffolder verifies
            before writing.
        core_target: Two-part UC name (catalog.schema) where OMOP tables
            live or will materialize. Defaults to <catalog>.core_omop, where
            <catalog> is the catalog portion of volume_target.
        bronze_target: Two-part UC name (catalog.schema) where the EHR
            landing-zone tables live. Defaults to a `<CHANGEME>`-flagged
            placeholder under volume_target's catalog — there is no safe
            inferable bronze schema, so the customer must pass the real
            value or replace the placeholder before pipeline-time.
        project_name: Bundle and project identifier.
        profile: Optional Databricks CLI profile for SDK auth.

    Returns:
        ScaffoldResult describing what was written and what state was found.

    Raises:
        VolumeNotFoundError: if volume_target Volume doesn't exist or isn't
            accessible. Agent catches and asks customer to create.
        ValueError: if project_path contains a *completed* OMOP project (all
            three of `databricks.yml`, `src/`, and the `.omop-skill-version`
            marker present); if volume_target / core_target / bronze_target
            are malformed; or if the three targets don't share a single
            Unity Catalog.

    Retry-safe over partial state. The scaffolder writes idempotently:
    `_copy_template_tree` and `_template_catalog_in_load_vocabulary` are
    overwrite-safe, and `_write_templated_files` rewrites both
    `databricks.yml` and `README.md` on every run. Any state that's *not* a
    completed project (crashed mid-scaffold, hand-authored `databricks.yml`,
    `databricks bundle init` output, etc.) is treated as "scaffold over
    this," and the original artifacts are overwritten cleanly. Customer
    files NOT under `templates/project_scaffold/` (e.g., `configs/<table>.yaml`
    drafts) survive untouched.
    """
    target = Path(project_path)
    has_databricks_yml = (target / "databricks.yml").exists()
    has_src = (target / "src").exists()
    has_marker = (target / ".omop-skill-version").exists()
    if has_databricks_yml and has_src and has_marker:
        raise ValueError(
            f"{project_path} contains a completed OMOP project "
            "(databricks.yml, src/, and .omop-skill-version marker all "
            "present). Refusing to scaffold over it. To start over, delete "
            "the project tree contents and re-run, or pick a fresh "
            "project_path. (To recover from a crashed scaffold, the marker "
            "is the missing indicator — re-running the scaffolder will "
            "complete the partial state safely.)"
        )

    # Format-validate all three targets before any disk write or SDK call so
    # malformed inputs surface atomically (no partial writes, no SDK roundtrip
    # against a typo'd volume).
    _validate_volume_target(volume_target)
    core_target = core_target or _default_core_target(volume_target)
    _validate_core_target(core_target)
    bronze_target = bronze_target or _default_bronze_target(volume_target)
    _validate_bronze_target(bronze_target)
    _assert_consistent_catalogs(volume_target, core_target, bronze_target)

    _verify_volume_exists(volume_target, profile=profile)

    catalog = volume_target.split(".")[0]

    files_written = _copy_template_tree(TEMPLATES_DIR, target)
    files_written += _copy_shared_module(SHARED_VALIDATOR_PATH, target / "src")
    templated = _template_catalog_in_load_vocabulary(target, catalog)
    if templated == 0:
        _log.info(
            "scaffolder: 'your_catalog' placeholder not found in scaffolded "
            "src/01_load_vocabulary.py; skipping catalog substitution. "
            "(Source template may have evolved — re-check the templating helper.)"
        )

    existing, skip_reason = _probe_existing_tables(core_target, profile=profile)

    files_written += _write_templated_files(
        target=target,
        volume_target=volume_target,
        core_target=core_target,
        bronze_target=bronze_target,
        project_name=project_name,
        existing_tables=existing,
        detection_skipped_reason=skip_reason,
    )

    marker_path = target / ".omop-skill-version"
    marker_path.write_text(f"{_CURRENT_SKILL_VERSION}\n", encoding="utf-8")
    files_written += 1

    return ScaffoldResult(
        project_path=str(target.resolve()),
        volume_target=volume_target,
        core_target=core_target,
        bronze_target=bronze_target,
        existing_tables=existing,
        detection_skipped_reason=skip_reason,
        files_written=files_written,
    )


def _verify_volume_exists(volume_target: str, profile: str | None = None) -> None:
    """Verify the UC Volume named by volume_target exists.

    Raises VolumeNotFoundError if it doesn't, or if the SDK call fails for any
    reason (auth, permissions, etc). The scaffolder doesn't distinguish between
    "doesn't exist" and "exists but you can't see it" — both require the
    customer to take action through UC governance.
    """
    _validate_volume_target(volume_target)
    full_volume_name = volume_target

    try:
        w = WorkspaceClient(profile=profile)
    except Exception as e:
        raise VolumeNotFoundError(
            volume_target=volume_target,
            underlying_reason=f"Databricks SDK auth failed: {type(e).__name__}: {e}",
        ) from e

    try:
        w.volumes.read(name=full_volume_name)
    except Exception as e:
        raise VolumeNotFoundError(
            volume_target=volume_target,
            underlying_reason=f"{type(e).__name__}: {e}",
        ) from e


def _copy_template_tree(src: Path, dst: Path) -> int:
    """Copy every file under src/ into dst/, preserving directory structure.

    Returns the number of files copied. Raises FileNotFoundError if src doesn't
    exist. Idempotent at the directory level: copying twice into the same dst
    overwrites cleanly.
    """
    if not src.exists():
        raise FileNotFoundError(
            f"Templates directory not found: {src}. "
            "The skill package is missing its bundled templates tree; reinstall "
            "via `databricks workspace import-dir --overwrite ...`."
        )
    dst.mkdir(parents=True, exist_ok=True)
    count = 0
    for path in src.rglob("*"):
        if path.is_dir():
            continue
        rel = path.relative_to(src)
        out = dst / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, out)
        count += 1
    return count


def _copy_shared_module(src_file: Path, dst_dir: Path) -> int:
    """Copy a single shared-module file into the customer's <target>/src/.

    Used to ship ``_omop_validator.py`` (the shared validation module) into
    the scaffolded project alongside the notebook so the notebook's
    ``from _omop_validator import ...`` resolves at customer runtime.

    Sibling helper to ``_copy_template_tree`` rather than an extension of
    it because the shared module is not part of the template tree —
    template files live in ``templates/project_scaffold/`` and represent
    the customer-facing scaffold; the shared module lives in ``scripts/``
    and is the single source of truth for validation logic shared
    between the CLI and the customer's notebook. Conflating the two
    would couple the template tree's lifecycle to the shared module's.

    Returns 1 on success (parallel to ``_copy_template_tree``'s file
    count). Raises ``FileNotFoundError`` if ``src_file`` doesn't exist —
    the skill package is broken if the shared module is missing, and
    silently scaffolding an incomplete customer project would break
    Commit 2's notebook at runtime in mysterious ways.
    """
    if not src_file.exists():
        raise FileNotFoundError(
            f"Shared validator module not found: {src_file}. "
            "The skill package is missing scripts/_omop_validator.py; "
            "reinstall via `databricks workspace import-dir --overwrite ...`."
        )
    dst_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_file, dst_dir / src_file.name)
    return 1


def _write_templated_files(
    target: Path,
    volume_target: str,
    core_target: str,
    bronze_target: str,
    project_name: str,
    existing_tables: list[str],
    detection_skipped_reason: str | None,
) -> int:
    """Write databricks.yml + README.md to target/, returning 2."""
    databricks_yml = _render_databricks_yml(
        volume_target=volume_target,
        core_target=core_target,
        bronze_target=bronze_target,
        project_name=project_name,
    )
    (target / "databricks.yml").write_text(databricks_yml, encoding="utf-8")

    readme = _render_readme(
        project_name=project_name,
        volume_target=volume_target,
        core_target=core_target,
        bronze_target=bronze_target,
        existing_tables=existing_tables,
        detection_skipped_reason=detection_skipped_reason,
    )
    (target / "README.md").write_text(readme, encoding="utf-8")
    return 2


def _template_catalog_in_load_vocabulary(target_dir: Path, catalog: str) -> int:
    """Substitute the placeholder catalog in the scaffolded src/01_load_vocabulary.py.

    The source template ships with `your_catalog` as the widget default and in
    path strings. The scaffolder replaces every literal occurrence with the
    customer's actual catalog (derived from volume_target) so the customer
    doesn't ship the placeholder default. Returns 1 if the file was modified,
    0 if the placeholder was absent (drift-safety: source template may have
    evolved).
    """
    target_file = target_dir / "src" / "01_load_vocabulary.py"
    if not target_file.exists():
        return 0
    content = target_file.read_text(encoding="utf-8")
    if "your_catalog" not in content:
        return 0
    new_content = content.replace("your_catalog", catalog)
    target_file.write_text(new_content, encoding="utf-8")
    return 1


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Scaffold an OMOP CDM v5.4 build project."
    )
    parser.add_argument(
        "--project-path",
        required=True,
        help="Filesystem path where the project tree is written.",
    )
    parser.add_argument(
        "--volume-target",
        required=True,
        help=(
            "Three-part UC name: catalog.schema.volume. "
            "The Volume must already exist."
        ),
    )
    parser.add_argument(
        "--core-target",
        default=None,
        help=(
            "Two-part UC name: catalog.schema. "
            "Defaults to <volume-target-catalog>.core_omop"
        ),
    )
    parser.add_argument(
        "--bronze-target",
        default=None,
        help=(
            "Two-part UC name: catalog.schema. The schema where your EHR "
            "landing-zone tables live (e.g. cat.bronze_caboodle). Defaults "
            "to a <CHANGEME>-flagged placeholder you must edit before "
            "running the pipeline."
        ),
    )
    parser.add_argument("--project-name", default="omop-build")
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile for SDK auth.",
    )
    args = parser.parse_args()

    try:
        result = scaffold_project(
            project_path=args.project_path,
            volume_target=args.volume_target,
            core_target=args.core_target,
            bronze_target=args.bronze_target,
            project_name=args.project_name,
            profile=args.profile,
        )
    except VolumeNotFoundError as e:
        print(f"ERROR: {e}")
        raise SystemExit(2)
    except FileNotFoundError as e:
        print(
            f"ERROR: scaffold templates missing — {e}\n"
            "  This usually means the skill package is broken or the script "
            "was moved out of its expected location. Reinstall the skill via "
            "`databricks workspace import-dir --overwrite ...` and retry."
        )
        raise SystemExit(3)
    except ValueError as e:
        print(f"ERROR: {e}")
        raise SystemExit(1)

    print(f"Scaffolded project at: {result.project_path}")
    print(f"  volume_target: {result.volume_target}")
    print(f"  bronze_target: {result.bronze_target}")
    print(f"  core_target: {result.core_target}")
    print(f"  files written: {result.files_written}")
    print(f"  skill version stamped: {_CURRENT_SKILL_VERSION}")
    if result.existing_tables:
        print(f"  existing tables found: {len(result.existing_tables)}")
        for t in result.existing_tables:
            print(f"    - {t}")
    elif result.detection_skipped_reason:
        print(f"  table detection skipped: {result.detection_skipped_reason}")
    else:
        print("  no existing OMOP tables detected (greenfield)")


if __name__ == "__main__":
    _cli()
