#!/usr/bin/env python3
"""Scaffold a new OMOP project skeleton in a customer-chosen path.

Generates a working DAB-shaped OMOP build project: bundle config, jobs DAG with
all 14 OMOP tables as commented placeholders, src/ boilerplate copied from this
skill's reference implementation, empty configs/ folder, seed_data template, and
a quickstart README.

Called from Step 0 of the omop-pipeline-builder workflow. The agent collects
parameters conversationally and invokes scaffold_project() directly; this module
also exposes a thin argparse CLI for engineer-driven re-scaffolds.

The scaffolder verifies the target UC Volume exists before writing. If the
Volume is missing, it raises VolumeNotFoundError; the agent surfaces this to
the customer and asks them to create the Volume via UC governance, then resumes.

The scaffolder probes the silver_target schema for existing OMOP tables and
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

_log = logging.getLogger(__name__)

TEMPLATES_DIR = (
    Path(__file__).resolve().parent.parent / "templates" / "project_scaffold"
)


class VolumeNotFoundError(Exception):
    """Raised when the target UC Volume doesn't exist or isn't accessible.

    The scaffolder does not create UC objects. The agent catches this and
    asks the customer to create the Volume through standard UC governance
    (Catalog Explorer, SQL CREATE VOLUME, or their platform team).
    """

    def __init__(self, bundle_target: str, underlying_reason: str):
        self.bundle_target = bundle_target
        self.underlying_reason = underlying_reason
        super().__init__(
            f"UC Volume '{bundle_target}' not found or not accessible.\n"
            f"  Reason: {underlying_reason}\n"
            f"  Action: Ask your UC admin to create the Volume, or run:\n"
            f"    CREATE VOLUME IF NOT EXISTS {bundle_target};\n"
            f"  Then re-run the scaffolder."
        )


@dataclass
class ScaffoldResult:
    """What the scaffolder reports back to the agent."""

    project_path: str
    bundle_target: str
    silver_target: str
    existing_tables: list[str]
    detection_skipped_reason: str | None
    files_written: int


def _default_silver_target(bundle_target: str) -> str:
    parts = bundle_target.split(".")
    if len(parts) < 2:
        raise ValueError(
            "bundle_target must be at least catalog.schema to derive a default "
            f"silver_target, got: {bundle_target}"
        )
    return f"{parts[0]}.core_omop"


def _validate_silver_target(silver_target: str) -> None:
    """Raises ValueError if silver_target isn't exactly two parts (catalog.schema).

    Defensive guard against malformed --silver-target inputs that would otherwise
    crash later inside _render_databricks_yml's `silver_target.split('.')[1]`
    with an IndexError, or silently render the wrong core_schema if 3+ parts
    were supplied.
    """
    parts = silver_target.split(".")
    if len(parts) != 2:
        raise ValueError(
            f"silver_target must be two-part catalog.schema, got: {silver_target}"
        )


def _render_databricks_yml(
    bundle_target: str, silver_target: str, project_name: str
) -> str:
    catalog = bundle_target.split(".")[0]
    config_volume = bundle_target.split(".")[2]
    core_schema = silver_target.split(".")[1]
    return f"""bundle:
  name: {project_name}

variables:
  catalog:
    description: Unity Catalog name for OMOP build
    default: {catalog}
  bronze_schema:
    description: Bronze schema with EHR source tables
    default: bronze_clinical
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
"""


def _render_existing_tables_section(
    silver_target: str, tables: list[str], skip_reason: str | None
) -> str:
    if skip_reason:
        return f"""## Existing OMOP tables in `{silver_target}`

Detection skipped: {skip_reason}

Run `databricks tables list {silver_target}` after fixing the issue, then
update this section manually before deciding which tables to rebuild via
the skill.
"""

    if not tables:
        return f"""## Existing OMOP tables in `{silver_target}`

None detected. This is a greenfield build — every table will be authored
through the skill from scratch.
"""

    table_list = "\n".join(f"- `{t}`" for t in tables)
    core_schema_name = silver_target.split(".")[1]
    return f"""## Existing OMOP tables in `{silver_target}`

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
    bundle_target: str,
    silver_target: str,
    existing_tables: list[str],
    detection_skipped_reason: str | None,
) -> str:
    existing_section = _render_existing_tables_section(
        silver_target, existing_tables, detection_skipped_reason
    )
    return f"""# {project_name}

OMOP CDM v5.4 build project, scaffolded by `omop-pipeline-builder`.

## What's here

- `databricks.yml` — bundle root with catalog/schema variables
- `resources/jobs.yml` — DAG with all 14 OMOP tables as commented placeholders
- `resources/pipeline_generic.yml` — parameterized SDP pipeline definition
- `src/` — pipeline code (config_loader, vocab_resolver, transform pipeline, validators)
- `configs/` — empty; YAML configs land here as you build each table
- `seed_data/` — STCM template for source-to-concept mappings
- `tests/` — Pydantic schema tests
- `docs/omop-runbook.md` — quickstart guide

## Bundle target

This bundle deploys to: `{bundle_target}`

## Silver schema

OMOP tables materialize in: `{silver_target}`

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
    bundle_target: str,
    silver_target: str | None = None,
    project_name: str = "omop-build",
    profile: str | None = None,
) -> ScaffoldResult:
    """Scaffold a new OMOP project at project_path.

    Args:
        project_path: Filesystem path where the project tree is written.
            Customer-chosen. Typically a UC Volume mount path
            (/Volumes/<catalog>/<schema>/<volume>/), but local paths and
            Workspace paths also work.
        bundle_target: Three-part UC name where the bundle deploys:
            catalog.schema.volume. The Volume MUST exist; scaffolder verifies
            before writing.
        silver_target: Two-part UC name (catalog.schema) where OMOP tables
            live or will materialize. Defaults to <bundle_catalog>.core_omop.
        project_name: Bundle and project identifier.
        profile: Optional Databricks CLI profile for SDK auth.

    Returns:
        ScaffoldResult describing what was written and what state was found.

    Raises:
        VolumeNotFoundError: if bundle_target Volume doesn't exist or isn't
            accessible. Agent catches and asks customer to create.
        ValueError: if project_path already contains a databricks.yml.
    """
    target = Path(project_path)
    if (target / "databricks.yml").exists():
        raise ValueError(
            f"{project_path} already has a databricks.yml. Refusing to overwrite. "
            "Pick a different path, or delete the existing project to start fresh."
        )

    silver_target = silver_target or _default_silver_target(bundle_target)
    _validate_silver_target(silver_target)

    _verify_volume_exists(bundle_target, profile=profile)

    catalog = bundle_target.split(".")[0]

    files_written = _copy_template_tree(TEMPLATES_DIR, target)
    templated = _template_catalog_in_load_vocabulary(target, catalog)
    if templated == 0:
        _log.info(
            "scaffolder: 'your_catalog' placeholder not found in scaffolded "
            "src/01_load_vocabulary.py; skipping catalog substitution. "
            "(Source template may have evolved — re-check the templating helper.)"
        )

    existing, skip_reason = _probe_existing_tables(silver_target, profile=profile)

    files_written += _write_templated_files(
        target=target,
        bundle_target=bundle_target,
        silver_target=silver_target,
        project_name=project_name,
        existing_tables=existing,
        detection_skipped_reason=skip_reason,
    )

    return ScaffoldResult(
        project_path=str(target.resolve()),
        bundle_target=bundle_target,
        silver_target=silver_target,
        existing_tables=existing,
        detection_skipped_reason=skip_reason,
        files_written=files_written,
    )


def _verify_volume_exists(bundle_target: str, profile: str | None = None) -> None:
    """Verify the UC Volume named by bundle_target exists.

    Raises VolumeNotFoundError if it doesn't, or if the SDK call fails for any
    reason (auth, permissions, etc). The scaffolder doesn't distinguish between
    "doesn't exist" and "exists but you can't see it" — both require the
    customer to take action through UC governance.
    """
    parts = bundle_target.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"bundle_target must be three-part catalog.schema.volume, "
            f"got: {bundle_target}"
        )
    full_volume_name = bundle_target

    try:
        w = WorkspaceClient(profile=profile)
    except Exception as e:
        raise VolumeNotFoundError(
            bundle_target=bundle_target,
            underlying_reason=f"Databricks SDK auth failed: {type(e).__name__}: {e}",
        ) from e

    try:
        w.volumes.read(name=full_volume_name)
    except Exception as e:
        raise VolumeNotFoundError(
            bundle_target=bundle_target,
            underlying_reason=f"{type(e).__name__}: {e}",
        ) from e


def _probe_existing_tables(
    silver_target: str, profile: str | None = None
) -> tuple[list[str], str | None]:
    """Best-effort probe. Returns ([], reason) on any failure."""
    parts = silver_target.split(".")
    if len(parts) != 2:
        return [], f"silver_target must be catalog.schema, got: {silver_target}"
    catalog, schema = parts

    try:
        w = WorkspaceClient(profile=profile)
    except Exception as e:
        return [], f"SDK auth failed: {type(e).__name__}: {e}"

    try:
        tables = list(w.tables.list(catalog_name=catalog, schema_name=schema))
        return [t.name for t in tables if t.name], None
    except Exception as e:
        return (
            [],
            f"Could not list tables in {silver_target}: {type(e).__name__}: {e}",
        )


def _copy_template_tree(src: Path, dst: Path) -> int:
    """Copy every file under src/ into dst/, preserving directory structure.

    Returns the number of files copied. Raises FileNotFoundError if src doesn't
    exist. Idempotent at the directory level: copying twice into the same dst
    overwrites cleanly.
    """
    if not src.exists():
        raise FileNotFoundError(
            f"Templates directory not found: {src}. "
            "Run Phase 1 of the v1.6 build first."
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


def _write_templated_files(
    target: Path,
    bundle_target: str,
    silver_target: str,
    project_name: str,
    existing_tables: list[str],
    detection_skipped_reason: str | None,
) -> int:
    """Write databricks.yml + README.md to target/, returning 2."""
    databricks_yml = _render_databricks_yml(
        bundle_target=bundle_target,
        silver_target=silver_target,
        project_name=project_name,
    )
    (target / "databricks.yml").write_text(databricks_yml, encoding="utf-8")

    readme = _render_readme(
        project_name=project_name,
        bundle_target=bundle_target,
        silver_target=silver_target,
        existing_tables=existing_tables,
        detection_skipped_reason=detection_skipped_reason,
    )
    (target / "README.md").write_text(readme, encoding="utf-8")
    return 2


def _template_catalog_in_load_vocabulary(target_dir: Path, catalog: str) -> int:
    """Substitute the placeholder catalog in the scaffolded src/01_load_vocabulary.py.

    The source template ships with `your_catalog` as the widget default and in
    path strings. The scaffolder replaces every literal occurrence with the
    customer's actual catalog (derived from bundle_target) so the customer
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
        "--bundle-target",
        required=True,
        help=(
            "Three-part UC name: catalog.schema.volume. "
            "The Volume must already exist."
        ),
    )
    parser.add_argument(
        "--silver-target",
        default=None,
        help=(
            "Two-part UC name: catalog.schema. "
            "Defaults to <bundle-catalog>.core_omop"
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
            bundle_target=args.bundle_target,
            silver_target=args.silver_target,
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
    print(f"  bundle_target: {result.bundle_target}")
    print(f"  silver_target: {result.silver_target}")
    print(f"  files written: {result.files_written}")
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
