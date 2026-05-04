# Recommended CI/CD integration

This document describes how to integrate `omop-pipeline-builder`'s
validators into your CI/CD pipeline. **The skill itself does not
deploy** — your CI/CD pipeline owns deploy. These snippets help you
wire the skill's validators into the pipeline you already have.

The snippets are working YAML, written for the project layout the
skill scaffolds (see the skill's `SKILL.md` at
`.assistant/skills/omop-pipeline-builder/SKILL.md` in your deployed
workspace, "Step 1 — Scaffold the project"). Adjust paths if your
bundle lives elsewhere.

---

## What runs in CI

Two validators belong in CI for OMOP builds. Both are **fast** and
**stateless** — exactly the shape CI is good at:

1. **Pydantic schema validation.** Validates every `configs/*.yaml`
   file in the project against the OMOP config schema. Catches
   malformed YAML, missing required fields, invalid resolution
   strategies, cross-field violations (e.g., a vocabulary lookup
   declares `resolution: concept_table` but omits `domain_id`). Runs
   the scaffolded `tests/test_config_schema.py`. Fast: typically
   under 5 seconds per run.

2. **Databricks Asset Bundle validation.** Runs
   `databricks bundle validate -t <target>`. Catches missing
   pipeline references, malformed `depends_on` chains, schema
   violations in `resources/*.yml`, missing variables. Fast:
   typically under 10 seconds per run.

Two surfaces belong in **post-deploy** validation, NOT in CI:

3. **5-layer OMOP fidelity validation.** `scripts/validate_omop.py`
   runs against materialized tables in your `core_target` schema
   and checks: schema conformance to OMOP CDM v5.4, primary-key
   uniqueness, concept FK referential integrity, domain
   correctness, completeness / null-rate. **Slow** (typically
   30 seconds to several minutes per table) and **requires a
   populated table** — neither fits a pre-deploy CI gate. Run as
   a downstream Workflow task or scheduled validation; see
   "What about post-deploy validation?" below.

4. **Pipeline runs.** `scripts/run_pipeline.py` is a developer-loop
   tool. Production pipeline runs are orchestrated by your
   Databricks Workflows (scheduled jobs, file arrival triggers,
   etc.), not by CI. **The skill never deploys, never triggers
   production runs.**

---

## Conventions used by these snippets

The snippets assume:

- Your bundle is at the **repo root** with the standard scaffold
  layout (`databricks.yml`, `configs/`, `resources/`, `src/`,
  `tests/`). Adjust `working-directory` / `cwd` if your bundle
  lives in a sub-path.
- The bundle target is parameterized via the `DAB_TARGET`
  environment variable (defaults to `production`). If your team
  has multiple targets (`dev` / `staging` / `production`), gate
  each on the appropriate branch using your CI's branch matrix or
  conditional steps.
- **Service principal (SP) authentication** for production CI.
  Personal access tokens (PATs) are acceptable for dev / sandbox
  CI but **not** for production HLS environments — most BAA-bound
  Databricks deployments forbid PATs in production CI by policy.
  Each snippet documents both patterns.
- Python 3.11 (the Databricks Runtime baseline as of this writing).
  Adjust `python-version` if your runtime's Python differs.
- Databricks CLI v0.250.0 or newer. Newer CLIs are usually
  backward-compatible for `bundle validate`; pin to a known-good
  version and update periodically.

---

## GitHub Actions

Drop this into `.github/workflows/omop-ci.yml` at the repo root.
Customize the four marked points (`# CUSTOMIZE`) for your team:

```yaml
# .github/workflows/omop-ci.yml
#
# CI for omop-pipeline-builder-scaffolded projects. Runs Pydantic
# schema validation and Databricks bundle validation on every PR
# to main and every push to main. Does NOT deploy and does NOT
# trigger pipeline runs — see references/recommended_ci_config.md
# for why.

name: OMOP CI

on:
  pull_request:
    branches:
      - main
  push:
    branches:
      - main

env:
  # CUSTOMIZE: rename if your team uses a different default target
  # (e.g., DAB_TARGET: dev for a dev-default repo).
  DAB_TARGET: production

  # CUSTOMIZE: align with your Databricks Runtime's Python version.
  # 3.11 matches DBR 15.x LTS as of this writing.
  PYTHON_VERSION: "3.11"

  # NOTE: the Databricks CLI version is pinned in the
  # `databricks/setup-cli@vX.Y.Z` step below, not here. GitHub
  # Actions does not allow ${{ env.* }} interpolation in `uses:`
  # references — the action ref must be a literal. If you bump
  # the CLI version, edit the `uses:` line directly. Re-test
  # before bumping; some 0.2xx -> 0.3xx transitions changed flag
  # syntax.

jobs:
  validate:
    name: Validate configs and bundle
    runs-on: ubuntu-latest
    timeout-minutes: 10

    permissions:
      contents: read
      # No write permissions — CI validates, it does not deploy.

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
          cache: pip

      - name: Install Python dependencies
        run: |
          python -m pip install --upgrade pip
          # pyspark is required because the scaffolded
          # tests/test_config_schema.py imports src/config_loader.py
          # which imports pyspark at module level. ~300 MB cold,
          # ~5 s warm cache.
          pip install \
            pydantic \
            pyyaml \
            "pyspark>=3.5,<4.0"

      - name: Pydantic schema validation
        # The scaffolded test_template_health smoke test always
        # passes (see templates/project_scaffold/tests/test_config_schema.py), so
        # this step exits 0 even when configs/ is empty. As you
        # commit configs/<table>.yaml files, parametrize collection
        # picks them up automatically.
        run: pytest tests/test_config_schema.py -v

      - name: Install Databricks CLI
        # Official setup action. CUSTOMIZE: bump the version tag
        # (e.g., v0.260.0) when ready; the action ref MUST be a
        # literal — GitHub Actions does not allow env vars in
        # `uses:` references.
        uses: databricks/setup-cli@v0.250.0

      - name: Databricks bundle validate
        # Service principal OAuth M2M is the recommended pattern
        # for production CI. The Databricks CLI auto-detects these
        # three env vars; no flag wiring needed.
        #
        # CUSTOMIZE: create three repository secrets in
        # Settings -> Secrets and variables -> Actions:
        #   DATABRICKS_HOST          (e.g. https://adb-xxx.x.azuredatabricks.net)
        #   DATABRICKS_CLIENT_ID     (SP application ID)
        #   DATABRICKS_CLIENT_SECRET (SP OAuth secret)
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
        run: databricks bundle validate -t "${DAB_TARGET}"
```

### Dev-only PAT alternative (NOT for production CI)

For a personal sandbox or a lower-risk dev workspace where SP
provisioning is overkill, the same `Databricks bundle validate`
step accepts a PAT:

```yaml
      - name: Databricks bundle validate (PAT — dev only)
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: databricks bundle validate -t "${DAB_TARGET}"
```

**Production HLS environments commonly forbid PATs in CI.** The
PAT pattern is documented for completeness; verify against your
team's auth policy before adopting it.

### What to verify before merging this workflow

1. The four `# CUSTOMIZE` points are filled in for your repo.
2. The three secrets (`DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`,
   `DATABRICKS_CLIENT_SECRET`) exist in repo settings.
3. The SP has *read* access to the workspace and to any UC
   resources (catalogs, schemas, volumes) that
   `databricks.yml` references. `bundle validate` doesn't need
   write access.
4. `python -m pytest tests/test_config_schema.py` runs locally
   against your repo before pushing the workflow.

---

## Azure DevOps Pipelines

Common in HLS Azure customers. Drop this into `azure-pipelines.yml`
at the repo root. **Modern Azure DevOps is YAML-first;** classic
editor / release pipelines still exist but the YAML pipeline below
is the recommended pattern. If your team is on classic editor,
port the four logical steps (setup Python → install deps → pytest
→ bundle validate) one-for-one into classic tasks.

Customize the four marked points (`# CUSTOMIZE`):

```yaml
# azure-pipelines.yml
#
# CI for omop-pipeline-builder-scaffolded projects on Azure DevOps.
# Runs Pydantic schema validation and Databricks bundle validation
# on every PR to main and every push to main. Does NOT deploy and
# does NOT trigger pipeline runs — see references/recommended_ci_config.md
# for why.

trigger:
  branches:
    include:
      - main

pr:
  branches:
    include:
      - main

pool:
  vmImage: ubuntu-latest

variables:
  # CUSTOMIZE: link a variable group that holds the Databricks
  # auth secrets. The recommended setup is a variable group
  # backed by Azure Key Vault — secrets stay in Key Vault and the
  # pipeline reads them at run time. Avoid storing the SP secret
  # as a plain pipeline variable; HLS BAA policies typically
  # forbid that.
  #
  # The variable group MUST expose three variables:
  #   DATABRICKS_HOST          (e.g. https://adb-xxx.x.azuredatabricks.net)
  #   DATABRICKS_CLIENT_ID     (SP application ID)
  #   DATABRICKS_CLIENT_SECRET (SP OAuth secret; Key-Vault-backed)
  - group: omop-databricks-prod

  # CUSTOMIZE: rename if your team uses a different default target.
  - name: DAB_TARGET
    value: production

  # CUSTOMIZE: align with your Databricks Runtime's Python version.
  - name: PYTHON_VERSION
    value: '3.11'

steps:
  - task: UsePythonVersion@0
    displayName: 'Set up Python $(PYTHON_VERSION)'
    inputs:
      versionSpec: $(PYTHON_VERSION)

  - script: |
      python -m pip install --upgrade pip
      # pyspark is required because the scaffolded
      # tests/test_config_schema.py imports src/config_loader.py
      # which imports pyspark at module level.
      pip install \
        pydantic \
        pyyaml \
        "pyspark>=3.5,<4.0"
    displayName: 'Install Python dependencies'

  - script: pytest tests/test_config_schema.py -v
    displayName: 'Pydantic schema validation'

  - script: |
      # CUSTOMIZE: bump the version in the URL when ready
      # (e.g., v0.260.0). No native Azure DevOps task ships for
      # the Databricks CLI; the curl-pipe-sh installer is the
      # documented official pattern. `sudo sh` ensures the binary
      # lands in /usr/local/bin (already on the agent's PATH);
      # without sudo the installer falls back to $HOME/.local/bin
      # which is NOT on PATH for the current task and would force
      # a vso prependpath workaround.
      curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.250.0/install.sh | sudo sh
      databricks --version
    displayName: 'Install Databricks CLI'

  - script: databricks bundle validate -t $(DAB_TARGET)
    displayName: 'Databricks bundle validate'
    # Service principal OAuth M2M is the recommended pattern for
    # production CI. The Databricks CLI auto-detects these three
    # env vars from the variable group above; no flag wiring
    # needed.
    env:
      DATABRICKS_HOST: $(DATABRICKS_HOST)
      DATABRICKS_CLIENT_ID: $(DATABRICKS_CLIENT_ID)
      DATABRICKS_CLIENT_SECRET: $(DATABRICKS_CLIENT_SECRET)
```

### Service connection alternative (for teams already wiring Azure subscriptions that way)

If your team already has an Azure DevOps **service connection** to
the Databricks workspace (set up via Project Settings → Service
connections → Databricks), you can replace the variable-group
auth pattern with the service connection. Service connections
manage SP credentials centrally and avoid duplicating the secret
across variable groups:

```yaml
  - task: AzureCLI@2
    displayName: 'Databricks bundle validate (via service connection)'
    inputs:
      # CUSTOMIZE: name of your Databricks service connection.
      azureSubscription: 'omop-databricks-prod-sc'
      scriptType: 'bash'
      scriptLocation: 'inlineScript'
      inlineScript: |
        export DATABRICKS_HOST="$(DATABRICKS_HOST)"
        databricks bundle validate -t $(DAB_TARGET)
```

The `AzureCLI@2` task injects the SP's credentials into the
process env automatically; the inline script reads
`DATABRICKS_HOST` from the variable group and lets the CLI's
auto-auth pick up the rest. Use this pattern only if your team
already manages Databricks service connections — otherwise the
plain variable-group pattern above is simpler.

**Verify the credential plumbing.** Service-connection contracts
vary by Azure DevOps / Databricks integration version. Some
connections export `DATABRICKS_TOKEN`, others export Azure-AD
tokens that the Databricks CLI consumes via `azure-resource-id`
auth, others require `az login` to be run inside the inline
script before the CLI works. Before committing this snippet,
verify the credential flow with a one-shot run:

```bash
# Inside the AzureCLI@2 inlineScript, before bundle validate:
databricks auth env
# Confirms which env vars the CLI sees and which auth method
# it will use. Adjust the snippet (or the service connection)
# until `databricks auth env` reports a complete auth profile.
```

Your team's Databricks-on-Azure runbook is the source of truth
for which env vars the connection emits. Don't merge the snippet
on assumption.

### Dev-only PAT alternative (NOT for production CI)

For a personal sandbox or a lower-risk dev workspace, the same
`Databricks bundle validate` step accepts a PAT instead of an SP:

```yaml
  - script: databricks bundle validate -t $(DAB_TARGET)
    displayName: 'Databricks bundle validate (PAT — dev only)'
    env:
      DATABRICKS_HOST: $(DATABRICKS_HOST)
      DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)
```

The variable group must then expose `DATABRICKS_TOKEN` (PAT)
instead of the three SP variables. **Production HLS environments
commonly forbid PATs in CI** — verify against your team's auth
policy before adopting this pattern.

### What to verify before merging this pipeline

1. The four `# CUSTOMIZE` points are filled in for your repo.
2. The variable group (or service connection) exists in your
   Azure DevOps project, backed by Key Vault if your team's
   policy requires.
3. The SP / service connection has *read* access to the
   workspace and to any UC resources `databricks.yml` references.
4. `python -m pytest tests/test_config_schema.py` runs locally
   against your repo before pushing the pipeline.
5. The first run will likely prompt for permission to use the
   variable group — grant it from the pipeline run page.

---

## What about Jenkins and GitLab CI?

Skipped in v2.0 due to weaker customer signal in the HLS deploy
patterns we observed. If your team uses one of these platforms,
the same logic applies:

- Run the scaffolded `pytest tests/test_config_schema.py` for
  Pydantic validation.
- Run `databricks bundle validate -t <target>` for bundle
  validation.
- Do **NOT** run `databricks bundle deploy` — the skill does not
  deploy, and CI is not the right place to deploy production OMOP
  pipelines.
- Do **NOT** run `scripts/run_pipeline.py` — that's a developer-
  loop tool, not a CI tool.

The Pydantic step is a single `pytest` invocation; the bundle
validate is the same `databricks` CLI call. Both are easy to port
from the GitHub Actions snippet above.

If you'd like Jenkins or GitLab CI snippets shipped in a future
revision, open an issue against the skill repo with the platform's
specifics (declarative vs scripted Jenkins, GitLab Runner type,
etc.).

---

## What about post-deploy validation?

Post-deploy validation runs the **5-layer OMOP fidelity check**
(`scripts/validate_omop.py`) against tables that have actually
materialized in your `core_target` schema. CI is the wrong place
for it — the validator needs a populated table, which CI doesn't
have. Three patterns work for production:

1. **Manual.** The engineer runs
   `python scripts/validate_omop.py --table <catalog>.<core_schema>.<table>`
   after each deploy. Common for early-stage builds and small
   teams. Findings reviewed by the engineer; reviewer ratifies
   each. The skill's `SKILL.md`
   (`.assistant/skills/omop-pipeline-builder/SKILL.md` in your
   deployed workspace) Step 7 documents this flow.

2. **Workflow task.** Add a `python_wheel_task` (or notebook task)
   to your pipeline's downstream Workflow that runs `validate_omop`
   after each table materializes. Findings logged to MLflow,
   posted to Slack via webhook, or written to a Lakehouse table
   for tracking. **Recommended for steady-state production.**

3. **Scheduled.** Validate the full silver layer nightly via a
   scheduled Databricks Workflow. Catches drift over time, not
   just at deploy. Pairs well with pattern 2 — pattern 2 catches
   per-deploy regressions, pattern 3 catches drift caused by
   upstream bronze-layer changes.

### Differential post-deploy validation: buildable vs BYO-ETL tables

`validate_omop.py` checks all 20 OMOP CDM v5.4 spec-covered tables,
but only 14 are built by the skill's pipeline. The other 6
(`visit_detail`, `device_exposure`, `note`, `note_nlp`, `specimen`,
`dose_era`) are BYO-ETL — your data, your schedule (architectural
decision AD-001; see the scaffolded `docs/omop-runbook.md`
Section 7.5 'BYO-ETL: validation-only tables' for the full
pattern).

For CI orchestration, the validation gate fires after each
surface lands:

| Surface | When validation runs | What it catches |
|---|---|---|
| 14 buildable tables | After `omop_full_build` Workflow completes (the scaffolded `resources/jobs.yml`) | Layer 1 schema drift, Layer 5 NOT NULL violations from upstream changes, Layer 3 FK integrity gaps from vocabulary updates |
| 6 BYO-ETL tables | After your separate ETL completes (Lakeflow Connect job, NLP pipeline, lab system integration) | Same 5 layers; depends on when your BYO-ETL pipeline runs |

The validator surfaces don't currently support per-invocation table
subsetting — neither `validate_omop.py --table` (single-table CLI)
nor the scaffolded notebook (iterates all 20 spec tables) takes a
"validate only these N tables" flag. Two practical implications:

1. **Use the notebook for full-sweep validation.** Run the
   scaffolded `src/99_validate_omop_output.py` as a Workflow
   notebook task after both your buildable and BYO-ETL pipelines
   have run. It iterates all 20 spec-covered tables; tables that
   haven't been built yet (or that your BYO-ETL hasn't loaded yet)
   short-circuit at Layer 1 with a clean `schema:table_missing`
   finding. Subsequent layers cleanly skip — no traceback, no false
   regression signal.

2. **Use the CLI for targeted single-table validation.** Run
   `python scripts/validate_omop.py --table <catalog>.<core_schema>.<table>`
   when you want feedback on one table without iterating the full
   set. This fits the developer-loop / hotfix shape; the notebook
   surface fits the steady-state Workflow shape.

Example DAG ordering for a Workflow that validates after both
surfaces complete:

```yaml
- task_key: omop_full_build
  # ... your skill-scaffolded build pipeline ...

- task_key: byo_etl_pipeline
  # ... your custom ETL for BYO-ETL tables ...

- task_key: validate_all_omop_tables
  depends_on:
    - task_key: omop_full_build
    - task_key: byo_etl_pipeline
  notebook_task:
    notebook_path: ${var.bundle_path}/src/99_validate_omop_output.py
    base_parameters:
      catalog: ${var.catalog}
      core_schema: ${var.core_schema}
      ref_schema: ${var.ref_schema}
```

If your BYO-ETL pipeline runs on a separate schedule from the
skill's build, run the validation notebook after each — the
short-circuit behavior means the validator gives you per-surface
feedback without false alarms on the other surface.

The skill ships `validate_omop.py`; orchestrating it is your
team's call.

---

## What about declining validation?

By design: **validation is offered prominently; declining is the
engineer's call.** The skill records
the offer-and-decline in the conversation but does not enforce.
Your team's deploy policy decides whether declining validation is
acceptable for production.

If your team **requires** validation, configure the post-deploy
Workflow task (pattern 2 above) to fail the pipeline on validation
findings:

```python
# Inside the validation Workflow task, exit non-zero on findings.
import sys
findings = run_validate_omop(...)
if findings:
    print(f"VALIDATION FAILED: {len(findings)} issues")
    sys.exit(1)
```

Then "declining" the agent's offer is moot — production deploys
gate on the validation regardless, and the engineer's decline is
just a record of "I chose not to pre-flight; the deploy gate
caught it."

If your team **allows** declining (e.g., for hotfix paths under
oncall judgment), the agent's response surface is the record of
that choice. Surface that record in your incident review process
if a declined-validation deploy correlates with a production issue.
