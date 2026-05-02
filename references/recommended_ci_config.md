# Recommended CI/CD integration

This document describes how to integrate `omop-pipeline-builder`'s
validators into your CI/CD pipeline. **The skill itself does not
deploy** — your CI/CD pipeline owns deploy. These snippets help you
wire the skill's validators into the pipeline you already have.

The snippets are working YAML, written for the project layout the
skill scaffolds (Phase 0a / Step 1 of `SKILL.md`). Adjust paths if
your bundle lives elsewhere.

---

## What runs in CI

Two validators belong in CI for OMOP builds. Both are **fast** and
**stateless** — exactly the shape CI is good at:

1. **Pydantic schema validation.** Validates every `configs/*.yaml`
   file in the project against the OMOP config schema. Catches
   malformed YAML, missing required fields, invalid resolution
   strategies, cross-field violations (e.g., a vocabulary lookup
   declares `resolution: concept_table` but omits `domain_id`). Runs
   the scaffolded `tests/test_config_schema.py`. Fast: ≪5 seconds
   per run.

2. **Databricks Asset Bundle validation.** Runs
   `databricks bundle validate -t <target>`. Catches missing
   pipeline references, malformed `depends_on` chains, schema
   violations in `resources/*.yml`, missing variables. Fast: ≪10
   seconds per run.

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
        # passes (see templates/tests/test_config_schema.py), so
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

`<TODO Step 2: filled in by Phase 4 Step 2 commit.>`

<!--
Step-2 placeholder. The Azure DevOps Pipelines snippet ships in
the next commit of this phase. Same coverage as the GitHub Actions
snippet (Pydantic + bundle validate, no deploy, no pipeline runs),
adapted to Azure DevOps service connection patterns.
-->

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
  pipelines (Decision 12 in the v2.0 architecture log).
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
   each. The agent's `SKILL.md` Step 7 documents this flow.

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

The skill ships `validate_omop.py`; orchestrating it is your
team's call.

---

## What about declining validation?

Decision 14 in the v2.0 architecture log: **validation is offered
prominently; declining is the engineer's call.** The skill records
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
