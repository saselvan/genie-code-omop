"""Validate all configs/*.yaml against OMOPConfig and cross-field consistency rules.

Phase 4 CI pipelines (GitHub Actions, Azure DevOps) run this test against the
scaffolded project on every PR. Out-of-the-box behavior on a fresh scaffold:

  - configs/ contains only `_schema.yaml` (which is excluded — see SKIP_FILES).
  - test_template_health below ensures pytest collects at least one test and
    exits 0, so the Phase 4 CI snippet succeeds immediately after scaffold.
  - As you commit configs/<table>.yaml files, test_config_validates picks them
    up automatically via the parametrize collection.

Skipping configs:
  - Add literal filenames to SKIP_FILES (e.g., a frozen reference config kept
    for documentation but intentionally non-conformant).
  - Or prefix the filename with `_` (underscore) — picked up by the
    `not p.name.startswith("_")` rule below. Use this for smoke / scratch
    configs (e.g., `_smoke_person.yaml`).
"""

from pathlib import Path

import pytest
from config_loader import load_config

CONFIG_DIR = Path(__file__).resolve().parents[1] / "configs"

SKIP_FILES: set[str] = {
    "_schema.yaml",
}

CONFIG_FILES = [
    str(p)
    for p in sorted(CONFIG_DIR.glob("*.yaml"))
    if p.name not in SKIP_FILES and not p.name.startswith("_")
]


def test_template_health() -> None:
    """Smoke check that always passes. Guarantees pytest collects ≥1 test, so
    `pytest tests/test_config_schema.py` exits 0 even when configs/ is empty
    (e.g., immediately after scaffold). Phase 4 CI snippets rely on this.
    """
    assert CONFIG_DIR.exists(), f"configs/ missing at {CONFIG_DIR}"


@pytest.mark.parametrize("config_path", CONFIG_FILES)
def test_config_validates(config_path):
    cfg = load_config(config_path)
    assert cfg.table_name
    assert len(cfg.sources) >= 1
    assert len(cfg.column_mappings) >= 1

    aliases = {s.alias for s in cfg.sources}
    targets = [m.target for m in cfg.column_mappings]
    assert len(targets) == len(set(targets)), f"duplicate column_mapping targets: {targets}"

    for j in cfg.joins:
        assert j.left in aliases, f"join left {j.left!r} not in sources"
        assert j.right in aliases, f"join right {j.right!r} not in sources"

    for v in cfg.vocabulary_lookups:
        assert (
            v.source_alias in aliases
        ), f"vocabulary_lookup source_alias {v.source_alias!r} not in sources"
