"""Validate all configs/*.yaml against OMOPConfig and cross-field consistency rules."""

from pathlib import Path

import pytest
from config_loader import load_config

CONFIG_DIR = Path(__file__).resolve().parents[1] / "configs"

SKIP_FILES = {
    "_schema.yaml",
    # Verbatim Genie Code output kept as a demo artifact — intentionally does
    # NOT conform to the Pydantic schema. See .evidence/day4/step4_schema_validation.md
    # for the 14 validation errors and why they illustrate a useful teaching moment.
    "visit_occurrence_genie.yaml",
}

CONFIG_FILES = [
    str(p)
    for p in sorted(CONFIG_DIR.glob("*.yaml"))
    if p.name not in SKIP_FILES and not p.name.startswith("_")
]


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
