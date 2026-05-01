"""Generic SDP pipeline driven by configs/*.yaml. One @dp.table per invocation, parameterized by table_name."""

from pyspark import pipelines as dp
from config_loader import ColumnMapping, load_config, resolve_sources, apply_joins
from vocab_resolver import apply_vocab_lookups
from column_mapper import build_select_exprs

# Generic YAML-driven SDP pipeline. Parameters are read from spark.conf set by the SDP pipeline
# configuration in resources/pipeline_generic.yml. Per-table invocation: each pipeline run sets
# table_name to one of person, visit_occurrence, etc., and reads {config_volume}/{table_name}.yaml.
# Output table is {catalog}.{core_schema}.{table_name} (pipeline catalog/schema + table name).

def _conf(key: str) -> str:
    """Read a pipeline configuration key with a clear error if missing."""
    try:
        val = spark.conf.get(key)
    except Exception:
        raise RuntimeError(
            f"Pipeline configuration key '{key}' is missing. "
            f"Set it in resources/pipeline_generic.yml under configuration."
        )
    if not val:
        raise RuntimeError(f"Pipeline configuration key '{key}' is empty.")
    return val

catalog = _conf("catalog")
bronze_schema = _conf("bronze_schema")
core_schema = _conf("core_schema")
ref_schema = _conf("ref_schema")
config_volume = _conf("config_volume")
table_name = _conf("table_name")

config_path = f"{config_volume}/{table_name}.yaml"
cfg = load_config(config_path)


def _apply_expectation_decorators(fn, expectations):
    """Wrap ``fn`` with ``@dp.expect_*`` decorators.

    Application order matters: outermost decorator executes first in SDP.
    We want: fail outermost (catch contract violations first), then drop
    (remove bad rows), then warn (log soft quality). This matches the
    hardcoded 02a_person_hardcoded.py decorator stacking order.
    """
    out = fn
    # Apply innermost first (warn), then drop, then fail outermost.
    # SDP evaluates outermost first → fail → drop → warn.
    if expectations.warn:
        out = dp.expect_all({e.name: e.expr for e in expectations.warn})(out)
    if expectations.drop:
        out = dp.expect_all_or_drop({e.name: e.expr for e in expectations.drop})(out)
    if expectations.fail:
        out = dp.expect_all_or_fail({e.name: e.expr for e in expectations.fail})(out)
    return out


def _transform_impl():
    source_dfs = resolve_sources(cfg, spark, catalog, bronze_schema)
    joined_df = apply_joins(source_dfs, cfg.joins)
    vocab_df = apply_vocab_lookups(
        joined_df, cfg.vocabulary_lookups, spark, catalog, ref_schema
    )
    # Auto-project vocab-resolved target columns that weren't explicitly restated in
    # column_mappings. Without this, YAML expectations like `known_race_concept: race_concept_id != 0`
    # fail with UNRESOLVED_COLUMN because build_select_exprs drops the vocab column. Treating
    # `target_column` as an implicit column_mapping keeps YAMLs DRY (one declaration per concept).
    explicit_targets = {m.target for m in cfg.column_mappings}
    vocab_passthrough = [
        ColumnMapping(target=v.target_column, expr=f"`{v.target_column}`")
        for v in cfg.vocabulary_lookups
        if v.target_column not in explicit_targets
    ]
    effective_mappings = list(cfg.column_mappings) + vocab_passthrough
    return build_select_exprs(vocab_df, effective_mappings)


_inner = _apply_expectation_decorators(_transform_impl, cfg.expectations)
transform = dp.table(name=table_name, comment=cfg.description)(_inner)
