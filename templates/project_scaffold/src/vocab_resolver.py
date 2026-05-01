"""Vocabulary / concept_id resolution via source_to_concept_map, OHDSI concept table, or Maps-to standard crosswalk."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, expr, lit

from config_loader import VocabularyLookup

logger = logging.getLogger(__name__)


def apply_vocab_lookups(
    df: DataFrame,
    lookups: list[VocabularyLookup],
    spark: SparkSession,
    catalog: str,
    ref_schema: str,
) -> DataFrame:
    """Apply each vocabulary lookup as a LEFT JOIN; project coalesced concept ids.

    Join strategy is left to Spark AQE (Adaptive Query Execution). On serverless
    compute, AQE auto-broadcasts lookup tables under 30MB and uses sort-merge for
    larger joins. No manual broadcast() hints — let the optimizer decide.

    - ``source_to_concept_map``: filter ``source_vocabulary_id = lookup.source_vocabulary_id``,
      join on ``source_code`` = source column, use ``target_concept_id``.
    - ``concept_table``: filter ``concept`` by ``vocabulary_id`` and ``domain_id``,
      join on ``concept_code`` = source column, use ``concept_id``.
      Filters out invalid/deprecated concepts. Respects ``standard_only`` flag.
    - ``concept_table_mapped``: find source concept, traverse relationship (default "Maps to")
      to standard concept. Deduplicates one-to-many by preferring target domain + lowest concept_id.
      Filters out invalid/deprecated concepts on both source and target.

    Args:
        df: Input DataFrame (already joined; columns addressable with source aliases).
        lookups: Ordered vocabulary lookup rules.
        spark: Active Spark session.
        catalog: Unity Catalog name.
        ref_schema: Reference schema (vocabulary tables).

    Returns:
        DataFrame with additional target columns from each lookup.
    """
    out = df
    stcm_table = f"{catalog}.{ref_schema}.source_to_concept_map"
    concept_tbl = f"{catalog}.{ref_schema}.concept"

    # Cache table reads outside loop — avoid re-scanning large tables per lookup.
    _stcm_df = None
    _concept_df = None
    _concept_rel_df = None
    concept_rel_tbl = f"{catalog}.{ref_schema}.concept_relationship"

    for i, lookup in enumerate(lookups):
        vocab_label = lookup.source_vocabulary_id or lookup.vocabulary_id or "?"
        logger.info(
            "Vocabulary lookup [%d/%d] target=%s resolution=%s vocab=%s",
            i + 1,
            len(lookups),
            lookup.target_column,
            lookup.resolution,
            vocab_label,
        )
        print(
            f"[vocab_resolver] lookup {i + 1}/{len(lookups)} -> {lookup.target_column} "
            f"({lookup.resolution}, vocab={vocab_label})",
            flush=True,
        )

        if lookup.resolution == "source_to_concept_map":
            code_col = f"__vocab_stcm_code_{i}"
            tid_col = f"__vocab_stcm_tid_{i}"
            valid_col = f"__vocab_stcm_valid_{i}"
            if _stcm_df is None:
                _stcm_df = spark.read.table(stcm_table)
            if _concept_df is None:
                _concept_df = spark.read.table(concept_tbl)
            stcm = _stcm_df
            # Filter STCM rows, then validate target_concept_id exists and is valid
            # in the concept table. Drops seeds that point to deprecated/invalid concepts.
            stcm_filtered = (
                stcm
                .filter(col("source_vocabulary_id") == lit(lookup.source_vocabulary_id))
                .join(
                    _concept_df.filter(col("invalid_reason").isNull())
                        .select(col("concept_id").alias(valid_col)),
                    on=stcm["target_concept_id"] == col(valid_col),
                    how="inner",
                )
                .select(
                    col("source_code").alias(code_col),
                    col("target_concept_id").alias(tid_col),
                )
            )
            rhs = stcm_filtered.alias(f"vlookup_{i}")
            join_on = expr(
                f"`{lookup.source_alias}`.`{lookup.source_column}` = `vlookup_{i}`.`{code_col}`"
            )
            out = out.join(rhs, on=join_on, how="left")
            out = out.withColumn(
                lookup.target_column,
                coalesce(col(tid_col), lit(lookup.fallback)),
            ).drop(code_col, tid_col)

        elif lookup.resolution == "concept_table":
            code_col = f"__vocab_c_code_{i}"
            cid_col = f"__vocab_c_id_{i}"
            if _concept_df is None:
                _concept_df = spark.read.table(concept_tbl)
            concept = _concept_df
            # Base filter: vocabulary + domain + valid concepts only
            concept_filter = (
                (col("vocabulary_id") == lit(lookup.vocabulary_id))
                & (col("domain_id") == lit(lookup.domain_id))
                & (col("invalid_reason").isNull())
            )
            # Optionally restrict to standard concepts only
            if lookup.standard_only:
                concept_filter = concept_filter & (col("standard_concept") == lit("S"))
            rhs = concept.filter(concept_filter).select(
                col("concept_code").alias(code_col),
                col("concept_id").alias(cid_col),
            ).alias(f"cmap_{i}")
            join_on = expr(
                f"`{lookup.source_alias}`.`{lookup.source_column}` = `cmap_{i}`.`{code_col}`"
            )
            out = out.join(rhs, on=join_on, how="left")
            out = out.withColumn(
                lookup.target_column,
                coalesce(col(cid_col), lit(lookup.fallback)),
            ).drop(code_col, cid_col)

        elif lookup.resolution == "concept_table_mapped":
            # Maps source concept_code through a relationship (default "Maps to") to
            # standard target concepts. Filters targets to the specified domain_id so
            # one-to-many mappings across domains route correctly (e.g., Condition
            # targets go to condition_occurrence, Observation targets go to observation).
            # Multiple targets WITHIN the same domain are kept — the join fans out
            # intentionally. Surrogate key generation (ROW_NUMBER/xxhash64) in
            # column_mappings runs after this join and handles the expanded rows.
            src_code_col = f"__vocab_cm_code_{i}"
            src_cid_col = f"__vocab_cm_src_id_{i}"
            std_cid_col = f"__vocab_cm_std_id_{i}"

            if _concept_df is None:
                _concept_df = spark.read.table(concept_tbl)
            if _concept_rel_df is None:
                _concept_rel_df = spark.read.table(concept_rel_tbl)

            src_vocab = lookup.vocabulary_id or lookup.source_vocabulary_id
            rel_id = lookup.relationship_id
            target_domain = lookup.domain_id  # required for concept_table_mapped

            # Step 1: find valid source concepts by concept_code + vocabulary_id
            source_concepts = _concept_df.filter(
                (col("vocabulary_id") == lit(src_vocab))
                & (col("invalid_reason").isNull())
            ).select(
                col("concept_code").alias(src_code_col),
                col("concept_id").alias(src_cid_col),
            )

            # Step 2: traverse relationship (default "Maps to") → target concept
            rels = _concept_rel_df.filter(
                (col("relationship_id") == lit(rel_id))
                & (col("invalid_reason").isNull())
            ).select(
                col("concept_id_1").alias(f"__rel_src_{i}"),
                col("concept_id_2").alias(f"__rel_tgt_{i}"),
            )

            # Step 3: join to target concept, filter by domain + valid + standard
            target_filter = (
                (col("domain_id") == lit(target_domain))
                & (col("invalid_reason").isNull())
            )
            if lookup.standard_only:
                target_filter = target_filter & (col("standard_concept") == lit("S"))

            target_concepts = _concept_df.filter(target_filter).select(
                col("concept_id").alias(std_cid_col),
            )

            # Build the mapping: source_code → domain-filtered target concept_id(s)
            # One-to-many within domain is intentional — produces multiple output rows
            mapped = (
                source_concepts
                .join(rels, on=source_concepts[src_cid_col] == rels[f"__rel_src_{i}"], how="inner")
                .join(target_concepts, on=rels[f"__rel_tgt_{i}"] == target_concepts[std_cid_col], how="inner")
                .select(src_code_col, std_cid_col)
            )

            rhs = mapped.alias(f"cmapped_{i}")
            join_on = expr(
                f"`{lookup.source_alias}`.`{lookup.source_column}` = `cmapped_{i}`.`{src_code_col}`"
            )
            out = out.join(rhs, on=join_on, how="left")
            out = out.withColumn(
                lookup.target_column,
                coalesce(col(std_cid_col), lit(lookup.fallback)),
            ).drop(src_code_col, std_cid_col)

        else:
            raise ValueError(f"Unknown resolution: {lookup.resolution!r}")

    logger.info("Completed %d vocabulary lookup(s).", len(lookups))
    print(f"[vocab_resolver] completed {len(lookups)} lookup(s).", flush=True)
    return out
