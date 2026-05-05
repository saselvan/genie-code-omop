"""OHDSI vocabulary + custom source_to_concept_map seed loader.

Reference data load. Run once per environment. Not an SDP pipeline — COPY INTO
is the right tool for bulk reference data.

Pre-req: Download the Athena vocab bundle, extract CSVs, and upload them to
{vocab_volume_path} (tab-separated, header row). Default volume:
/Volumes/{catalog}/{ref_schema}/vocabulary_files/.

Runs as a Databricks Python task or as a notebook (re-add a `# Databricks
notebook source` header on line 1 if you want notebook-task semantics).
"""

dbutils.widgets.text("catalog", "your_catalog", "catalog")
dbutils.widgets.text("ref_schema", "reference", "ref_schema")
dbutils.widgets.text(
    "vocab_volume_path",
    "/Volumes/your_catalog/reference/vocabulary_files",
    "vocab_volume_path",
)
dbutils.widgets.text(
    "custom_seed_csv",
    "/Volumes/your_catalog/reference/vocabulary_files/source_to_concept_map_custom.csv",
    "custom_seed_csv",
)
dbutils.widgets.text(
    "workspace_seed_csv",
    "",
    "workspace_seed_csv",
)

catalog = dbutils.widgets.get("catalog").strip()
ref_schema = dbutils.widgets.get("ref_schema").strip()
vocab_volume_path = dbutils.widgets.get("vocab_volume_path").rstrip("/")
custom_seed_csv = dbutils.widgets.get("custom_seed_csv").strip()
workspace_seed_csv = dbutils.widgets.get("workspace_seed_csv").strip()

fq_ref = f"`{catalog}`.`{ref_schema}`"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq_ref}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {fq_ref}.`vocabulary_files`")

# COMMAND ----------

# OHDSI Athena CSV column order matches CREATE TABLE column order below.
# Dates are STRING for robust COPY INTO (Athena uses YYYYMMDD; nullable cols may be empty).
VOCAB_TABLE_DDL = {
    "concept": """
        concept_id INT,
        concept_name STRING,
        domain_id STRING,
        vocabulary_id STRING,
        concept_class_id STRING,
        standard_concept STRING,
        concept_code STRING,
        valid_start_date STRING,
        valid_end_date STRING,
        invalid_reason STRING
    """,
    "concept_relationship": """
        concept_id_1 INT,
        concept_id_2 INT,
        relationship_id STRING,
        valid_start_date STRING,
        valid_end_date STRING,
        invalid_reason STRING
    """,
    "concept_ancestor": """
        ancestor_concept_id INT,
        descendant_concept_id INT,
        min_levels_of_separation INT,
        max_levels_of_separation INT
    """,
    "concept_synonym": """
        concept_id INT,
        concept_synonym_name STRING,
        language_concept_id INT
    """,
    "concept_class": """
        concept_class_id STRING,
        concept_class_name STRING,
        concept_class_concept_id INT
    """,
    "vocabulary": """
        vocabulary_id STRING,
        vocabulary_name STRING,
        vocabulary_reference STRING,
        vocabulary_version STRING,
        vocabulary_concept_id INT
    """,
    "domain": """
        domain_id STRING,
        domain_name STRING,
        domain_concept_id INT
    """,
    "relationship": """
        relationship_id STRING,
        relationship_name STRING,
        is_hierarchical STRING,
        defines_ancestry STRING,
        reverse_relationship_id STRING,
        relationship_concept_id INT
    """,
    "drug_strength": """
        drug_concept_id INT,
        ingredient_concept_id INT,
        amount_value DOUBLE,
        amount_unit_concept_id INT,
        numerator_value DOUBLE,
        numerator_unit_concept_id INT,
        denominator_value DOUBLE,
        denominator_unit_concept_id INT,
        box_size INT,
        valid_start_date STRING,
        valid_end_date STRING,
        invalid_reason STRING
    """,
    "concept_cpt4": """
        concept_id INT,
        concept_name STRING,
        domain_id STRING,
        vocabulary_id STRING,
        concept_class_id STRING,
        standard_concept STRING,
        concept_code STRING,
        valid_start_date STRING,
        valid_end_date STRING,
        invalid_reason STRING
    """,
}

# File base name (Athena zip) -> logical table key in VOCAB_TABLE_DDL
VOCAB_LOAD_ORDER = [
    ("CONCEPT", "concept"),
    ("CONCEPT_RELATIONSHIP", "concept_relationship"),
    ("CONCEPT_ANCESTOR", "concept_ancestor"),
    ("CONCEPT_SYNONYM", "concept_synonym"),
    ("CONCEPT_CLASS", "concept_class"),
    ("VOCABULARY", "vocabulary"),
    ("DOMAIN", "domain"),
    ("RELATIONSHIP", "relationship"),
    ("DRUG_STRENGTH", "drug_strength"),
    ("CONCEPT_CPT4", "concept_cpt4"),
]

# COMMAND ----------

# Parse each DDL block into (name, type) pairs so we can build the SELECT-with-CAST used inside
# COPY INTO. We have to cast explicitly because COPY INTO from a CSV scan infers integer columns
# as INT, and Delta refuses to merge an INT source into a BIGINT target column with
# [DELTA_FAILED_TO_MERGE_FIELDS]. `FORMAT_OPTIONS ('inferSchema'='false')` alone is not sufficient
# on UC volumes — Delta still compares the read-side inferred schema to the table schema.
def _parse_ddl_cols(ddl: str):
    cols = []
    for line in ddl.strip().splitlines():
        line = line.strip().rstrip(",")
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        cols.append((parts[0], " ".join(parts[1:])))
    return cols


# Post-load assertion that the load-bearing tables
# are non-empty. The COPY INTO loop below intentionally swallows per-file
# failures (so the CPT4-license-gated vocabulary stays a soft skip), which
# means without this assertion the entire load could no-op silently and
# downstream pipelines would resolve every concept_id to 0. Pure function
# (no module-level dbutils dependency) so the assertion logic is testable
# in isolation via AST extraction.
#
# Tables NOT asserted because they are legitimately empty in supported
# configurations:
#   - source_to_concept_map: empty until the customer seeds custom codes
#     per docs/omop-runbook.md Section 6.2.
#   - concept_cpt4: license-gated (separate Athena license step; the
#     comment at the bottom of this file documents the soft skip).
#   - concept_synonym, concept_ancestor, drug_strength: may be omitted
#     from minimal Athena bundles; their absence does not invalidate
#     the core load.
def _assert_vocabulary_load_succeeded(
    spark,
    fq_ref: str,
    vocab_volume_path: str,
    required_tables=("concept", "concept_relationship"),
):
    """Raise RuntimeError if any required vocabulary table is empty.

    Closes the silent-success failure mode where empty vocabulary loads previously exited successfully.
    Before this assertion: a job exiting SUCCESS did not mean data
    loaded — the COPY INTO loop catches and logs per-file failures
    (intentional for the CPT4-license-gated case), so the entire load
    could no-op and the job would still exit 0. After this assertion:
    if the job exits SUCCESS, the load-bearing tables are non-empty.

    Parameters are injected so the function can be unit-tested with a
    mock spark; production call site below uses the real spark/dbutils
    bindings from the notebook environment.
    """
    empty_tables = []
    for t in required_tables:
        n = spark.sql(f"SELECT COUNT(*) AS c FROM {fq_ref}.`{t}`").collect()[0]["c"]
        if n == 0:
            empty_tables.append(t)
    if empty_tables:
        raise RuntimeError(
            "Vocabulary load failed: the following load-bearing "
            f"tables contain zero rows: {empty_tables}. Expected ~6-10M rows "
            "in `concept` and ~55M rows in `concept_relationship` after a "
            "full OHDSI Athena load. Common causes:\n"
            "  (1) CSV files not present on the Volume — verify with: "
            f"databricks fs ls '{vocab_volume_path}/'\n"
            "  (2) COPY INTO hit a type mismatch — re-run this script as "
            "a notebook and check the per-cell SKIP messages above.\n"
            "  (3) Wrong volume path — verify the vocab_volume_path widget "
            f"value (current: '{vocab_volume_path}').\n"
            "See docs/omop-runbook.md Section 6.1 for the full "
            "troubleshooting flow. If the issue persists after working through "
            "Section 6.1, file an issue at "
            "https://github.com/saselvan/genie-code-omop/issues."
        )


# COMMAND ----------

for file_base, table_key in VOCAB_LOAD_ORDER:
    fq_table = f"{fq_ref}.`{table_key}`"
    ddl_cols = VOCAB_TABLE_DDL[table_key]
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {fq_table} (
            {ddl_cols}
        )
        USING DELTA
        """
    )
    path = f"{vocab_volume_path}/{file_base}.csv"
    select_list = ", ".join(
        f"CAST(`{c}` AS {t}) AS `{c}`" for c, t in _parse_ddl_cols(ddl_cols)
    )
    try:
        spark.sql(
            f"""
            COPY INTO {fq_table}
            FROM (SELECT {select_list} FROM '{path}')
            FILEFORMAT = CSV
            FORMAT_OPTIONS (
                'header' = 'true',
                'delimiter' = '\\t'
            )
            """
        )
        print(f"OK COPY INTO {fq_table} <- {path}")
    except Exception as e:
        print(f"SKIP {file_base}: {e}")

# COMMAND ----------

# source_to_concept_map — OHDSI CDM v5.4-style columns (UC table name lower snake_case).
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {fq_ref}.`source_to_concept_map` (
        source_code STRING,
        source_concept_id INT,
        source_vocabulary_id STRING,
        source_code_description STRING,
        target_concept_id INT,
        target_vocabulary_id STRING,
        valid_start_date DATE,
        valid_end_date DATE,
        invalid_reason STRING
    )
    USING DELTA
    """
)

# Prefer repo/workspace path when provided; else use volume path (upload `seed_data/source_to_concept_map_custom.csv` there).
seed_path = workspace_seed_csv if workspace_seed_csv else custom_seed_csv
if workspace_seed_csv:
    print(f"Using workspace/repo seed path: {seed_path}")
else:
    print(
        "Using volume seed path (upload repo file seed_data/source_to_concept_map_custom.csv to this path if needed): "
        + seed_path
    )

from pyspark.sql import functions as F

seed_df = (
    spark.read.option("header", True)
    .option("inferSchema", False)
    .csv(seed_path)
    .withColumn("source_concept_id", F.col("source_concept_id").cast("int"))
    .withColumn("target_concept_id", F.col("target_concept_id").cast("int"))
    .withColumn(
        "valid_start_date",
        F.to_date(F.col("valid_start_date").cast("string"), "yyyyMMdd"),
    )
    .withColumn(
        "valid_end_date",
        F.to_date(F.col("valid_end_date").cast("string"), "yyyyMMdd"),
    )
    .withColumn(
        "invalid_reason",
        F.when(
            F.col("invalid_reason").isNull()
            | (F.trim(F.col("invalid_reason").cast("string")) == ""),
            F.lit(None),
        ).otherwise(F.col("invalid_reason")),
    )
)

seed_df.createOrReplaceTempView("_stcm_custom_seed")

spark.sql(
    f"""
    MERGE INTO {fq_ref}.`source_to_concept_map` AS t
    USING _stcm_custom_seed AS s
    ON  t.source_code = s.source_code
    AND t.source_vocabulary_id = s.source_vocabulary_id
    WHEN MATCHED THEN UPDATE SET
        t.source_concept_id = s.source_concept_id,
        t.source_code_description = s.source_code_description,
        t.target_concept_id = s.target_concept_id,
        t.target_vocabulary_id = s.target_vocabulary_id,
        t.valid_start_date = s.valid_start_date,
        t.valid_end_date = s.valid_end_date,
        t.invalid_reason = s.invalid_reason
    WHEN NOT MATCHED THEN INSERT (
        source_code,
        source_concept_id,
        source_vocabulary_id,
        source_code_description,
        target_concept_id,
        target_vocabulary_id,
        valid_start_date,
        valid_end_date,
        invalid_reason
    ) VALUES (
        s.source_code,
        s.source_concept_id,
        s.source_vocabulary_id,
        s.source_code_description,
        s.target_concept_id,
        s.target_vocabulary_id,
        s.valid_start_date,
        s.valid_end_date,
        s.invalid_reason
    )
    """
)
print("Merged custom source_to_concept_map seed (upsert by source_code + source_vocabulary_id).")

# COMMAND ----------

concept_cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {fq_ref}.`concept`").collect()[0]["c"]
print(f"concept row count (expect ~6M after full Athena load): {concept_cnt}")

count_tables = [k for _, k in VOCAB_LOAD_ORDER] + ["source_to_concept_map"]
for t in count_tables:
    try:
        n = spark.sql(f"SELECT COUNT(*) AS c FROM {fq_ref}.`{t}`").collect()[0]["c"]
        print(f"{catalog}.{ref_schema}.{t}: {n}")
    except Exception as ex:
        print(f"{catalog}.{ref_schema}.{t}: <error {ex}>")

# COMMAND ----------

_assert_vocabulary_load_succeeded(spark, fq_ref, vocab_volume_path)
print(
    "Post-load assertion: PASS — load-bearing vocabulary tables "
    "(concept, concept_relationship) are non-empty."
)

# COMMAND ----------

# Athena vocabulary download:
#   - Bulk download (CSV bundle): https://athena.ohdsi.org
#   - CONCEPT_CPT4.csv is optional until you complete the separate CPT4
#     license / vocabulary key step in Athena; this script skips missing
#     files without failing the run.
