# Databricks notebook source
# MAGIC %md
# MAGIC # OHDSI vocabulary + custom source_to_concept_map seed
# MAGIC
# MAGIC **Reference data load.** Run once per environment. Not an SDP pipeline — `COPY INTO` is the right tool for bulk reference data.
# MAGIC
# MAGIC **Pre-req:** Download the Athena vocab bundle, extract CSVs, and upload them to `{vocab_volume_path}` (tab-separated, header row). Default volume: `/Volumes/{catalog}/{ref_schema}/vocabulary_files/`.

# COMMAND ----------

# Databricks notebook source
dbutils.widgets.text("catalog", "samuels_fevm_catalog", "catalog")
dbutils.widgets.text("ref_schema", "reference", "ref_schema")
dbutils.widgets.text(
    "vocab_volume_path",
    "/Volumes/samuels_fevm_catalog/reference/vocabulary_files",
    "vocab_volume_path",
)
dbutils.widgets.text(
    "custom_seed_csv",
    "/Volumes/samuels_fevm_catalog/reference/vocabulary_files/source_to_concept_map_custom.csv",
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

# Databricks notebook source
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq_ref}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {fq_ref}.`vocabulary_files`")

# COMMAND ----------

# Databricks notebook source
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


# COMMAND ----------

# Databricks notebook source
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

# Databricks notebook source
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

# Databricks notebook source
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

# MAGIC %md
# MAGIC ## Athena vocabulary download
# MAGIC
# MAGIC - Bulk download (CSV bundle): [https://athena.ohdsi.org](https://athena.ohdsi.org)
# MAGIC - **CONCEPT_CPT4.csv** is optional until you complete the separate CPT4 license / vocabulary key step in Athena; this notebook skips missing files without failing the run.
