# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### OMOP silver validation (5 layers)
# MAGIC Set widgets, then run. Requires `{catalog}.{ref_schema}.concept` and target `{catalog}.{core_schema}.{table}`.

# COMMAND ----------
"""Five-layer OMOP CDM validation (schema, PK, RI, domain, completeness) for core silver tables."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

try:
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("core_schema", "core_omop")
    dbutils.widgets.text("ref_schema", "reference")
    dbutils.widgets.text("table", "person")
except NameError:
    pass

spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.appName("validate_omop_output").getOrCreate()

catalog = dbutils.widgets.get("catalog")
core_schema = dbutils.widgets.get("core_schema")
ref_schema = dbutils.widgets.get("ref_schema")
table = dbutils.widgets.get("table")

fq_table = f"{catalog}.{core_schema}.{table}"
fq_concept = f"{catalog}.{ref_schema}.concept"

# OMOP CDM v5.4 required columns and expected Spark simple types (Layer 1).
REQUIRED_SCHEMA = {
    "person": {
        "person_id": LongType,
        "gender_concept_id": IntegerType,
        "year_of_birth": IntegerType,
        "month_of_birth": IntegerType,
        "day_of_birth": IntegerType,
        "birth_datetime": TimestampType,
        "death_datetime": TimestampType,
        "race_concept_id": IntegerType,
        "ethnicity_concept_id": IntegerType,
        "location_id": LongType,
        "provider_id": LongType,
        "care_site_id": LongType,
        "person_source_value": StringType,
        "gender_source_value": StringType,
        "gender_source_concept_id": IntegerType,
        "race_source_value": StringType,
        "race_source_concept_id": IntegerType,
        "ethnicity_source_value": StringType,
        "ethnicity_source_concept_id": IntegerType,
    },
    "visit_occurrence": {
        "visit_occurrence_id": LongType,
        "person_id": LongType,
        "visit_concept_id": IntegerType,
        "visit_start_date": DateType,
        "visit_start_datetime": TimestampType,
        "visit_end_date": DateType,
        "visit_end_datetime": TimestampType,
        "visit_type_concept_id": IntegerType,
        "provider_id": LongType,
        "care_site_id": LongType,
        "visit_source_value": StringType,
        "visit_source_concept_id": IntegerType,
        "admitted_from_concept_id": IntegerType,
        "admitted_from_source_value": StringType,
        "discharged_to_concept_id": IntegerType,
        "discharged_to_source_value": StringType,
        "preceding_visit_occurrence_id": LongType,
    },
}

# Concept columns that must exist in reference.concept (Layer 3); 0 = unknown, skipped in RI check.
RI_CONCEPT_COLUMNS = {
    "person": [
        "gender_concept_id",
        "race_concept_id",
        "ethnicity_concept_id",
    ],
    "visit_occurrence": [
        "visit_concept_id",
        "visit_type_concept_id",
    ],
}

# Layer 4: concept_id column -> expected OMOP domain_id in reference.concept
DOMAIN_CHECKS = {
    "person": [
        ("gender_concept_id", "Gender"),
        ("race_concept_id", "Race"),
        ("ethnicity_concept_id", "Ethnicity"),
    ],
    "visit_occurrence": [
        ("visit_concept_id", "Visit"),
        ("visit_type_concept_id", "Type Concept"),
    ],
}

# Layer 5: must have zero nulls
NOT_NULL_COLUMNS = {
    "person": [
        "person_id",
        "gender_concept_id",
        "year_of_birth",
        "race_concept_id",
        "ethnicity_concept_id",
    ],
    "visit_occurrence": [
        "visit_occurrence_id",
        "person_id",
        "visit_concept_id",
        "visit_start_date",
        "visit_end_date",
        "visit_type_concept_id",
    ],
}

NULLABLE_INFO_COLUMNS = {
    "person": [
        "month_of_birth",
        "day_of_birth",
        "birth_datetime",
        "death_datetime",
        "location_id",
        "provider_id",
        "care_site_id",
        "person_source_value",
        "gender_source_value",
        "gender_source_concept_id",
        "race_source_value",
        "race_source_concept_id",
        "ethnicity_source_value",
        "ethnicity_source_concept_id",
    ],
    "visit_occurrence": [
        "visit_start_datetime",
        "visit_end_datetime",
        "provider_id",
        "care_site_id",
        "visit_source_value",
        "visit_source_concept_id",
        "admitted_from_concept_id",
        "admitted_from_source_value",
        "discharged_to_concept_id",
        "discharged_to_source_value",
        "preceding_visit_occurrence_id",
    ],
}

PK_COLUMN = {"person": "person_id", "visit_occurrence": "visit_occurrence_id"}

results: list[dict] = []


def _record(layer: str, check: str, passed: bool, details: str) -> None:
    results.append(
        {
            "layer": layer,
            "check": check,
            "passed": passed,
            "details": details,
        }
    )


def _type_matches(field_type, expected_py_type) -> bool:
    return isinstance(field_type, expected_py_type)


# --- Layer 1: schema conformance ---
if table not in REQUIRED_SCHEMA:
    _record(
        "1_schema",
        "known_table",
        False,
        f"No REQUIRED_SCHEMA entry for {table!r}; skipping column checks.",
    )
else:
    df = spark.table(fq_table)
    fields = {f.name: f.dataType for f in df.schema.fields}
    req = REQUIRED_SCHEMA[table]
    missing = [c for c in req if c not in fields]
    wrong_type = [
        c
        for c in req
        if c in fields and not _type_matches(fields[c], req[c])
    ]
    ok = not missing and not wrong_type
    details = f"missing={missing}, wrong_type={wrong_type}" if not ok else "all required columns present with expected types"
    _record("1_schema", "required_columns_types", ok, details)

# --- Layer 2: primary key uniqueness ---
pk = PK_COLUMN.get(table)
if pk:
    dup = spark.sql(
        f"SELECT COUNT(*) - COUNT(DISTINCT {pk}) AS d FROM {fq_table}"
    ).collect()[0]["d"]
    ok = dup == 0
    _record("2_pk", f"unique_{pk}", ok, f"duplicate_rows={dup}")
else:
    _record("2_pk", "pk_check", False, f"No PK mapping for table {table!r}")

# --- Layer 3: referential integrity to concept ---
ri_cols = RI_CONCEPT_COLUMNS.get(table, [])
bad_total = 0
for col_name in ri_cols:
    n = spark.sql(
        f"""
        SELECT COUNT(*) AS c
        FROM {fq_table} t
        LEFT JOIN {fq_concept} c ON t.{col_name} = c.concept_id
        WHERE t.{col_name} != 0 AND c.concept_id IS NULL
        """
    ).collect()[0]["c"]
    bad_total += n
    _record(
        "3_ri",
        f"concept_fk_{col_name}",
        n == 0,
        f"orphan_rows={n}",
    )

# --- Layer 4: domain conformance ---
for col_name, domain in DOMAIN_CHECKS.get(table, []):
    mismatch = spark.sql(
        f"""
        SELECT COUNT(*) AS c
        FROM {fq_table} t
        INNER JOIN {fq_concept} c ON t.{col_name} = c.concept_id
        WHERE t.{col_name} != 0 AND c.domain_id != '{domain}'
        """
    ).collect()[0]["c"]
    _record(
        "4_domain",
        f"{col_name}_domain_{domain}",
        mismatch == 0,
        f"mismatch_rows={mismatch}",
    )

# --- Layer 5: completeness ---
for col_name in NOT_NULL_COLUMNS.get(table, []):
    nulls = spark.table(fq_table).filter(F.col(col_name).isNull()).count()
    _record(
        "5_completeness",
        f"not_null_{col_name}",
        nulls == 0,
        f"null_count={nulls}",
    )

for col_name in NULLABLE_INFO_COLUMNS.get(table, []):
    total = spark.table(fq_table).count()
    nulls = spark.table(fq_table).filter(F.col(col_name).isNull()).count()
    rate = (nulls / total) if total else 0.0
    _record(
        "5_completeness",
        f"nullable_info_{col_name}",
        True,
        f"null_count={nulls}, null_rate={rate:.4f}",
    )

# --- Summary ---
summary_df = spark.createDataFrame(results)
summary_df.orderBy("layer", "check").show(200, truncate=False)
print("--- validation summary (passed counts) ---")
summary_df.groupBy("passed").count().show()
