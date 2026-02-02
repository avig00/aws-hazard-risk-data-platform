"""
05_census_clean.py

CENSUS ACS 5-YEAR (2022) → SILVER (census_clean)

Purpose:
    Consolidate selected ACS 5-year (2022) tables into one county-level
    feature table suitable for ML and analytics.

Inputs (Bronze):
    s3://<bucket>/<bronze_prefix>/census/acs5_2022_B01001/  (population)
    s3://<bucket>/<bronze_prefix>/census/acs5_2022_B15003/  (education)
    s3://<bucket>/<bronze_prefix>/census/acs5_2022_B19013/  (income)
    s3://<bucket>/<bronze_prefix>/census/acs5_2022_B23025/  (employment)
    s3://<bucket>/<bronze_prefix>/census/acs5_2022_B25077/  (home value)

Key transformations:
    - snake_case columns
    - build county_fips from state + county codes
    - handle duplicate NAME columns robustly (Spark may rename to name0/name1/etc.)
    - select a small, meaningful feature subset
    - left-join all tables onto county_fips
    - add acs_year=2022
    - dedupe on county_fips

Output (Silver):
    s3://<bucket>/<silver_prefix>/census_clean/
    - Parquet

Why this matters for Gold:
    - risk_feature_mart joins Census features by county_fips
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, lit, coalesce

from silver_utils import (
    normalize_columns,
    make_county_fips_from_state_county_codes,
    standardize_county_fips,
    dedupe,
    null_rates,
    log_validation_summary,
)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "BRONZE_PREFIX", "SILVER_PREFIX"])
bucket = args["S3_BUCKET"]
bronze_prefix = args["BRONZE_PREFIX"]
silver_prefix = args["SILVER_PREFIX"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

paths = {
    "b01001": f"s3://{bucket}/{bronze_prefix}/census/acs5_2022_B01001/",
    "b15003": f"s3://{bucket}/{bronze_prefix}/census/acs5_2022_B15003/",
    "b19013": f"s3://{bucket}/{bronze_prefix}/census/acs5_2022_B19013/",
    "b23025": f"s3://{bucket}/{bronze_prefix}/census/acs5_2022_B23025/",
    "b25077": f"s3://{bucket}/{bronze_prefix}/census/acs5_2022_B25077/",
}
out_path = f"s3://{bucket}/{silver_prefix}/census_clean/"


def read_norm(path: str):
    df = normalize_columns(spark.read.option("header", "true").csv(path))
    df = make_county_fips_from_state_county_codes(df, "state", "county", "county_fips")
    df = standardize_county_fips(df, "county_fips")
    return df


def canonicalize_name_column(df, preferred_out_col: str = "name"):
    """
    Spark can auto-rename duplicate 'name' columns after joins into name0, name1, name198, etc.
    This helper ensures there is a single canonical column `preferred_out_col` and drops extras.
    """
    name_candidates = [c for c in df.columns if c == preferred_out_col or c.startswith("name")]
    if not name_candidates:
        return df

    if preferred_out_col not in df.columns:
        df = df.withColumn(preferred_out_col, coalesce(*[col(c) for c in name_candidates]))
    else:
        others = [c for c in name_candidates if c != preferred_out_col]
        if others:
            df = df.withColumn(preferred_out_col, coalesce(col(preferred_out_col), *[col(c) for c in others]))

    drop_dupe_names = [c for c in df.columns if c.startswith("name") and c != preferred_out_col]
    if drop_dupe_names:
        df = df.drop(*drop_dupe_names)

    return df


b01001_raw = read_norm(paths["b01001"])
b01001_raw = canonicalize_name_column(b01001_raw, "name")

# Baseline table: population + county_name
b01001 = b01001_raw.select(
    "county_fips",
    col("name").alias("county_name"),
    col("b01001_001e").cast("bigint").alias("population_total"),
).dropDuplicates(["county_fips"])

b19013 = read_norm(paths["b19013"]).select(
    "county_fips",
    col("b19013_001e").cast("bigint").alias("median_household_income"),
).dropDuplicates(["county_fips"])

b25077 = read_norm(paths["b25077"]).select(
    "county_fips",
    col("b25077_001e").cast("bigint").alias("median_home_value"),
).dropDuplicates(["county_fips"])

b23025 = read_norm(paths["b23025"]).select(
    "county_fips",
    col("b23025_001e").cast("bigint").alias("employment_universe_total"),
    col("b23025_002e").cast("bigint").alias("in_labor_force"),
    col("b23025_005e").cast("bigint").alias("unemployed"),
).dropDuplicates(["county_fips"])

b15003 = read_norm(paths["b15003"]).select(
    "county_fips",
    col("b15003_001e").cast("bigint").alias("education_universe_total"),
    col("b15003_017e").cast("bigint").alias("high_school_grad"),
    col("b15003_022e").cast("bigint").alias("bachelors"),
    col("b15003_025e").cast("bigint").alias("graduate_degree"),
).dropDuplicates(["county_fips"])

# Use population table as the baseline frame
bronze_count = b01001.count()

df = (
    b01001
    .join(b19013, "county_fips", "left")
    .join(b25077, "county_fips", "left")
    .join(b23025, "county_fips", "left")
    .join(b15003, "county_fips", "left")
    .withColumn("acs_year", lit(2022))
)

df = dedupe(df, ["county_fips"])

silver_count = df.count()
nr = null_rates(df, ["county_fips", "population_total", "median_household_income"])
log_validation_summary("census_clean", bronze_count, silver_count, nr)

df.write.mode("overwrite").parquet(out_path)
job.commit()
