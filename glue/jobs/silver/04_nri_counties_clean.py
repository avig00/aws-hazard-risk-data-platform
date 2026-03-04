"""
04_nri_counties_clean.py

NRI COUNTIES → SILVER (nri_scores_clean)

Purpose:
    Clean and standardize National Risk Index county-level data into a
    join-ready reference table keyed by county_fips.

Input (Bronze):
    s3://<bucket>/<bronze_prefix>/nri/counties/

Key transformations:
    - snake_case columns
    - derive county_fips (prefer stcofips)
    - standardize county_fips (5-char, zero padded)
    - dedupe on county_fips

Output (Silver):
    s3://<bucket>/<silver_prefix>/nri_scores_clean/
    - Parquet

Why this matters for Gold:
    - risk_feature_mart joins NRI directly by county_fips
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, trim, when

from silver_utils import (
    normalize_columns,
    make_county_fips_from_state_county_codes,
    standardize_county_fips,
    apply_basic_county_fips_sanity,
    dedupe,
    df_rowcount,
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

src_path = f"s3://{bucket}/{bronze_prefix}/nri/counties/"
out_path = f"s3://{bucket}/{silver_prefix}/nri_scores_clean/"
unresolved_path = f"s3://{bucket}/{silver_prefix}/nri_scores_clean_unresolved_county_fips/"

df_bronze = spark.read.option("header", "true").csv(src_path)
df = normalize_columns(df_bronze)

bronze_count = df_rowcount(df)

# stcofips is typically already a 5-digit county identifier.
# If it exists in the source schema, use it exclusively and quarantine blanks
# instead of silently falling back to potentially malformed concatenation.
if "stcofips" in df.columns:
    df = df.withColumn(
        "county_fips",
        when(trim(col("stcofips").cast("string")) == "", None).otherwise(col("stcofips").cast("string")),
    )
elif "statefips" in df.columns and "countyfips" in df.columns:
    df = make_county_fips_from_state_county_codes(df, "statefips", "countyfips", "county_fips")
else:
    raise ValueError("NRI county file is missing stcofips and state/county fallback fields.")

df = standardize_county_fips(df, "county_fips")
df = dedupe(df, ["county_fips"])

df_valid = apply_basic_county_fips_sanity(df, "county_fips")
valid_keys = df_valid.select("county_fips").dropDuplicates(["county_fips"])
df_unresolved = df.join(valid_keys, "county_fips", "left_anti")

silver_count = df_rowcount(df_valid)
unresolved_count = df_rowcount(df_unresolved)
nr = null_rates(df_valid, ["county_fips", "nri_id", "risk_score"])
log_validation_summary("nri_scores_clean", bronze_count, silver_count, nr)
print(f"[VALIDATION] dataset=nri_scores_clean unresolved_rows={unresolved_count}")

df_valid.write.mode("overwrite").parquet(out_path)
df_unresolved.write.mode("overwrite").parquet(unresolved_path)
job.commit()
