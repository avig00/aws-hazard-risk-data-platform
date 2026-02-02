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

from pyspark.sql.functions import col

from silver_utils import (
    normalize_columns,
    standardize_county_fips,
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

df_bronze = spark.read.option("header", "true").csv(src_path)
df = normalize_columns(df_bronze)

bronze_count = df_rowcount(df)

# stcofips is typically already a 5-digit county identifier
if "stcofips" in df.columns:
    df = df.withColumn("county_fips", col("stcofips").cast("string"))
elif "statefips" in df.columns and "countyfips" in df.columns:
    df = df.withColumn("county_fips", (col("statefips").cast("string") + col("countyfips").cast("string")))

df = standardize_county_fips(df, "county_fips")
df = dedupe(df, ["county_fips"])

silver_count = df_rowcount(df)
nr = null_rates(df, ["county_fips", "nri_id", "risk_score"])
log_validation_summary("nri_scores_clean", bronze_count, silver_count, nr)

df.write.mode("overwrite").parquet(out_path)
job.commit()
