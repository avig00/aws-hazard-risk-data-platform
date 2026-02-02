"""
02_fema_disaster_declarations_clean.py

FEMA DISASTER DECLARATIONS → SILVER (fema_disaster_declarations_clean)

Purpose:
    Clean FEMA disaster declaration data into a stable county-level reference
    table that provides authoritative county_fips for FEMA disasters.

Input (Bronze):
    s3://<bucket>/<bronze_prefix>/fema/disaster_declarations/

Key transformations:
    - snake_case columns
    - cast numeric identifiers
    - parse date fields
    - county_fips = fipsstatecode(2) + fipscountycode(3)
    - deduplicate (prefer FEMA's 'id' field)

Output (Silver):
    s3://<bucket>/<silver_prefix>/fema_disaster_declarations_clean/
    - Parquet

Why this matters for Gold:
    - FEMA claims tables do not have reliable county FIPS.
    - Gold will attribute claim aggregates to counties using:
        claims (by disasternumber) → declarations → county_fips
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col

from silver_utils import (
    normalize_columns,
    make_county_fips_from_state_county_codes,
    standardize_county_fips,
    dedupe,
    df_rowcount,
    null_rates,
    log_validation_summary,
    safe_to_date,
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

src_path = f"s3://{bucket}/{bronze_prefix}/fema/disaster_declarations/"
out_path = f"s3://{bucket}/{silver_prefix}/fema_disaster_declarations_clean/"

df_bronze = spark.read.option("header", "true").csv(src_path)
df = normalize_columns(df_bronze)

bronze_count = df_rowcount(df)

# Type casts (per your DDL)
for c in [
    "disasternumber", "fydeclared",
    "ihprogramdeclared", "iaprogramdeclared", "paprogramdeclared", "hmprogramdeclared",
    "tribalrequest",
    "fipsstatecode", "fipscountycode", "placecode",
    "declarationrequestnumber",
    "incidentid", "region",
]:
    if c in df.columns:
        df = df.withColumn(c, col(c).cast("bigint"))

# Parse dates
df = safe_to_date(df, [
    "declarationdate", "incidentbegindate", "incidentenddate",
    "disastercloseoutdate", "lastiafilingdate", "lastrefresh"
])

# Build county_fips
df = make_county_fips_from_state_county_codes(df, "fipsstatecode", "fipscountycode", "county_fips")
df = standardize_county_fips(df, "county_fips")

# Dedupe: prefer FEMA id if present
if "id" in df.columns:
    df = dedupe(df, ["id"])
else:
    df = dedupe(df, ["disasternumber", "county_fips", "incidenttype", "declarationtype"])

silver_count = df_rowcount(df)
nr = null_rates(df, ["disasternumber", "county_fips", "fydeclared"])
log_validation_summary("fema_disaster_declarations_clean", bronze_count, silver_count, nr)

df.write.mode("overwrite").parquet(out_path)
job.commit()
