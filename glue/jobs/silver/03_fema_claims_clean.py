"""
03_fema_claims_clean.py

FEMA HOUSING ASSISTANCE (OWNERS + RENTERS) → SILVER (fema_claims_clean)

Purpose:
    Consolidate FEMA housing assistance data for owners and renters into one
    typed, deduplicated fact dataset.

Inputs (Bronze):
    s3://<bucket>/<bronze_prefix>/fema/housing_assistance_owners/
    s3://<bucket>/<bronze_prefix>/fema/housing_assistance_renters/

Key transformations:
    - snake_case columns
    - union owners + renters into a single table
    - add tenure field ('owner' or 'renter')
    - cast common numeric measures (counts + dollar amounts)
    - dedupe by (id, tenure)

Important design choice:
    - We intentionally do NOT assign county_fips here.
      The claims files contain county names/zip, but not authoritative FIPS.
      County attribution happens safely in Gold via declarations:
        claim aggregates by disasternumber → declarations → county_fips

Output (Silver):
    s3://<bucket>/<silver_prefix>/fema_claims_clean/
    - Parquet
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType, LongType

from silver_utils import (
    normalize_columns,
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

owners_path = f"s3://{bucket}/{bronze_prefix}/fema/housing_assistance_owners/"
renters_path = f"s3://{bucket}/{bronze_prefix}/fema/housing_assistance_renters/"
out_path = f"s3://{bucket}/{silver_prefix}/fema_claims_clean/"

owners = normalize_columns(spark.read.option("header", "true").csv(owners_path)).withColumn("tenure", lit("owner"))
renters = normalize_columns(spark.read.option("header", "true").csv(renters_path)).withColumn("tenure", lit("renter"))

bronze_count = owners.count() + renters.count()

# Create a stable, union-safe schema (columns present in both will populate; missing become null)
base_cols = [
    "id", "tenure",
    "disasternumber", "state", "county", "city", "zipcode",
    "validregistrations", "approvedforfemaassistance",
    "totalapprovedihpamount", "repairreplaceamount", "rentalamount", "otherneedsamount",
    "totalinspected", "totaldamage",
]

def project(df):
    existing = [c for c in base_cols if c in df.columns]
    out = df.select(*existing)
    for c in base_cols:
        if c not in existing:
            out = out.withColumn(c, lit(None))
    return out.select(*base_cols)

claims = project(owners).unionByName(project(renters))

# Cast counts/ids
for c in ["disasternumber", "zipcode", "validregistrations", "approvedforfemaassistance", "totalinspected"]:
    if c in claims.columns:
        claims = claims.withColumn(c, col(c).cast(LongType()))

# Cast dollars / continuous measures
for c in ["totalapprovedihpamount", "repairreplaceamount", "rentalamount", "otherneedsamount", "totaldamage"]:
    if c in claims.columns:
        claims = claims.withColumn(c, col(c).cast(DoubleType()))

# Dedupe
claims = dedupe(claims, ["id", "tenure"])

silver_count = claims.count()
nr = null_rates(claims, ["id", "disasternumber", "state", "county"])
log_validation_summary("fema_claims_clean", bronze_count, silver_count, nr)

claims.write.mode("overwrite").parquet(out_path)
job.commit()
