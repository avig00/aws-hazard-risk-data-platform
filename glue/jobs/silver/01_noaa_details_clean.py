"""
01_noaa_details_clean.py

NOAA DETAILS → SILVER (noaa_events_clean)

Fixes included:
  - Some NOAA CSVs contain duplicate YEAR-like headers that normalize to the same name ("year"),
    which causes Spark "Reference 'year' is ambiguous".
  - We force unique column names right after normalize_columns() by renaming duplicates to <col>__dupN.
  - Drop rows missing required grain keys (prevents downstream blocking quality failures), including
    handling blank strings that should be treated as null.
  - Use a hard filter for required keys (more reliable than dropna in the presence of odd CSV/schema edge cases).
"""

import sys
from functools import reduce

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, to_timestamp, regexp_extract, trim, when
from pyspark.sql.types import IntegerType, DoubleType, StringType

from silver_utils import (
    normalize_columns,
    make_county_fips_from_noaa_state_cz,
    standardize_county_fips,
    dedupe,
    df_rowcount,
    null_rates,
    log_validation_summary,
)


def make_unique_columns(df):
    """
    Rename duplicate column names by appending __dupN so Spark references are unambiguous.
    Example: ["year","year"] -> ["year","year__dup1"]
    """
    seen = {}
    new_cols = []
    for c in df.columns:
        if c not in seen:
            seen[c] = 0
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}__dup{seen[c]}")
    return df.toDF(*new_cols)


args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "BRONZE_PREFIX", "SILVER_PREFIX"])
bucket = args["S3_BUCKET"]
bronze_prefix = args["BRONZE_PREFIX"]
silver_prefix = args["SILVER_PREFIX"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# IMPORTANT: set before read/plan
spark.conf.set("spark.sql.caseSensitive", "true")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

src_path = f"s3://{bucket}/{bronze_prefix}/noaa/details/"
out_path = f"s3://{bucket}/{silver_prefix}/noaa_events_clean/"

df_bronze = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(src_path)
)

df = normalize_columns(df_bronze)
df = make_unique_columns(df)   # prevent ambiguous references like year/year

bronze_count = df_rowcount(df)

# Parse timestamps
for c in ["begin_date_time", "end_date_time"]:
    if c in df.columns:
        df = df.withColumn(c, to_timestamp(col(c)))

# Cast integer-like fields
for c in [
    "begin_yearmonth", "begin_day", "begin_time",
    "end_yearmonth", "end_day", "end_time",
    "injuries_direct", "injuries_indirect",
    "deaths_direct", "deaths_indirect",
]:
    if c in df.columns:
        df = df.withColumn(c, col(c).cast(IntegerType()))

# Cast double-like fields
for c in ["magnitude", "tor_length", "tor_width", "begin_lat", "begin_lon", "end_lat", "end_lon"]:
    if c in df.columns:
        df = df.withColumn(c, col(c).cast(DoubleType()))

# Keep damage fields as string in Silver
for c in ["damage_property", "damage_crops"]:
    if c in df.columns:
        df = df.withColumn(c, col(c).cast(StringType()))

# Build a clean partitionable year column (string)
# Prefer 'year' if present, else 'year_col'
if "year" in df.columns:
    df = df.withColumn("year", regexp_extract(col("year").cast("string"), r"(\d{4})", 1).cast(StringType()))
elif "year_col" in df.columns:
    df = df.withColumn("year", regexp_extract(col("year_col").cast("string"), r"(\d{4})", 1).cast(StringType()))
else:
    raise ValueError("Missing 'year'/'year_col' after normalization.")

# Treat blank strings as null for key fields (common in CSVs)
if "state" in df.columns:
    df = df.withColumn("state", when(trim(col("state")) == "", None).otherwise(col("state")))

if "state_fips" in df.columns:
    df = df.withColumn(
        "state_fips",
        when(trim(col("state_fips").cast("string")) == "", None).otherwise(col("state_fips"))
    )

if "cz_fips" in df.columns:
    df = df.withColumn(
        "cz_fips",
        when(trim(col("cz_fips").cast("string")) == "", None).otherwise(col("cz_fips"))
    )

if "event_type" in df.columns:
    df = df.withColumn("event_type", when(trim(col("event_type")) == "", None).otherwise(col("event_type")))

# Hard filter rows missing required grain keys (more reliable than dropna)
required = ["event_id", "episode_id", "state", "state_fips", "cz_fips", "event_type", "year"]
required = [c for c in required if c in df.columns]  # safety if schema evolves

if required:
    cond = reduce(lambda a, b: a & b, [(col(c).isNotNull()) for c in required])
    df = df.filter(cond)

# Derive county_fips when cz_type == 'C'
df = make_county_fips_from_noaa_state_cz(df)
df = standardize_county_fips(df, "county_fips")

# Deduplicate
df = dedupe(df, ["event_id", "episode_id"])

silver_count = df_rowcount(df)

# Avoid including year in validation collect (not needed, prevents odd pushdowns)
nr = null_rates(df, ["event_id", "episode_id", "county_fips"])
log_validation_summary("noaa_events_clean", bronze_count, silver_count, nr)

(df.write
   .mode("overwrite")
   .partitionBy("year")
   .parquet(out_path))

job.commit()
