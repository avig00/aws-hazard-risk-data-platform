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

from pyspark.sql.functions import col, coalesce, lit, lpad, to_timestamp, regexp_extract, trim, upper, when
from pyspark.sql.types import IntegerType, DoubleType, StringType

from silver_utils import (
    normalize_columns,
    make_county_fips_from_noaa_state_cz,
    make_county_fips_from_state_county_codes,
    normalize_geo_name_column,
    compact_geo_name_column,
    coalesce_columns,
    standardize_county_fips,
    apply_basic_county_fips_sanity,
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
unresolved_path = f"s3://{bucket}/{silver_prefix}/noaa_events_clean_unresolved_county_fips/"
non_county_path = f"s3://{bucket}/{silver_prefix}/noaa_events_clean_non_county_zones/"
nri_ref_path = f"s3://{bucket}/{bronze_prefix}/nri/counties/"


def build_canonical_county_reference():
    """
    Build a canonical county_fips reference from the bronze NRI county file.
    We use this as a stable upstream reference to map NOAA county rows to
    canonical county_fips values.
    """
    ref = normalize_columns(spark.read.option("header", "true").csv(nri_ref_path))

    if "stcofips" in ref.columns:
        ref = ref.withColumn(
            "county_fips",
            when(trim(col("stcofips").cast("string")) == "", None).otherwise(col("stcofips").cast("string")),
        )
    elif "statefips" in ref.columns and "countyfips" in ref.columns:
        ref = make_county_fips_from_state_county_codes(ref, "statefips", "countyfips", "county_fips")
    else:
        raise ValueError("Bronze NRI county file is missing stcofips and state/county fallback fields.")

    if "statefips" in ref.columns:
        ref = ref.withColumn(
            "state_fips_norm",
            when(trim(col("statefips").cast("string")) == "", None).otherwise(lpad(col("statefips").cast("string"), 2, "0")),
        )
    else:
        ref = ref.withColumn("state_fips_norm", col("county_fips").substr(1, 2))

    ref = coalesce_columns(ref, ["county", "county_name", "countyname", "name"], "county_name_source")
    if "county_name_source" not in ref.columns:
        ref = ref.withColumn("county_name_source", lit(None))
    ref = normalize_geo_name_column(ref, "county_name_source", "county_name_norm")
    ref = compact_geo_name_column(ref, "county_name_norm", "county_name_compact")
    ref = standardize_county_fips(ref, "county_fips")
    ref = apply_basic_county_fips_sanity(ref, "county_fips")
    return (
        ref.select("county_fips", "state_fips_norm", "county_name_norm", "county_name_compact")
        .filter(col("county_fips").isNotNull())
        .dropDuplicates(["county_fips"])
    )

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

# Prepare NOAA geography fields
if "cz_type" in df.columns:
    df = df.withColumn("cz_type_norm", when(trim(col("cz_type").cast("string")) == "", None).otherwise(upper(trim(col("cz_type").cast("string")))))
else:
    df = df.withColumn("cz_type_norm", lit(None))

df = df.withColumn(
    "state_fips_norm",
    when(trim(col("state_fips").cast("string")) == "", None)
    .when(lpad(col("state_fips").cast("string"), 2, "0") == lit("99"), lit("72"))
    .otherwise(lpad(col("state_fips").cast("string"), 2, "0")),
)

# Derive county_fips candidate when cz_type == 'C'
df = make_county_fips_from_noaa_state_cz(df)
df = standardize_county_fips(df, "county_fips")
df = df.withColumn(
    "county_fips",
    when(col("state_fips_norm") == lit("72"), col("state_fips_norm") + lpad(col("cz_fips").cast("string"), 3, "0"))
    .otherwise(col("county_fips")),
)
df = df.withColumn("county_fips_raw_candidate", col("county_fips"))
df = normalize_geo_name_column(df, "cz_name", "cz_name_norm")
df = compact_geo_name_column(df, "cz_name_norm", "cz_name_compact")

# Deduplicate
df = dedupe(df, ["event_id", "episode_id"])

canonical_counties = build_canonical_county_reference()
canonical_direct = canonical_counties.select(col("county_fips").alias("__direct_county_fips")).dropDuplicates(["__direct_county_fips"])
canonical_name = (
    canonical_counties.groupBy("state_fips_norm", "county_name_norm")
    .count()
    .filter(col("count") == 1)
    .drop("count")
    .join(canonical_counties, ["state_fips_norm", "county_name_norm"], "inner")
    .select(
        col("state_fips_norm").alias("__name_state_fips_norm"),
        col("county_name_norm").alias("__name_county_name_norm"),
        col("county_fips").alias("__name_county_fips"),
    )
)
canonical_name_compact = (
    canonical_counties.groupBy("state_fips_norm", "county_name_compact")
    .count()
    .filter(col("count") == 1)
    .drop("count")
    .join(canonical_counties, ["state_fips_norm", "county_name_compact"], "inner")
    .select(
        col("state_fips_norm").alias("__compact_state_fips_norm"),
        col("county_name_compact").alias("__compact_county_name"),
        col("county_fips").alias("__compact_county_fips"),
    )
)

df_county_candidates = df.filter(col("cz_type_norm") == lit("C"))
df_non_county = (
    df.filter(col("cz_type_norm").isNull() | (col("cz_type_norm") != lit("C")))
    .withColumn("county_fips_mapping_method", lit("non_county_zone"))
)

df_county_candidates = df_county_candidates.join(
    canonical_direct,
    trim(col("county_fips").cast("string")) == col("__direct_county_fips"),
    "left",
)

df_county_candidates = df_county_candidates.join(
    canonical_name,
    (col("__direct_county_fips").isNull())
    & (col("state_fips_norm") == col("__name_state_fips_norm"))
    & (col("cz_name_norm") == col("__name_county_name_norm")),
    "left",
)

df_county_candidates = df_county_candidates.join(
    canonical_name_compact,
    (col("__direct_county_fips").isNull())
    & (col("__name_county_fips").isNull())
    & (col("state_fips_norm") == col("__compact_state_fips_norm"))
    & (col("cz_name_compact") == col("__compact_county_name")),
    "left",
)

df_county_mapped = (
    df_county_candidates
    .withColumn("county_fips", coalesce(col("__direct_county_fips"), col("__name_county_fips"), col("__compact_county_fips")))
    .withColumn(
        "county_fips_mapping_method",
        when(col("__direct_county_fips").isNotNull(), lit("direct_code"))
        .when(col("__name_county_fips").isNotNull(), lit("name_match"))
        .when(col("__compact_county_fips").isNotNull(), lit("name_match_compact"))
        .otherwise(lit("unresolved")),
    )
)

df_valid = df_county_mapped.filter(col("county_fips").isNotNull())
df_unresolved = df_county_mapped.filter(col("county_fips").isNull())

silver_count = df_rowcount(df_valid)
unresolved_count = df_rowcount(df_unresolved)
non_county_count = df_rowcount(df_non_county)

# Avoid including year in validation collect (not needed, prevents odd pushdowns)
nr = null_rates(df_valid, ["event_id", "episode_id", "county_fips"])
log_validation_summary("noaa_events_clean", bronze_count, silver_count, nr)
print(f"[VALIDATION] dataset=noaa_events_clean unresolved_rows={unresolved_count}")
print(f"[VALIDATION] dataset=noaa_events_clean non_county_rows={non_county_count}")

(df_valid.write
   .mode("overwrite")
   .partitionBy("year")
   .parquet(out_path))

(df_unresolved.write
   .mode("overwrite")
   .partitionBy("year")
   .parquet(unresolved_path))

(df_non_county.write
   .mode("overwrite")
   .partitionBy("year")
   .parquet(non_county_path))

job.commit()
