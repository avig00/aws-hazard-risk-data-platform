"""
Glue Job: 06_silver_zip2county_clean

Expected args:
--PLATFORM_S3_BUCKET
--BRONZE_PREFIX
--SILVER_PREFIX
--OPS_PREFIX
--RUN_DT
"""

from __future__ import annotations

import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["PLATFORM_S3_BUCKET", "BRONZE_PREFIX", "SILVER_PREFIX", "OPS_PREFIX", "RUN_DT"],
    )

    bucket = args["PLATFORM_S3_BUCKET"]
    bronze_prefix = args["BRONZE_PREFIX"]
    silver_prefix = args["SILVER_PREFIX"]

    spark = SparkSession.builder.getOrCreate()

    bronze_path = f"s3://{bucket}/{bronze_prefix}/reference/zip2county_master_xwalk/"
    silver_path = f"s3://{bucket}/{silver_prefix}/zip2county_xwalk_clean/"

    df = spark.read.parquet(bronze_path) if bronze_path.endswith(".parquet") else spark.read.csv(
        bronze_path, header=True, inferSchema=True
    )

    # Clean + normalize
    df_clean = (
        df
        .filter(F.col("top_match") == True)
        .withColumn("zip5", F.lpad(F.col("zip").cast("string"), 5, "0"))
        .withColumn("county_fips", F.lpad(F.col("county").cast("string"), 5, "0"))
        .withColumn("year", F.col("year").cast("long"))
        .withColumn("tot_ratio", F.col("tot_ratio").cast("double"))
        .filter(F.length(F.col("county_fips")) == 5)
        .filter(~F.col("county_fips").startswith("00"))
        .filter(~F.col("county_fips").startswith("03"))
        .filter(~F.col("county_fips").endswith("000"))
        .select("zip5", "county_fips", "year", "tot_ratio")
    )

    (
        df_clean
        .write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(silver_path)
    )


if __name__ == "__main__":
    main()
