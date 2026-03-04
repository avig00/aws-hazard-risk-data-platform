"""
silver_utils.py

Purpose:
    Shared, reusable utilities for Phase 3 (Silver layer) Glue Spark jobs.

What it provides:
    - snake_case normalization for column names
    - county_fips construction + standardization (zero-padded 5-char)
    - deduplication helpers
    - surrogate key helper (sha2 over selected columns)
    - lightweight validation helpers:
        - row counts
        - null-rate computation for key fields
        - standardized console logging

Design notes:
    - Keep Silver jobs small and declarative by moving common logic here.
    - Avoid any job-specific assumptions; keep it generic and reusable.
"""

import re
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    lpad,
    length,
    regexp_replace,
    when,
    count,
    sum as fsum,
    sha2,
    concat_ws,
    trim,
    upper,
    lit,
    to_date,
)


# -------------------------
# Naming
# -------------------------
def snake_case(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"__+", "_", name)
    return name.lower().strip("_")


def normalize_columns(df: DataFrame) -> DataFrame:
    for c in df.columns:
        df = df.withColumnRenamed(c, snake_case(c))
    return df


# -------------------------
# FIPS helpers
# -------------------------
def standardize_county_fips(df: DataFrame, colname: str = "county_fips") -> DataFrame:
    """
    Ensure county_fips is digits only and zero-padded to 5.
    """
    if colname not in df.columns:
        return df

    cleaned = lpad(regexp_replace(col(colname).cast("string"), r"[^0-9]", ""), 5, "0")
    return df.withColumn(
        colname,
        when(col(colname).isNull(), lit(None)).otherwise(cleaned),
    )


def apply_basic_county_fips_sanity(df: DataFrame, colname: str = "county_fips") -> DataFrame:
    """
    Apply baseline county_fips sanity checks.

    This is not a full canonical validation, but it removes obvious bad keys:
      - not exactly 5 digits after standardization
      - invalid state placeholders (00, 03)
      - county code 000
    """
    if colname not in df.columns:
        return df

    cleaned = trim(col(colname).cast("string"))
    return df.filter(
        col(colname).isNotNull()
        & (length(cleaned) == 5)
        & (~cleaned.startswith("00"))
        & (~cleaned.startswith("03"))
        & (cleaned.substr(3, 3) != lit("000"))
    )


def keep_only_canonical_counties(
    df: DataFrame,
    canonical_counties_df: DataFrame,
    colname: str = "county_fips",
) -> DataFrame:
    """
    Keep only rows whose county_fips exists in a canonical reference DataFrame.
    The reference must contain a `county_fips` column.
    """
    if colname not in df.columns or "county_fips" not in canonical_counties_df.columns:
        return df

    ref = canonical_counties_df.select(
        trim(col("county_fips").cast("string")).alias("__valid_county_fips")
    ).dropDuplicates(["__valid_county_fips"])

    return df.join(ref, trim(col(colname).cast("string")) == col("__valid_county_fips"), "left_semi")


def normalize_geo_name_column(
    df: DataFrame,
    source_col: str,
    out_col: str,
) -> DataFrame:
    """
    Normalize geography labels for deterministic joins.

    This is intentionally conservative:
      - uppercase
      - drop trailing state text after commas
      - strip NOAA suffix markers like "(C)"
      - replace punctuation with spaces
      - remove common county/jurisdiction suffixes
      - collapse repeated whitespace
    """
    if source_col not in df.columns:
        return df

    cleaned = upper(trim(col(source_col).cast("string")))
    cleaned = regexp_replace(cleaned, r",.*$", "")
    cleaned = regexp_replace(cleaned, r"&", " AND ")
    cleaned = regexp_replace(cleaned, r"\([A-Z]\)", " ")
    cleaned = regexp_replace(cleaned, r"\b(COUNTY|PARISH|BOROUGH|CENSUS AREA|CITY AND BOROUGH|MUNICIPALITY|MUNICIPIO)\b", "")
    cleaned = regexp_replace(cleaned, r"\bCITY\b", " ")
    cleaned = regexp_replace(cleaned, r"[^A-Z0-9]+", " ")
    cleaned = regexp_replace(cleaned, r"\s+", " ")

    return df.withColumn(
        out_col,
        when(trim(col(source_col).cast("string")) == "", lit(None)).otherwise(trim(cleaned)),
    )


def compact_geo_name_column(
    df: DataFrame,
    source_col: str,
    out_col: str,
) -> DataFrame:
    """
    Build a compact geography key by removing spaces from a normalized name.
    Useful for variants like "DE KALB" vs "DEKALB".
    """
    if source_col not in df.columns:
        return df

    compacted = regexp_replace(trim(col(source_col).cast("string")), r"\s+", "")
    return df.withColumn(
        out_col,
        when(trim(col(source_col).cast("string")) == "", lit(None)).otherwise(compacted),
    )


def coalesce_columns(df: DataFrame, source_cols: List[str], out_col: str) -> DataFrame:
    """
    Coalesce the first non-null source column into out_col.
    Missing source columns are ignored.
    """
    usable = [col(c) for c in source_cols if c in df.columns]
    if not usable:
        return df
    return df.withColumn(out_col, coalesce(*usable))


def make_county_fips_from_state_county_codes(
    df: DataFrame,
    state_code_col: str,
    county_code_col: str,
    out_col: str = "county_fips",
) -> DataFrame:
    """
    county_fips = state_fips(2) + county_fips(3)
    """
    if state_code_col not in df.columns or county_code_col not in df.columns:
        return df

    st = lpad(col(state_code_col).cast("string"), 2, "0")
    co = lpad(col(county_code_col).cast("string"), 3, "0")

    return df.withColumn(
        out_col,
        when(col(state_code_col).isNull() | col(county_code_col).isNull(), lit(None)).otherwise(st + co),
    )


def make_county_fips_from_noaa_state_cz(
    df: DataFrame,
    state_fips_col: str = "state_fips",
    cz_fips_col: str = "cz_fips",
    cz_type_col: str = "cz_type",
    out_col: str = "county_fips",
) -> DataFrame:
    """
    NOAA:
      - cz_type == 'C' => cz_fips is county code (3 digits)
      - state_fips is 2 digits
      - else (zone) => county_fips should be NULL
    """
    needed = {state_fips_col, cz_fips_col, cz_type_col}
    if not needed.issubset(set(df.columns)):
        return df

    st = lpad(col(state_fips_col).cast("string"), 2, "0")
    cz = lpad(col(cz_fips_col).cast("string"), 3, "0")
    is_county = upper(trim(col(cz_type_col))) == lit("C")

    return df.withColumn(
        out_col,
        when(is_county & col(state_fips_col).isNotNull() & col(cz_fips_col).isNotNull(), st + cz).otherwise(lit(None)),
    )


# -------------------------
# Dedupe + surrogate keys
# -------------------------
def dedupe(df: DataFrame, keys: List[str]) -> DataFrame:
    keys = [k for k in keys if k in df.columns]
    if not keys:
        return df
    return df.dropDuplicates(keys)


def add_surrogate_key(df: DataFrame, cols: List[str], out_col: str) -> DataFrame:
    cols = [c for c in cols if c in df.columns]
    if not cols:
        return df
    return df.withColumn(out_col, sha2(concat_ws("||", *[col(c).cast("string") for c in cols]), 256))


# -------------------------
# Validations
# -------------------------
def df_rowcount(df: DataFrame) -> int:
    return df.count()


def null_rates(df: DataFrame, cols: List[str]) -> DataFrame:
    """
    Robust null-rate computation:
      - counts NULLs
      - counts empty strings (after trim) as null-equivalent
    Avoids isnan() because many columns are strings in Silver.
    """
    cols = [c for c in cols if c in df.columns]
    exprs = []
    for c in cols:
        null_or_empty = col(c).isNull() | (trim(col(c).cast("string")) == lit(""))
        exprs.append((fsum(when(null_or_empty, 1).otherwise(0)) / count("*")).alias(f"{c}_null_rate"))
    return df.select(exprs) if exprs else df.sql_ctx.createDataFrame([("no_cols",)], ["note"])


def log_validation_summary(dataset: str, bronze_count: int, silver_count: int, null_rate_df: DataFrame) -> None:
    print(f"[VALIDATION] dataset={dataset} bronze_rows={bronze_count} silver_rows={silver_count}")
    rows = null_rate_df.collect()
    if rows:
        d = rows[0].asDict()
        for k, v in d.items():
            print(f"[VALIDATION] dataset={dataset} {k}={v}")


def safe_to_date(df: DataFrame, cols: List[str]) -> DataFrame:
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, to_date(col(c)))
    return df
