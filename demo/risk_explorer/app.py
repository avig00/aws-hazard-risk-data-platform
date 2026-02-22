from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import List, Optional

import boto3
import pandas as pd
import streamlit as st


# -----------------------------
# Config
# -----------------------------
@dataclass(frozen=True)
class AppConfig:
    region: str
    workgroup: str
    database: str
    output_s3: str  # e.g. s3://aws-hazard-risk-vigamogh-dev/hazard/athena/results/
    max_rows: int


def get_cfg() -> AppConfig:
    # Prefer explicit env vars, fallback to your existing project conventions.
    region = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    workgroup = os.getenv("ATHENA_WORKGROUP", "athena-gold")
    database = os.getenv("ATHENA_DB_GOLD", "gold_hazard")
    output_s3 = os.getenv("ATHENA_RESULTS_S3", "")

    if not output_s3.startswith("s3://"):
        raise ValueError(
            "Missing/invalid ATHENA_RESULTS_S3 env var. "
            "Set it to something like: s3://<bucket>/hazard/athena/results/"
        )

    return AppConfig(
        region=region,
        workgroup=workgroup,
        database=database,
        output_s3=output_s3,
        max_rows=int(os.getenv("RISK_EXPLORER_MAX_ROWS", "2000")),
    )


# -----------------------------
# Athena helpers (boto3)
# -----------------------------
def athena_client(region: str):
    return boto3.client("athena", region_name=region)


def run_athena_query(
    client,
    *,
    sql: str,
    database: str,
    workgroup: str,
    output_s3: str,
    poll_seconds: float = 1.0,
    timeout_seconds: int = 90,
) -> str:
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_s3},
        WorkGroup=workgroup,
    )
    qid = resp["QueryExecutionId"]

    start = time.time()
    while True:
        s = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = s["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() - start > timeout_seconds:
            client.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f"Athena query timed out after {timeout_seconds}s: {qid}")
        time.sleep(poll_seconds)

    if state != "SUCCEEDED":
        reason = s.get("StateChangeReason", "Unknown")
        raise RuntimeError(f"Athena query failed: state={state} reason={reason}\nSQL:\n{sql}")

    return qid


def fetch_athena_results(client, qid: str, max_rows: int = 2000) -> pd.DataFrame:
    paginator = client.get_paginator("get_query_results")

    rows = []
    colnames: Optional[List[str]] = None
    fetched = 0

    for page in paginator.paginate(QueryExecutionId=qid):
        page_rows = page["ResultSet"]["Rows"]
        # First page: header row
        if colnames is None:
            colnames = [c.get("VarCharValue", "") for c in page_rows[0]["Data"]]
            data_rows = page_rows[1:]
        else:
            data_rows = page_rows

        for r in data_rows:
            rows.append([d.get("VarCharValue", None) for d in r["Data"]])
            fetched += 1
            if fetched >= max_rows:
                break
        if fetched >= max_rows:
            break

    if not colnames:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=colnames)
    return df


# -----------------------------
# SQL builders 
# -----------------------------
def sql_years() -> str:
    return """
    SELECT CAST(year AS BIGINT) AS year
    FROM gold_hazard.risk_feature_mart_current
    GROUP BY 1
    ORDER BY 1
    """


def sql_top_claims_per_event(year: int, limit: int = 25) -> str:
    return f"""
    SELECT
      county_fips,
      CAST(year AS BIGINT) AS year,
      CAST(fema_valid_registrations AS DOUBLE) AS fema_valid_registrations,
      CAST(noaa_event_count AS BIGINT) AS noaa_event_count,
      CAST(fema_valid_registrations AS DOUBLE) / NULLIF(CAST(noaa_event_count AS DOUBLE), 0.0) AS claims_per_event
    FROM gold_hazard.risk_feature_mart_current
    WHERE CAST(year AS BIGINT) = {int(year)}
    ORDER BY claims_per_event DESC NULLS LAST
    LIMIT {int(limit)}
    """


def sql_county_timeseries(county_fips: str) -> str:
    county_fips = county_fips.strip()
    return f"""
    SELECT
      CAST(year AS BIGINT) AS year,
      CAST(noaa_event_count AS BIGINT) AS noaa_event_count,
      CAST(fema_valid_registrations AS DOUBLE) AS fema_valid_registrations,
      CAST(fema_total_damage AS DOUBLE) AS fema_total_damage,
      CAST(nri_risk_score AS DOUBLE) AS nri_risk_score
    FROM gold_hazard.risk_feature_mart_current
    WHERE county_fips = '{county_fips}'
    ORDER BY year
    """


def sql_hazard_breakdown(county_fips: str, year: int, limit: int = 50) -> str:
    county_fips = county_fips.strip()
    return f"""
    SELECT
      hazard_type,
      hazard_category,
      CAST(event_count AS BIGINT) AS event_count,
      CAST(total_fatalities AS BIGINT) AS total_fatalities,
      CAST(total_injuries AS BIGINT) AS total_injuries,
      CAST(avg_property_damage AS DOUBLE) AS avg_property_damage
    FROM gold_hazard.hazard_event_summary_current
    WHERE county_fips = '{county_fips}'
      AND CAST(year AS BIGINT) = {int(year)}
    ORDER BY event_count DESC
    LIMIT {int(limit)}
    """


def sql_ranked_risk(year: int, limit: int = 1000) -> str:
    return f"""
    SELECT
      county_fips,
      CAST(year AS BIGINT) AS year,
      CAST(nri_risk_score AS DOUBLE) AS nri_risk_score,
      CAST(noaa_event_count AS BIGINT) AS noaa_event_count,
      CAST(fema_total_damage AS DOUBLE) AS fema_total_damage
    FROM gold_hazard.risk_feature_mart_current
    WHERE CAST(year AS BIGINT) = {int(year)}
    ORDER BY nri_risk_score DESC NULLS LAST
    LIMIT {int(limit)}
    """


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Risk Explorer (Gold)", layout="wide")

st.title("Risk Explorer")
st.caption(
    "Lightweight demo over validated, stable `_current` views in Athena. "
)

cfg = get_cfg()
client = athena_client(cfg.region)

with st.sidebar:
    st.header("Connection")
    st.text(f"Region: {cfg.region}")
    st.text(f"Workgroup: {cfg.workgroup}")
    st.text(f"Database: {cfg.database}")
    st.text(f"Results: {cfg.output_s3}")

    st.divider()
    st.header("Filters")

    # Load available years (cached)
    @st.cache_data(ttl=600)
    def load_years() -> List[int]:
        qid = run_athena_query(
            client,
            sql=sql_years(),
            database=cfg.database,
            workgroup=cfg.workgroup,
            output_s3=cfg.output_s3,
        )
        df = fetch_athena_results(client, qid, max_rows=5000)
        years = sorted({int(y) for y in df["year"].dropna().tolist()})
        return years

    years = load_years()
    year = st.selectbox("Year", years, index=len(years) - 1)

    county_fips = st.text_input("County FIPS (5 digits)", value="06037")  # LA County default

    st.divider()
    st.header("Actions")
    run_btn = st.button("Run Explorer", type="primary")
    st.write("Tip: start with Alaska (02xxx) to sanity-check extremes.")

if not run_btn:
    st.info("Set filters on the left and click **Run Explorer**.")
    st.stop()


# -----------------------------
# Run Queries
# -----------------------------
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("Top claim intensity (claims per event)")
    qid = run_athena_query(
        client,
        sql=sql_top_claims_per_event(year=year, limit=25),
        database=cfg.database,
        workgroup=cfg.workgroup,
        output_s3=cfg.output_s3,
    )
    df_top = fetch_athena_results(client, qid, max_rows=cfg.max_rows)

    # Basic numeric conversion
    for c in ["fema_valid_registrations", "noaa_event_count", "claims_per_event"]:
        if c in df_top.columns:
            df_top[c] = pd.to_numeric(df_top[c], errors="coerce")

    st.dataframe(df_top, use_container_width=True)

with col2:
    st.subheader("County time series")
    qid = run_athena_query(
        client,
        sql=sql_county_timeseries(county_fips=county_fips),
        database=cfg.database,
        workgroup=cfg.workgroup,
        output_s3=cfg.output_s3,
    )
    df_ts = fetch_athena_results(client, qid, max_rows=cfg.max_rows)

    for c in ["year", "noaa_event_count", "fema_valid_registrations", "fema_total_damage", "nri_risk_score"]:
        if c in df_ts.columns:
            df_ts[c] = pd.to_numeric(df_ts[c], errors="coerce")

    st.line_chart(df_ts.set_index("year")[["noaa_event_count", "fema_valid_registrations"]])

    st.write("Damage + NRI")
    st.line_chart(df_ts.set_index("year")[["fema_total_damage", "nri_risk_score"]])

st.divider()

c3, c4 = st.columns([1, 1])

with c3:
    st.subheader("Hazard breakdown for selected county/year")
    qid = run_athena_query(
        client,
        sql=sql_hazard_breakdown(county_fips=county_fips, year=year),
        database=cfg.database,
        workgroup=cfg.workgroup,
        output_s3=cfg.output_s3,
    )
    df_h = fetch_athena_results(client, qid, max_rows=cfg.max_rows)
    st.dataframe(df_h, use_container_width=True)

with c4:
    st.subheader("Map-ready export (top risk by county)")
    qid = run_athena_query(
        client,
        sql=sql_ranked_risk(year=year, limit=1000),
        database=cfg.database,
        workgroup=cfg.workgroup,
        output_s3=cfg.output_s3,
    )
    df_r = fetch_athena_results(client, qid, max_rows=cfg.max_rows)
    for c in ["year", "nri_risk_score", "noaa_event_count", "fema_total_damage"]:
        if c in df_r.columns:
            df_r[c] = pd.to_numeric(df_r[c], errors="coerce")

    st.dataframe(df_r.head(50), use_container_width=True)

    csv = df_r.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV (for mapping)",
        data=csv,
        file_name=f"risk_ranked_{year}.csv",
        mime="text/csv",
    )

st.success("Done. This demo reads from Gold `_current` views and stays stable across rebuilds.")