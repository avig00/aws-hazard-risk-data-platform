from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

import altair as alt
import boto3
import pandas as pd
import pydeck as pdk
import streamlit as st

# Load local .env for development (Streamlit Community Cloud ignores this unless a .env exists)
load_dotenv()

# =============================================================================
# Config
# =============================================================================
@dataclass(frozen=True)
class AppConfig:
    region: str
    workgroup: str
    database: str
    output_s3: str  # must be trailing slash
    max_rows: int
    map_default_top_n: int
    map_max_points_cap: int


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def _normalize_s3_uri(uri: str) -> str:
    uri = (uri or "").strip()
    if not uri:
        return ""
    if not uri.startswith("s3://"):
        return uri
    return uri if uri.endswith("/") else uri + "/"


def get_cfg() -> AppConfig:
    region = _get_env("AWS_REGION", _get_env("AWS_DEFAULT_REGION", "us-east-1")) or "us-east-1"
    workgroup = _get_env("ATHENA_WORKGROUP", "athena-gold") or "athena-gold"
    database = _get_env("ATHENA_DB_GOLD", "gold_hazard") or "gold_hazard"

    # Prefer explicit ATHENA_RESULTS_S3, otherwise try to derive from WorkGroup OutputLocation.
    output_s3 = _normalize_s3_uri(_get_env("ATHENA_RESULTS_S3", ""))

    if not output_s3.startswith("s3://"):
        try:
            client = boto3.client("athena", region_name=region)
            wg = client.get_work_group(WorkGroup=workgroup)["WorkGroup"]
            wg_out = wg.get("Configuration", {}).get("ResultConfiguration", {}).get("OutputLocation", "")
            wg_out = _normalize_s3_uri(wg_out)
            if wg_out.startswith("s3://"):
                output_s3 = wg_out
        except Exception:
            # We'll surface a friendly error in the UI if still missing.
            pass

    return AppConfig(
        region=region,
        workgroup=workgroup,
        database=database,
        output_s3=output_s3,
        max_rows=int(_get_env("RISK_EXPLORER_MAX_ROWS", "2000") or "2000"),
        map_default_top_n=int(_get_env("RISK_EXPLORER_MAP_TOP_N", "600") or "600"),
        map_max_points_cap=int(_get_env("RISK_EXPLORER_MAP_MAX_POINTS_CAP", "1500") or "1500"),
    )


# =============================================================================
# Athena helpers (boto3)
# =============================================================================
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
    timeout_seconds: int = 120,
) -> str:
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_s3},
        WorkGroup=workgroup,
    )
    qid = resp["QueryExecutionId"]

    start = time.time()
    state = "RUNNING"
    status_obj: Dict = {}
    while True:
        status_obj = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = status_obj["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() - start > timeout_seconds:
            client.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f"Athena query timed out after {timeout_seconds}s: {qid}")
        time.sleep(poll_seconds)

    if state != "SUCCEEDED":
        reason = status_obj.get("StateChangeReason", "Unknown")
        raise RuntimeError(f"Athena query failed: state={state} reason={reason}\nSQL:\n{sql}")

    return qid


def get_query_stats(client, qid: str) -> Tuple[int, int]:
    """
    Returns: (data_scanned_bytes, engine_execution_ms)
    """
    qe = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
    stats = qe.get("Statistics", {})
    scanned = int(stats.get("DataScannedInBytes", 0) or 0)
    exec_ms = int(stats.get("EngineExecutionTimeInMillis", 0) or 0)
    return scanned, exec_ms


def fetch_athena_results(client, qid: str, max_rows: int = 2000) -> pd.DataFrame:
    paginator = client.get_paginator("get_query_results")

    rows: List[List[Optional[str]]] = []
    colnames: Optional[List[str]] = None
    fetched = 0

    for page in paginator.paginate(QueryExecutionId=qid):
        page_rows = page.get("ResultSet", {}).get("Rows", []) or []
        if not page_rows:
            continue

        # First page contains header row
        if colnames is None:
            header = page_rows[0].get("Data", []) or []
            colnames = [c.get("VarCharValue", "") for c in header]
            data_rows = page_rows[1:]
        else:
            data_rows = page_rows

        for r in data_rows:
            data = r.get("Data", []) or []
            rows.append([d.get("VarCharValue", None) for d in data])
            fetched += 1
            if fetched >= max_rows:
                break

        if fetched >= max_rows:
            break

    if not colnames:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=colnames)


# =============================================================================
# SQL builders
# =============================================================================
def sql_years() -> str:
    return """
    SELECT CAST(year AS BIGINT) AS year
    FROM gold_hazard.risk_feature_mart_current
    GROUP BY 1
    ORDER BY 1
    """


def sql_health_check_tables(year: int) -> str:
    y = int(year)
    return f"""
    SELECT
      '{y}' AS selected_year,
      (SELECT COUNT(*) FROM gold_hazard.risk_feature_mart_current WHERE CAST(year AS BIGINT) = {y}) AS risk_rows_year,
      (SELECT COUNT(*) FROM gold_hazard.county_centroids_current) AS centroid_rows,
      (SELECT COUNT(*) FROM gold_hazard.hazard_event_summary_current WHERE CAST(year AS BIGINT) = {y}) AS noaa_rows_year
    """


def sql_year_kpis(year: int) -> str:
    y = int(year)
    return f"""
    WITH base AS (
      SELECT
        LPAD(CAST(county_fips AS VARCHAR), 5, '0') AS county_fips,
        CAST(year AS BIGINT) AS year,
        CAST(nri_risk_score AS DOUBLE) AS nri_risk_score,
        CAST(noaa_event_count AS DOUBLE) AS noaa_event_count
      FROM gold_hazard.risk_feature_mart_current
      WHERE CAST(year AS BIGINT) = {y}
    )
    SELECT
      CAST(100.0 * AVG(CASE WHEN COALESCE(noaa_event_count, 0.0) = 0.0 THEN 1.0 ELSE 0.0 END) AS DOUBLE) AS pct_noaa_zero,
      CAST(APPROX_PERCENTILE(CASE WHEN COALESCE(noaa_event_count, 0.0) = 0.0 THEN nri_risk_score END, 0.5) AS DOUBLE) AS median_nri_noaa_zero,
      CAST(APPROX_PERCENTILE(CASE WHEN COALESCE(noaa_event_count, 0.0) > 0.0 THEN nri_risk_score END, 0.5) AS DOUBLE) AS median_nri_noaa_pos
    FROM base
    """


def sql_noaa0_high_nri(year: int, limit: int = 50) -> str:
    y = int(year)
    lim = int(limit)
    return f"""
    SELECT
      county_fips,
      CAST(year AS BIGINT) AS year,
      CAST(nri_risk_score AS DOUBLE) AS nri_risk_score,
      CAST(noaa_event_count AS BIGINT) AS noaa_event_count,
      CAST(fema_valid_registrations AS DOUBLE) AS fema_valid_registrations,
      CAST(fema_total_damage AS DOUBLE) AS fema_total_damage
    FROM gold_hazard.risk_feature_mart_current
    WHERE CAST(year AS BIGINT) = {y}
      AND COALESCE(CAST(noaa_event_count AS BIGINT), 0) = 0
    ORDER BY nri_risk_score DESC NULLS LAST, fema_valid_registrations DESC NULLS LAST
    LIMIT {lim}
    """


def sql_county_lookup(county_fips: str) -> str:
    cf = normalize_county_fips(county_fips)
    return f"""
    SELECT
      LPAD(CAST(county_fips AS VARCHAR), 5, '0') AS county_fips,
      county_name,
      state,
      CAST(lat AS DOUBLE) AS lat,
      CAST(lon AS DOUBLE) AS lon
    FROM gold_hazard.county_centroids_current
    WHERE LPAD(CAST(county_fips AS VARCHAR), 5, '0') = '{cf}'
    LIMIT 1
    """


def sql_top_claims_per_event(year: int, limit: int = 25) -> str:
    return f"""
    SELECT
      county_fips,
      CAST(year AS BIGINT) AS year,
      CAST(fema_valid_registrations AS DOUBLE) AS fema_valid_registrations,
      CAST(noaa_event_count AS BIGINT) AS noaa_event_count,
      ROUND(CAST(fema_valid_registrations AS DOUBLE) / NULLIF(CAST(noaa_event_count AS DOUBLE), 0.0), 2) AS claims_per_event
    FROM gold_hazard.risk_feature_mart_current
    WHERE CAST(year AS BIGINT) = {int(year)}
    ORDER BY claims_per_event DESC NULLS LAST
    LIMIT {int(limit)}
    """


def sql_hazard_breakdown(county_fips: str, year: int, limit: int = 50) -> str:
    cf = normalize_county_fips(county_fips)
    return f"""
    SELECT
      hazard_type,
      CAST(event_count AS BIGINT) AS event_count,
      CAST(total_fatalities AS BIGINT) AS total_fatalities,
      CAST(total_injuries AS BIGINT) AS total_injuries,
      CAST(avg_property_damage AS DOUBLE) AS avg_property_damage
    FROM gold_hazard.hazard_event_summary_current
    WHERE LPAD(CAST(county_fips AS VARCHAR), 5, '0') = '{cf}'
      AND CAST(year AS BIGINT) = {int(year)}
    ORDER BY event_count DESC
    LIMIT {int(limit)}
    """


def sql_ranked_risk_structural(year: int, limit: int = 1000) -> str:
    return f"""
    SELECT
      county_fips,
      CAST(year AS BIGINT) AS year,
      CAST(nri_risk_score AS DOUBLE) AS nri_risk_score,
      CAST(noaa_event_count AS BIGINT) AS noaa_event_count,
      CAST(fema_valid_registrations AS DOUBLE) AS fema_valid_registrations,
      CAST(fema_total_damage AS DOUBLE) AS fema_total_damage
    FROM gold_hazard.risk_feature_mart_current
    WHERE CAST(year AS BIGINT) = {int(year)}
    ORDER BY nri_risk_score DESC NULLS LAST
    LIMIT {int(limit)}
    """


def sql_ranked_risk_realized_frequency(year: int, limit: int = 1000) -> str:
    return f"""
    SELECT
      county_fips,
      CAST(year AS BIGINT) AS year,
      CAST(nri_risk_score AS DOUBLE) AS nri_risk_score,
      CAST(noaa_event_count AS BIGINT) AS noaa_event_count,
      CAST(fema_valid_registrations AS DOUBLE) AS fema_valid_registrations,
      CAST(fema_total_damage AS DOUBLE) AS fema_total_damage
    FROM gold_hazard.risk_feature_mart_current
    WHERE CAST(year AS BIGINT) = {int(year)}
    ORDER BY noaa_event_count DESC NULLS LAST, fema_valid_registrations DESC NULLS LAST
    LIMIT {int(limit)}
    """


def sql_ranked_risk_realized_impact(year: int, limit: int = 1000) -> str:
    return f"""
    SELECT
      county_fips,
      CAST(year AS BIGINT) AS year,
      CAST(nri_risk_score AS DOUBLE) AS nri_risk_score,
      CAST(noaa_event_count AS BIGINT) AS noaa_event_count,
      CAST(fema_valid_registrations AS DOUBLE) AS fema_valid_registrations,
      CAST(fema_total_damage AS DOUBLE) AS fema_total_damage
    FROM gold_hazard.risk_feature_mart_current
    WHERE CAST(year AS BIGINT) = {int(year)}
    ORDER BY fema_total_damage DESC NULLS LAST, fema_valid_registrations DESC NULLS LAST
    LIMIT {int(limit)}
    """


def sql_ranked_risk_realized_claims(year: int, limit: int = 1000) -> str:
    return f"""
    SELECT
      county_fips,
      CAST(year AS BIGINT) AS year,
      CAST(nri_risk_score AS DOUBLE) AS nri_risk_score,
      CAST(noaa_event_count AS BIGINT) AS noaa_event_count,
      CAST(fema_valid_registrations AS DOUBLE) AS fema_valid_registrations,
      CAST(fema_total_damage AS DOUBLE) AS fema_total_damage
    FROM gold_hazard.risk_feature_mart_current
    WHERE CAST(year AS BIGINT) = {int(year)}
    ORDER BY fema_valid_registrations DESC NULLS LAST, fema_total_damage DESC NULLS LAST
    LIMIT {int(limit)}
    """


def sql_county_timeseries_with_rollups(county_fips: str, window_years: int = 5) -> str:
    cf = normalize_county_fips(county_fips)
    w = int(window_years)
    return f"""
    WITH base AS (
      SELECT
        CAST(year AS BIGINT) AS year,
        CAST(noaa_event_count AS DOUBLE) AS noaa_event_count,
        CAST(fema_valid_registrations AS DOUBLE) AS fema_valid_registrations,
        CAST(fema_total_damage AS DOUBLE) AS fema_total_damage,
        CAST(nri_risk_score AS DOUBLE) AS nri_risk_score
      FROM gold_hazard.risk_feature_mart_current
      WHERE LPAD(CAST(county_fips AS VARCHAR), 5, '0') = '{cf}'
    )
    SELECT
      year,
      noaa_event_count,
      fema_valid_registrations,
      fema_total_damage,
      nri_risk_score,
      AVG(noaa_event_count) OVER (ORDER BY year ROWS BETWEEN {w-1} PRECEDING AND CURRENT ROW) AS noaa_events_roll{w},
      AVG(fema_valid_registrations) OVER (ORDER BY year ROWS BETWEEN {w-1} PRECEDING AND CURRENT ROW) AS fema_regs_roll{w},
      AVG(fema_total_damage) OVER (ORDER BY year ROWS BETWEEN {w-1} PRECEDING AND CURRENT ROW) AS fema_damage_roll{w}
    FROM base
    ORDER BY year
    """


def sql_ranked_risk_with_centroids(inner_sql: str) -> str:
    return f"""
    SELECT
      r.*,
      CAST(c.lat AS DOUBLE) AS lat,
      CAST(c.lon AS DOUBLE) AS lon,
      c.county_name,
      c.state
    FROM (
      {inner_sql}
    ) r
    LEFT JOIN gold_hazard.county_centroids_current c
      ON LPAD(CAST(r.county_fips AS VARCHAR), 5, '0') = LPAD(CAST(c.county_fips AS VARCHAR), 5, '0')
    """


# =============================================================================
# UI helpers
# =============================================================================
def filter_badges(*labels: str) -> None:
    cols = st.columns(len(labels))
    for c, lab in zip(cols, labels):
        c.caption(lab)


def normalize_county_fips(x: str) -> str:
    x = (x or "").strip()
    if not x:
        return ""
    x = re.sub(r"\D+", "", x)
    if len(x) <= 5:
        x = x.zfill(5)
    return x


def is_valid_fips(x: str) -> bool:
    return bool(re.fullmatch(r"\d{5}", x or ""))


def coerce_year_int(df: pd.DataFrame, col: str = "year") -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def fmt_year_axis() -> alt.Axis:
    return alt.Axis(format="d", title="Year")


def line_chart(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    y_title: str,
    height: int = 280,
) -> alt.Chart:
    base = alt.Chart(df).mark_line(point=True).encode(
        x=alt.X(f"{x}:Q", axis=fmt_year_axis()),
        y=alt.Y(f"{y}:Q", axis=alt.Axis(title=y_title)),
        tooltip=[alt.Tooltip(f"{x}:Q", format="d"), alt.Tooltip(f"{y}:Q")],
    )
    return base.properties(title=title, height=height).interactive()


def dataframe_year_config():
    return {"year": st.column_config.NumberColumn("year", format="%d")}


def human_money(x: float) -> str:
    if x is None or pd.isna(x):
        return "—"
    x = float(x)
    if abs(x) >= 1e9:
        return f"${x/1e9:.2f}B"
    if abs(x) >= 1e6:
        return f"${x/1e6:.2f}M"
    if abs(x) >= 1e3:
        return f"${x/1e3:.2f}K"
    return f"${x:.0f}"


def safe_float(x) -> Optional[float]:
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def format_scan_runtime(scanned_bytes: int, exec_ms: int) -> str:
    mb = scanned_bytes / (1024 * 1024)
    return f"Athena scanned: {mb:.2f} MB • Engine runtime: {exec_ms/1000:.2f}s"


def utc_now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


# =============================================================================
# Cached query runner + cached years
# =============================================================================
@st.cache_data(ttl=300, show_spinner=False)
def run_query_cached(
    *,
    region: str,
    database: str,
    workgroup: str,
    output_s3: str,
    sql: str,
    max_rows: int,
    timeout_seconds: int,
) -> Tuple[pd.DataFrame, int, int]:
    client = athena_client(region)
    qid = run_athena_query(
        client,
        sql=sql,
        database=database,
        workgroup=workgroup,
        output_s3=output_s3,
        timeout_seconds=timeout_seconds,
    )
    scanned, exec_ms = get_query_stats(client, qid)
    df = fetch_athena_results(client, qid, max_rows=max_rows)
    return df, scanned, exec_ms


@st.cache_data(ttl=600, show_spinner=False)
def load_years_cached(region: str, database: str, workgroup: str, output_s3: str, timeout_seconds: int) -> List[int]:
    df, _, _ = run_query_cached(
        region=region,
        database=database,
        workgroup=workgroup,
        output_s3=output_s3,
        sql=sql_years(),
        max_rows=5000,
        timeout_seconds=timeout_seconds,
    )
    if "year" not in df.columns:
        return []
    df = coerce_year_int(df, "year")
    return sorted({int(y) for y in df["year"].dropna().tolist()})


# =============================================================================
# App
# =============================================================================
st.set_page_config(page_title="Risk Explorer", layout="wide")

st.title("Risk Explorer")
st.caption("Lightweight demo over validated, stable Gold `_current` views in Athena.")

cfg = get_cfg()

# Friendly config validation (prevents hard crash on Streamlit Cloud)
missing = []
if not cfg.output_s3.startswith("s3://"):
    missing.append("ATHENA_RESULTS_S3 (or Athena WorkGroup OutputLocation)")
if not cfg.workgroup:
    missing.append("ATHENA_WORKGROUP")
if not cfg.database:
    missing.append("ATHENA_DB_GOLD")

if missing:
    st.error(
        "Missing required configuration:\n\n"
        + "\n".join([f"- {m}" for m in missing])
        + "\n\nSet these in **Streamlit → App settings → Secrets** (or ensure your workgroup has OutputLocation)."
    )
    st.stop()

# Persist "has run", and "last query stats"
if "has_run" not in st.session_state:
    st.session_state["has_run"] = False
if "last_run_utc" not in st.session_state:
    st.session_state["last_run_utc"] = None
if "last_query_stats" not in st.session_state:
    st.session_state["last_query_stats"] = ""


def record_last_query_stats(scanned: int, exec_ms: int) -> None:
    st.session_state["last_query_stats"] = format_scan_runtime(scanned, exec_ms)


with st.sidebar:
    st.header("Connection")
    st.text(f"Region: {cfg.region}")
    st.text(f"Workgroup: {cfg.workgroup}")
    st.text(f"Database: {cfg.database}")
    st.text(f"Results: {cfg.output_s3}")

    st.divider()
    st.header("Demo Status")
    st.caption(f"Last run (UTC): {st.session_state['last_run_utc'] or '—'}")
    if st.session_state["last_query_stats"]:
        st.caption(f"Last query: {st.session_state['last_query_stats']}")

    st.divider()
    st.header("Health Check")
    st.caption("Quick checks to confirm Athena + Gold views are reachable.")
    show_health = st.toggle("Show health check panel", value=True, key="show_health_panel")

    st.divider()
    st.header("Global Filters")
    st.caption("Filters apply only after you click **Run Explorer**.")

    timeout_seconds = st.slider(
        "Athena timeout (seconds)", min_value=30, max_value=240, value=120, step=10, key="timeout_seconds"
    )

    try:
        years = load_years_cached(cfg.region, cfg.database, cfg.workgroup, cfg.output_s3, timeout_seconds)
    except Exception as e:
        st.error("Could not load available years from Athena.")
        st.code(str(e))
        st.stop()

    if not years:
        st.error("No years found in `risk_feature_mart_current`. Check Gold views and database setting.")
        st.stop()

    year = st.selectbox("Year", years, index=len(years) - 1, key="year_select")

    county_fips_in = st.text_input("County FIPS (5 digits)", value="06037", key="county_fips_input")
    county_fips = normalize_county_fips(county_fips_in)

    roll_window = st.selectbox("Rolling window (years)", [3, 5, 7], index=1, key="roll_window")

    st.divider()
    st.header("Run Settings")
    show_query_stats = st.toggle("Show Athena scan + runtime stats", value=True, key="show_query_stats")

    map_points_cap = st.slider(
        "Map point cap (performance)",
        min_value=250,
        max_value=max(500, cfg.map_max_points_cap),
        value=min(cfg.map_max_points_cap, 1500),
        step=250,
        help="Hard cap after filters (prevents rendering too many points).",
        key="map_points_cap",
    )

    st.divider()
    st.header("Actions")
    c_run, c_reset, c_clear = st.columns([1, 1, 1])

    with c_run:
        if st.button("Run Explorer", type="primary", key="run_btn"):
            st.session_state["has_run"] = True
            st.session_state["last_year"] = int(year)
            st.session_state["last_county_fips"] = county_fips
            st.session_state["last_roll_window"] = int(roll_window)
            st.session_state["last_run_utc"] = utc_now_str()

    with c_reset:
        if st.button("Reset", key="reset_btn"):
            st.session_state["has_run"] = False
            st.session_state["last_query_stats"] = ""
            st.session_state["last_year"] = int(years[-1])
            st.session_state["last_county_fips"] = "06037"
            st.session_state["last_roll_window"] = 5
            st.rerun()

    with c_clear:
        if st.button("Clear cache", key="clear_cache_btn"):
            st.cache_data.clear()
            st.success("Cleared Streamlit cache. Re-run Explorer.")


# Pre-run validation
if not st.session_state["has_run"]:
    st.info("Set filters on the left and click **Run Explorer**.")
    st.stop()

# Always operate on last applied filters to avoid confusing reruns
year = int(st.session_state.get("last_year", int(year)))
county_fips = str(st.session_state.get("last_county_fips", county_fips))
roll_window = int(st.session_state.get("last_roll_window", int(roll_window)))

if not is_valid_fips(county_fips):
    st.error("County FIPS must be exactly 5 digits (e.g., 06037).")
    st.stop()

# =============================================================================
# Health Check (main body)
# =============================================================================
if st.session_state.get("show_health_panel", True):
    with st.expander("✅ Health Check (connection + Gold views)", expanded=False):
        st.caption("Runs lightweight queries and confirms row counts for key Gold `_current` views.")
        if st.button("Run Health Check", key="run_health_btn"):
            with st.spinner("Running health checks..."):
                try:
                    df_1, s1, e1 = run_query_cached(
                        region=cfg.region,
                        database=cfg.database,
                        workgroup=cfg.workgroup,
                        output_s3=cfg.output_s3,
                        sql="SELECT 1 AS ok",
                        max_rows=10,
                        timeout_seconds=timeout_seconds,
                    )
                    record_last_query_stats(s1, e1)

                    df_hc, s2, e2 = run_query_cached(
                        region=cfg.region,
                        database=cfg.database,
                        workgroup=cfg.workgroup,
                        output_s3=cfg.output_s3,
                        sql=sql_health_check_tables(year),
                        max_rows=50,
                        timeout_seconds=timeout_seconds,
                    )
                    record_last_query_stats(s2, e2)

                    st.success("Health check passed.")
                    if show_query_stats:
                        st.caption(f"SELECT 1 • {format_scan_runtime(s1, e1)}")
                        st.caption(f"Counts • {format_scan_runtime(s2, e2)}")
                    st.dataframe(df_hc, use_container_width=True)
                except Exception as ex:
                    st.error("Health check failed.")
                    st.code(str(ex))

# =============================================================================
# Applied Filters banner
# =============================================================================
st.markdown("### Applied Filters")
filter_badges(
    f"Year = **{year}**",
    f"County FIPS = **{county_fips}**",
    f"Rolling window = **{roll_window}y**",
    f"Max rows = **{cfg.max_rows}**",
)

with st.expander("How to interpret these metrics (structural vs realized)", expanded=True):
    st.markdown(
        """
**Important:** the platform combines **structural** and **realized** signals:

- **Structural risk (NRI)** is usually a *baseline* index (often static across years).
- **Realized frequency (NOAA events)** is *annual* and can be 0 in high-risk counties.
- **Realized impact (FEMA registrations / damage)** is *annual* and captures observed outcomes.

To reduce year-to-year noise, the County View also includes **rolling averages** for realized metrics.
"""
    )

with st.expander("Data sources used (Gold `_current` views)", expanded=False):
    st.markdown(
        """
- `gold_hazard.risk_feature_mart_current`
- `gold_hazard.hazard_event_summary_current`
- `gold_hazard.county_centroids_current`
"""
    )

# =============================================================================
# Results
# =============================================================================
tab_county, tab_year = st.tabs(["County View", "Year View (All Counties)"])

# ============================================================
# County View
# ============================================================
with tab_county:
    st.markdown("## County View")
    st.caption("Single-county lens (County FIPS filter applies).")

    # County lookup (name/state + lat/lon)
    with st.spinner("Loading county metadata..."):
        df_meta, scanned_meta, exec_ms_meta = run_query_cached(
            region=cfg.region,
            database=cfg.database,
            workgroup=cfg.workgroup,
            output_s3=cfg.output_s3,
            sql=sql_county_lookup(county_fips),
            max_rows=10,
            timeout_seconds=timeout_seconds,
        )
    record_last_query_stats(scanned_meta, exec_ms_meta)

    meta = {}
    if not df_meta.empty:
        meta = df_meta.iloc[0].to_dict()
        st.markdown(
            f"**Selected county:** {meta.get('county_name','—')}, {meta.get('state','—')} "
            f"(FIPS **{meta.get('county_fips','—')}**)"
        )
    if show_query_stats:
        st.caption(f"County lookup • {format_scan_runtime(scanned_meta, exec_ms_meta)}")

    st.markdown("### County time series (structural vs realized)")
    st.caption(
        "We show **realized** signals yearly and as rolling averages, alongside the (often static) **structural** NRI baseline."
    )
    filter_badges("Filters: County FIPS", "Source: risk_feature_mart_current")

    with st.spinner("Running Athena query: county time series..."):
        df_ts, scanned_ts, exec_ms_ts = run_query_cached(
            region=cfg.region,
            database=cfg.database,
            workgroup=cfg.workgroup,
            output_s3=cfg.output_s3,
            sql=sql_county_timeseries_with_rollups(county_fips=county_fips, window_years=roll_window),
            max_rows=cfg.max_rows,
            timeout_seconds=timeout_seconds,
        )
    record_last_query_stats(scanned_ts, exec_ms_ts)

    df_ts = coerce_year_int(df_ts, "year")
    for c in [
        "noaa_event_count",
        "fema_valid_registrations",
        "fema_total_damage",
        "nri_risk_score",
        f"noaa_events_roll{roll_window}",
        f"fema_regs_roll{roll_window}",
        f"fema_damage_roll{roll_window}",
    ]:
        if c in df_ts.columns:
            df_ts[c] = pd.to_numeric(df_ts[c], errors="coerce")

    # Derived metric: registrations per event (intensity)
    if {"fema_valid_registrations", "noaa_event_count"}.issubset(df_ts.columns):
        df_ts["claims_per_event"] = df_ts["fema_valid_registrations"] / df_ts["noaa_event_count"].replace(0, pd.NA)

    if show_query_stats:
        st.caption(format_scan_runtime(scanned_ts, exec_ms_ts))

    if df_ts.empty or "year" not in df_ts.columns:
        st.warning("No time series rows returned for this county. (Check county_fips formatting + Gold views.)")
    else:
        st.altair_chart(
            line_chart(
                df_ts.dropna(subset=["year", "nri_risk_score"]),
                x="year",
                y="nri_risk_score",
                title="Structural baseline: NRI risk score (often static across years)",
                y_title="NRI risk score",
                height=240,
            ),
            use_container_width=True,
        )

        st.divider()

        c1, c2 = st.columns([1, 1])
        with c1:
            st.altair_chart(
                line_chart(
                    df_ts.dropna(subset=["year", "noaa_event_count"]),
                    x="year",
                    y="noaa_event_count",
                    title="Realized frequency: NOAA events (annual)",
                    y_title="NOAA events",
                ),
                use_container_width=True,
            )
            roll_col = f"noaa_events_roll{roll_window}"
            if roll_col in df_ts.columns:
                st.altair_chart(
                    line_chart(
                        df_ts.dropna(subset=["year", roll_col]),
                        x="year",
                        y=roll_col,
                        title=f"Realized frequency: NOAA events ({roll_window}y rolling avg)",
                        y_title="NOAA events (rolling)",
                        height=240,
                    ),
                    use_container_width=True,
                )

        with c2:
            st.altair_chart(
                line_chart(
                    df_ts.dropna(subset=["year", "fema_valid_registrations"]),
                    x="year",
                    y="fema_valid_registrations",
                    title="Realized outcomes: FEMA registrations (annual)",
                    y_title="FEMA registrations",
                ),
                use_container_width=True,
            )
            roll_col = f"fema_regs_roll{roll_window}"
            if roll_col in df_ts.columns:
                st.altair_chart(
                    line_chart(
                        df_ts.dropna(subset=["year", roll_col]),
                        x="year",
                        y=roll_col,
                        title=f"Realized outcomes: FEMA registrations ({roll_window}y rolling avg)",
                        y_title="Registrations (rolling)",
                        height=240,
                    ),
                    use_container_width=True,
                )

        st.markdown("#### Claim intensity (registrations per NOAA event)")
        st.caption("Undefined in years with 0 NOAA events (division by zero).")
        if "claims_per_event" in df_ts.columns:
            st.altair_chart(
                line_chart(
                    df_ts.dropna(subset=["year", "claims_per_event"]),
                    x="year",
                    y="claims_per_event",
                    title="Realized intensity: FEMA registrations per NOAA event",
                    y_title="Registrations / event",
                    height=240,
                ),
                use_container_width=True,
            )

        st.markdown("#### Realized impact (FEMA total damage)")
        roll_col = f"fema_damage_roll{roll_window}"
        c3, c4 = st.columns([1, 1])
        with c3:
            st.altair_chart(
                line_chart(
                    df_ts.dropna(subset=["year", "fema_total_damage"]),
                    x="year",
                    y="fema_total_damage",
                    title="FEMA total damage (annual)",
                    y_title="Total damage",
                    height=240,
                ),
                use_container_width=True,
            )
        with c4:
            if roll_col in df_ts.columns:
                st.altair_chart(
                    line_chart(
                        df_ts.dropna(subset=["year", roll_col]),
                        x="year",
                        y=roll_col,
                        title=f"FEMA total damage ({roll_window}y rolling avg)",
                        y_title="Damage (rolling)",
                        height=240,
                    ),
                    use_container_width=True,
                )

        latest = df_ts.dropna(subset=["year"]).sort_values("year").tail(1)
        if not latest.empty:
            row = latest.iloc[0].to_dict()
            st.markdown("### County snapshot (latest year in series)")
            cA, cB, cC, cD = st.columns(4)
            cA.metric(
                "NRI (structural)",
                f"{row.get('nri_risk_score', float('nan')):.2f}" if pd.notna(row.get("nri_risk_score")) else "—",
            )
            cB.metric("NOAA events", f"{int(row.get('noaa_event_count'))}" if pd.notna(row.get("noaa_event_count")) else "—")
            cC.metric(
                "FEMA registrations",
                f"{row.get('fema_valid_registrations', float('nan')):.0f}"
                if pd.notna(row.get("fema_valid_registrations"))
                else "—",
            )
            cD.metric("FEMA damage", human_money(row.get("fema_total_damage")))

        with st.expander("Raw county time series (table)", expanded=False):
            st.dataframe(df_ts, use_container_width=True, column_config=dataframe_year_config())

    st.divider()

    st.markdown("### Hazard breakdown (selected county + year)")
    st.caption("Realized breakdown of observed NOAA hazards for the selected county/year.")
    filter_badges("Filters: County FIPS + Year", "Source: hazard_event_summary_current")

    with st.spinner("Running Athena query: hazard breakdown..."):
        df_h, scanned_h, exec_ms_h = run_query_cached(
            region=cfg.region,
            database=cfg.database,
            workgroup=cfg.workgroup,
            output_s3=cfg.output_s3,
            sql=sql_hazard_breakdown(county_fips=county_fips, year=year),
            max_rows=cfg.max_rows,
            timeout_seconds=timeout_seconds,
        )
    record_last_query_stats(scanned_h, exec_ms_h)

    if show_query_stats:
        st.caption(format_scan_runtime(scanned_h, exec_ms_h))

    if df_h.empty:
        st.info(
            "No hazard rows returned for this county/year. This can happen in quiet years (0 realized events) "
            "even if structural risk is high."
        )
    st.dataframe(df_h, use_container_width=True)

# ============================================================
# Year View (All Counties)
# ============================================================
with tab_year:
    st.markdown("## Year View (All Counties)")
    st.caption("Single-year lens (Year filter applies). Distinguishes **structural** vs **realized** rankings.")

    # KPI row (fast, high-signal)
    with st.spinner("Computing year KPIs..."):
        df_k, scanned_k, exec_ms_k = run_query_cached(
            region=cfg.region,
            database=cfg.database,
            workgroup=cfg.workgroup,
            output_s3=cfg.output_s3,
            sql=sql_year_kpis(year),
            max_rows=50,
            timeout_seconds=timeout_seconds,
        )
    record_last_query_stats(scanned_k, exec_ms_k)

    if show_query_stats:
        st.caption(f"Year KPIs • {format_scan_runtime(scanned_k, exec_ms_k)}")

    if not df_k.empty:
        k = df_k.iloc[0].to_dict()
        pct_zero = safe_float(k.get("pct_noaa_zero"))
        med_zero = safe_float(k.get("median_nri_noaa_zero"))
        med_pos = safe_float(k.get("median_nri_noaa_pos"))

        c1, c2, c3 = st.columns(3)
        c1.metric("% counties with NOAA=0", f"{pct_zero:.2f}%" if pct_zero is not None else "—")
        c2.metric("Median NRI (NOAA=0)", f"{med_zero:.2f}" if med_zero is not None else "—")
        c3.metric("Median NRI (NOAA>0)", f"{med_pos:.2f}" if med_pos is not None else "—")

    st.divider()
    st.markdown("### Ranking mode")
    rank_mode = st.radio(
        "Rank counties by…",
        [
            "Structural risk (NRI baseline)",
            "Realized frequency (NOAA events)",
            "Realized outcomes (FEMA registrations)",
            "Realized impact (FEMA damage)",
        ],
        index=0,
        horizontal=True,
        key="rank_mode",
    )

    if rank_mode.startswith("Structural"):
        base_sql = sql_ranked_risk_structural(year=year, limit=1000)
        rank_explain = "Ranks by **NRI** (structural baseline; often static across years)."
        rank_col = "nri_risk_score"
    elif rank_mode.startswith("Realized frequency"):
        base_sql = sql_ranked_risk_realized_frequency(year=year, limit=1000)
        rank_explain = "Ranks by **NOAA event count** (realized annual frequency)."
        rank_col = "noaa_event_count"
    elif rank_mode.startswith("Realized outcomes"):
        base_sql = sql_ranked_risk_realized_claims(year=year, limit=1000)
        rank_explain = "Ranks by **FEMA registrations** (realized annual outcomes proxy)."
        rank_col = "fema_valid_registrations"
    else:
        base_sql = sql_ranked_risk_realized_impact(year=year, limit=1000)
        rank_explain = "Ranks by **FEMA total damage** (realized annual impact proxy)."
        rank_col = "fema_total_damage"

    st.caption(rank_explain)

    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.markdown("### Top claim intensity (registrations per event)")
        st.caption("Spots counties with unusually high registrations relative to realized event count.")
        filter_badges("Filters: Year", "Source: risk_feature_mart_current")

        with st.spinner("Running Athena query: top claim intensity..."):
            df_top, scanned_top, exec_ms_top = run_query_cached(
                region=cfg.region,
                database=cfg.database,
                workgroup=cfg.workgroup,
                output_s3=cfg.output_s3,
                sql=sql_top_claims_per_event(year=year, limit=25),
                max_rows=cfg.max_rows,
                timeout_seconds=timeout_seconds,
            )
        record_last_query_stats(scanned_top, exec_ms_top)

        df_top = coerce_year_int(df_top, "year")
        for c in ["fema_valid_registrations", "noaa_event_count", "claims_per_event"]:
            if c in df_top.columns:
                df_top[c] = pd.to_numeric(df_top[c], errors="coerce")

        if show_query_stats:
            st.caption(format_scan_runtime(scanned_top, exec_ms_top))
        st.dataframe(df_top, use_container_width=True, column_config=dataframe_year_config())

        st.divider()
        st.markdown("### NOAA=0 but high structural risk (NRI)")
        st.caption("Signature pattern: **high baseline risk** even when realized annual NOAA events are 0.")
        with st.spinner("Running Athena query: NOAA=0 high NRI..."):
            df_n0, scanned_n0, exec_ms_n0 = run_query_cached(
                region=cfg.region,
                database=cfg.database,
                workgroup=cfg.workgroup,
                output_s3=cfg.output_s3,
                sql=sql_noaa0_high_nri(year=year, limit=50),
                max_rows=1000,
                timeout_seconds=timeout_seconds,
            )
        record_last_query_stats(scanned_n0, exec_ms_n0)

        df_n0 = coerce_year_int(df_n0, "year")
        for c in ["nri_risk_score", "noaa_event_count", "fema_valid_registrations", "fema_total_damage"]:
            if c in df_n0.columns:
                df_n0[c] = pd.to_numeric(df_n0[c], errors="coerce")

        if show_query_stats:
            st.caption(format_scan_runtime(scanned_n0, exec_ms_n0))

        # Polished: remove drilldown UI, keep table only
        if df_n0.empty:
            st.info("No rows found. (Unusual — verify data for this year.)")
        else:
            st.dataframe(df_n0, use_container_width=True, column_config=dataframe_year_config())

    with col_right:
        st.markdown("### Ranked counties + map")
        st.caption("Map uses county centroids (points). Includes a **highlight** for your selected County FIPS.")

        map_mode = st.radio(
            "Map mode",
            ["Show map (requires county centroids view)", "Table only"],
            index=0,
            horizontal=True,
            key="map_mode",
        )

        metric = st.selectbox(
            "Map color metric",
            ["nri_risk_score", "noaa_event_count", "fema_valid_registrations", "fema_total_damage"],
            index=0,
            key="map_metric",
        )
        size_by = st.selectbox(
            "Dot size by",
            ["noaa_event_count", "fema_valid_registrations", "fema_total_damage", "nri_risk_score", "(constant)"],
            index=0,
            key="map_size_by",
        )
        only_noaa0 = st.toggle("Filter: only NOAA=0 counties", value=False, key="only_noaa0")
        top_n = st.slider(
            "Max points (top N after ranking)",
            min_value=100,
            max_value=1000,
            value=min(cfg.map_default_top_n, 1000),
            step=50,
            key="top_n",
        )

        want_map = map_mode.startswith("Show map")
        sql_to_run = sql_ranked_risk_with_centroids(base_sql) if want_map else base_sql

        # Load ranked dataset (optionally with centroids). If centroids query fails, we fall back to table-only.
        if want_map:
            try:
                with st.spinner("Running Athena query: ranked counties (with centroids)..."):
                    df_r, scanned_r, exec_ms_r = run_query_cached(
                        region=cfg.region,
                        database=cfg.database,
                        workgroup=cfg.workgroup,
                        output_s3=cfg.output_s3,
                        sql=sql_to_run,
                        max_rows=max(cfg.max_rows, 1500),
                        timeout_seconds=timeout_seconds,
                    )
            except Exception as e:
                st.warning("Could not load county centroids for map rendering. Falling back to table-only.")
                with st.expander("Error details (debug)", expanded=False):
                    st.code(str(e))
                with st.spinner("Running Athena query: ranked counties (table)..."):
                    df_r, scanned_r, exec_ms_r = run_query_cached(
                        region=cfg.region,
                        database=cfg.database,
                        workgroup=cfg.workgroup,
                        output_s3=cfg.output_s3,
                        sql=base_sql,
                        max_rows=max(cfg.max_rows, 1500),
                        timeout_seconds=timeout_seconds,
                    )
                want_map = False  # no centroids, so we won't attempt the map
        else:
            with st.spinner("Running Athena query: ranked counties (table)..."):
                df_r, scanned_r, exec_ms_r = run_query_cached(
                    region=cfg.region,
                    database=cfg.database,
                    workgroup=cfg.workgroup,
                    output_s3=cfg.output_s3,
                    sql=base_sql,
                    max_rows=max(cfg.max_rows, 1500),
                    timeout_seconds=timeout_seconds,
                )

        record_last_query_stats(scanned_r, exec_ms_r)

        df_r = coerce_year_int(df_r, "year")
        for c in ["nri_risk_score", "noaa_event_count", "fema_valid_registrations", "fema_total_damage", "lat", "lon"]:
            if c in df_r.columns:
                df_r[c] = pd.to_numeric(df_r[c], errors="coerce")

        if show_query_stats:
            st.caption(format_scan_runtime(scanned_r, exec_ms_r))

        if "noaa_event_count" in df_r.columns and len(df_r) > 0:
            zero_pct = 100.0 * (df_r["noaa_event_count"].fillna(0).astype(float) == 0).mean()
            st.caption(f"Share of counties with **0 NOAA events** in {year}: {zero_pct:.2f}%")

        # Apply filters / top-N
        df_view = df_r.copy()
        if only_noaa0 and "noaa_event_count" in df_view.columns:
            df_view = df_view[df_view["noaa_event_count"].fillna(0).astype(float) == 0.0]
        df_view = df_view.head(int(top_n))

        # Map rendering (pydeck) if we have lat/lon
        if want_map and {"lat", "lon"}.issubset(df_view.columns):
            df_map = df_view.dropna(subset=["lat", "lon"]).copy()
            df_map["lat"] = pd.to_numeric(df_map["lat"], errors="coerce")
            df_map["lon"] = pd.to_numeric(df_map["lon"], errors="coerce")
            df_map = df_map.dropna(subset=["lat", "lon"])

            # Hard cap for performance
            df_map = df_map.head(int(map_points_cap))

            if df_map.empty:
                st.warning("No lat/lon values available to plot on a map (after filters).")
            else:
                # Build size scalar
                if size_by == "(constant)":
                    df_map["_radius"] = 25000.0
                else:
                    sraw = pd.to_numeric(df_map.get(size_by), errors="coerce").fillna(0.0).clip(lower=0.0)
                    smax = float(sraw.max()) if len(sraw) else 0.0
                    df_map["_radius"] = 25000.0 if smax <= 0 else (2000.0 + 38000.0 * (sraw / smax)).astype(float)

                # Build color scalar (simple normalization, no custom palette dependency)
                vraw = pd.to_numeric(df_map.get(metric), errors="coerce").fillna(0.0)
                vmax = float(vraw.max()) if len(vraw) else 0.0
                if vmax <= 0:
                    df_map["_c"] = 120
                else:
                    df_map["_c"] = (50 + 180 * (vraw / vmax)).clip(50, 230).astype(int)

                # RGBA list
                df_map["_color"] = df_map["_c"].apply(lambda x: [int(x), 60, int(255 - min(int(x), 200)), 160])

                tooltip = {
                    "html": (
                        "<b>{county_name}, {state}</b><br/>"
                        "FIPS: {county_fips}<br/>"
                        f"{metric}: {{{metric}}}<br/>"
                        "NOAA events: {noaa_event_count}<br/>"
                        "FEMA regs: {fema_valid_registrations}<br/>"
                        "FEMA damage: {fema_total_damage}"
                    ),
                    "style": {"backgroundColor": "white", "color": "black"},
                }

                base_layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=df_map,
                    get_position="[lon, lat]",
                    get_radius="_radius",
                    get_fill_color="_color",
                    pickable=True,
                    stroked=False,
                    auto_highlight=True,
                )

                # Highlight selected county (if present with lat/lon)
                highlight_layers = [base_layer]
                selected_cf = normalize_county_fips(county_fips)
                df_sel = df_map[df_map["county_fips"].astype(str).apply(normalize_county_fips) == selected_cf].copy()
                if not df_sel.empty:
                    df_sel["_radius_sel"] = (df_sel["_radius"].astype(float) * 1.4).clip(4000.0, 60000.0)
                    highlight_layer = pdk.Layer(
                        "ScatterplotLayer",
                        data=df_sel,
                        get_position="[lon, lat]",
                        get_radius="_radius_sel",
                        get_fill_color="[255, 165, 0, 220]",  # highlight: orange-ish
                        pickable=True,
                        stroked=True,
                        get_line_color="[0, 0, 0, 220]",
                        line_width_min_pixels=2,
                    )
                    highlight_layers.append(highlight_layer)

                view_state = pdk.ViewState(latitude=39.5, longitude=-98.35, zoom=3, pitch=0)

                st.markdown("#### Map (county centroids)")
                st.caption(f"Color: {metric} • Size: {size_by} • Points shown: {len(df_map)} (cap={map_points_cap})")
                st.pydeck_chart(pdk.Deck(layers=highlight_layers, initial_view_state=view_state, tooltip=tooltip))

        st.markdown(f"#### Ranked table (top 50 by {rank_col})")
        st.dataframe(df_view.head(50), use_container_width=True, column_config=dataframe_year_config())

        # Polished: remove ranked-table drilldown UI
        csv = df_view.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            data=csv,
            file_name=f"ranked_{rank_mode.split('(')[0].strip().replace(' ', '_').lower()}_{year}.csv",
            mime="text/csv",
        )

st.divider()
st.markdown("### Platform guarantees")
st.markdown(
    """
- Reads from Gold `_current` views (stable downstream contract)
- Partition-aware queries (Year filter reduces scanned data and cost)
- Deterministic build → validate → promote pipeline supports stable analytics
- Hard-fail behavior (no silent data corruption)
"""
)

st.success("Done. This demo reads from Gold `_current` views and stays stable across rebuilds.")