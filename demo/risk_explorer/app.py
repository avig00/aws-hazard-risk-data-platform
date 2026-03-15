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


YEAR_OPTIONS = list(range(2010, 2024))


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


def sql_county_directory() -> str:
    return """
    SELECT
      LPAD(CAST(county_fips AS VARCHAR), 5, '0') AS county_fips,
      county_name,
      state
    FROM gold_hazard.county_centroids_current
    ORDER BY state, county_name
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
STREAMLIT_RED = "#ff4b4b"


def inject_accent_styles() -> None:
    st.markdown(
        f"""
        <style>
        div[data-testid="stMarkdownContainer"] code,
        div[data-testid="stCaptionContainer"] code,
        p code,
        li code {{
            color: {STREAMLIT_RED} !important;
            background-color: rgba(255, 75, 75, 0.12) !important;
            border: 1px solid rgba(255, 75, 75, 0.18);
        }}
        .hero-insight {{
            border: 1px solid rgba(255, 75, 75, 0.22);
            border-radius: 14px;
            padding: 1rem 1.1rem;
            margin: 0.35rem 0 1rem 0;
            background: linear-gradient(180deg, rgba(255, 75, 75, 0.08), rgba(255, 75, 75, 0.03));
        }}
        .hero-insight .eyebrow {{
            color: {STREAMLIT_RED};
            font-size: 0.8rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            margin-bottom: 0.25rem;
        }}
        .hero-insight h3 {{
            margin: 0;
            font-size: 1.25rem;
        }}
        .hero-insight p {{
            margin: 0.35rem 0 0;
            opacity: 0.9;
        }}
        .hero-insight ul {{
            margin: 0.7rem 0 0;
            padding-left: 1.15rem;
        }}
        .panel-label {{
            color: {STREAMLIT_RED};
            font-size: 0.8rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            margin-bottom: 0.1rem;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def filter_badges(*labels: str) -> None:
    if not labels:
        return
    st.caption(" | ".join(labels))


def section_intro(title: str, body: str) -> None:
    st.subheader(title)
    st.caption(body)


def render_notice(title: str, body: str, *, tone: str = "info") -> None:
    message = f"**{title}**\n\n{body}"
    if tone == "warning":
        st.warning(message)
    elif tone == "success":
        st.success(message)
    else:
        st.info(message)


def render_kpi_cards(cards: List[Tuple[str, str]]) -> None:
    if not cards:
        return
    cols = st.columns(len(cards))
    for col, (label, value) in zip(cols, cards):
        col.metric(label, value)


def render_insight_block(title: str, summary: str, bullets: List[str]) -> None:
    st.markdown(f"### {title}")
    st.caption(summary)
    if bullets:
        st.markdown("\n".join([f"- {item}" for item in bullets]))


def render_hero_insight(eyebrow: str, title: str, summary: str, bullets: List[str]) -> None:
    bullets_html = "".join([f"<li>{item}</li>" for item in bullets]) if bullets else ""
    st.markdown(
        f"""
        <div class="hero-insight">
            <div class="eyebrow">{eyebrow}</div>
            <h3>{title}</h3>
            <p>{summary}</p>
            {"<ul>" + bullets_html + "</ul>" if bullets_html else ""}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_panel_label(text: str) -> None:
    st.markdown(f"<div class='panel-label'>{text}</div>", unsafe_allow_html=True)


def render_map_color_scale(metric: str, values: pd.Series) -> None:
    vals = pd.to_numeric(values, errors="coerce").dropna()
    if vals.empty:
        return

    vmin = float(vals.min())
    vmax = float(vals.max())
    vmid = float(vals.quantile(0.5))
    fmt = ".0f" if max(abs(vmin), abs(vmid), abs(vmax)) >= 100 else ".2f"

    st.caption(f"Color scale for `{metric}`")
    st.markdown(
        """
        <div style="margin:0.2rem 0 0.35rem 0;">
          <div style="height:12px;border-radius:999px;background:linear-gradient(90deg, rgba(50,60,205,0.70) 0%, rgba(140,60,145,0.80) 50%, rgba(230,60,55,0.90) 100%);"></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    legend_cols = st.columns(3)
    legend_cols[0].caption(f"Low: {format(vmin, fmt)}")
    legend_cols[1].caption(f"Median: {format(vmid, fmt)}")
    legend_cols[2].caption(f"High: {format(vmax, fmt)}")


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
    base = alt.Chart(df).mark_line(point=alt.OverlayMarkDef(filled=True, size=55), strokeWidth=3).encode(
        x=alt.X(f"{x}:Q", axis=fmt_year_axis()),
        y=alt.Y(f"{y}:Q", axis=alt.Axis(title=y_title)),
        tooltip=[alt.Tooltip(f"{x}:Q", format="d"), alt.Tooltip(f"{y}:Q")],
        color=alt.value(STREAMLIT_RED),
    )
    return base.properties(title=title, height=height).interactive()


def bar_chart(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    x_title: str,
    y_title: str,
    height: int = 300,
) -> alt.Chart:
    plot_df = df.copy()
    if y in plot_df.columns:
        plot_df[y] = pd.to_numeric(plot_df[y], errors="coerce")
    plot_df = plot_df.dropna(subset=[x, y])

    x_enc = alt.X(
        f"{x}:N",
        sort=alt.EncodingSortField(field=y, order="descending"),
        axis=alt.Axis(title=x_title, labelAngle=0, labelLimit=240),
    )
    y_enc = alt.Y(f"{y}:Q", axis=alt.Axis(title=y_title))

    bars = alt.Chart(plot_df).mark_bar(size=42).encode(
        x=x_enc,
        y=y_enc,
        tooltip=[alt.Tooltip(f"{x}:N"), alt.Tooltip(f"{y}:Q")],
        color=alt.value(STREAMLIT_RED),
    )
    labels = alt.Chart(plot_df).mark_text(dy=-8, color="white").encode(
        x=x_enc,
        y=y_enc,
        text=alt.Text(f"{y}:Q", format=".0f"),
    )
    return (bars + labels).properties(
        title=alt.TitleParams(text=title, anchor="start", offset=18),
        height=height,
    )


def dataframe_year_config():
    return {"year": st.column_config.NumberColumn("year", format="%d")}


def show_table(df: pd.DataFrame, *, column_config=None) -> None:
    st.dataframe(df, use_container_width=True, hide_index=True, column_config=column_config)


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
st.set_page_config(page_title="Risk Explorer", page_icon="🌊", layout="wide")
inject_accent_styles()
hero_left, hero_right = st.columns([2.2, 1.2])
with hero_left:
    st.title("Risk Explorer")
    st.caption(
        "Lightweight demo over validated Gold `_current` views in Athena for structural vs realized risk analysis."
    )
with hero_right:
    st.markdown("##### At a Glance")
    st.caption("Stable downstream contract")
    st.caption("Partition-aware queries")
    st.caption("County and year views")

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
if "pending_county_fips" not in st.session_state:
    st.session_state["pending_county_fips"] = None
if "pending_year_select" not in st.session_state:
    st.session_state["pending_year_select"] = None
if "pending_roll_window" not in st.session_state:
    st.session_state["pending_roll_window"] = None
page_options = ["County FIPS Directory", "County View", "Year View (All Counties)"]
if "current_page" not in st.session_state:
    qp_page = str(st.query_params.get("page", page_options[0]))
    st.session_state["current_page"] = qp_page if qp_page in page_options else page_options[0]
if "last_synced_page" not in st.session_state:
    st.session_state["last_synced_page"] = st.session_state["current_page"]
if "directory_state" not in st.session_state:
    st.session_state["directory_state"] = str(st.query_params.get("directory_state", "All"))
if "directory_county_search" not in st.session_state:
    st.session_state["directory_county_search"] = str(st.query_params.get("directory_search", ""))
if "last_year" not in st.session_state:
    qp_year = st.query_params.get("year")
    st.session_state["last_year"] = int(qp_year) if str(qp_year).isdigit() and int(qp_year) in YEAR_OPTIONS else YEAR_OPTIONS[-1]
if "last_county_fips" not in st.session_state:
    qp_fips = normalize_county_fips(str(st.query_params.get("county_fips", "06037")))
    st.session_state["last_county_fips"] = qp_fips if is_valid_fips(qp_fips) else "06037"
if "last_roll_window" not in st.session_state:
    qp_roll = st.query_params.get("roll_window")
    st.session_state["last_roll_window"] = int(qp_roll) if str(qp_roll).isdigit() and int(qp_roll) in [3, 5, 7] else 5
if "county_fips_input" not in st.session_state:
    st.session_state["county_fips_input"] = st.session_state["last_county_fips"]
if "year_select" not in st.session_state:
    st.session_state["year_select"] = st.session_state["last_year"]
if "roll_window" not in st.session_state:
    st.session_state["roll_window"] = st.session_state["last_roll_window"]
if str(st.query_params.get("has_run", "0")) == "1":
    st.session_state["has_run"] = True


def record_last_query_stats(scanned: int, exec_ms: int) -> None:
    st.session_state["last_query_stats"] = format_scan_runtime(scanned, exec_ms)


def sync_query_params() -> None:
    st.query_params.clear()
    st.query_params["page"] = str(st.session_state.get("current_page", page_options[0]))
    st.query_params["year"] = str(st.session_state.get("last_year", YEAR_OPTIONS[-1]))
    st.query_params["county_fips"] = str(st.session_state.get("last_county_fips", "06037"))
    st.query_params["roll_window"] = str(st.session_state.get("last_roll_window", 5))
    st.query_params["has_run"] = "1" if st.session_state.get("has_run", False) else "0"
    if st.session_state.get("directory_state", "All") != "All":
        st.query_params["directory_state"] = str(st.session_state["directory_state"])
    directory_search = str(st.session_state.get("directory_county_search", "")).strip()
    if directory_search:
        st.query_params["directory_search"] = directory_search


with st.sidebar:
    st.header("Connection")
    st.caption("Runtime target for this session.")
    st.text(f"Region: {cfg.region}")
    st.text(f"Workgroup: {cfg.workgroup}")
    st.text(f"Database: {cfg.database}")
    st.text(f"Results: {cfg.output_s3}")

    st.divider()
    st.header("Demo Status")
    st.caption("Most recent run state.")
    st.caption(f"Last run (UTC): {st.session_state['last_run_utc'] or '—'}")
    if st.session_state["last_query_stats"]:
        st.caption(f"Last query: {st.session_state['last_query_stats']}")

    st.divider()
    st.header("Health Check")
    st.caption("Quick checks to confirm Athena + Gold views are reachable.")
    show_health = st.toggle("Show health check panel", value=True, key="show_health_panel")

    st.divider()
    st.header("Global Filters")
    st.caption("Changes apply only after you click `Run Explorer`.")

    timeout_seconds = st.slider(
        "Athena timeout (seconds)", min_value=30, max_value=240, value=120, step=10, key="timeout_seconds"
    )

    # Do not block initial page load on a live Athena query.
    # The mart contract already enforces a fixed 2010-2023 year window.
    years = YEAR_OPTIONS

    if st.session_state.get("pending_year_select") is not None:
        st.session_state["year_select"] = int(st.session_state["pending_year_select"])
        st.session_state["pending_year_select"] = None
    year = st.selectbox("Year", years, index=years.index(int(st.session_state["year_select"])), key="year_select")

    if st.session_state.get("pending_county_fips"):
        st.session_state["county_fips_input"] = st.session_state["pending_county_fips"]
        st.session_state["pending_county_fips"] = None

    county_fips_in = st.text_input("County FIPS (5 digits)", value=st.session_state["county_fips_input"], key="county_fips_input")
    county_fips = normalize_county_fips(county_fips_in)

    if st.session_state.get("pending_roll_window") is not None:
        st.session_state["roll_window"] = int(st.session_state["pending_roll_window"])
        st.session_state["pending_roll_window"] = None
    roll_window = st.selectbox("Rolling window (years)", [3, 5, 7], index=[3, 5, 7].index(int(st.session_state["roll_window"])), key="roll_window")

    st.divider()
    st.header("Run Settings")
    st.caption("Performance and telemetry controls.")
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
        if st.button("Run Explorer", key="run_btn"):
            st.session_state["has_run"] = True
            st.session_state["last_year"] = int(year)
            st.session_state["last_county_fips"] = county_fips
            st.session_state["last_roll_window"] = int(roll_window)
            st.session_state["last_run_utc"] = utc_now_str()
            sync_query_params()

    with c_reset:
        if st.button("Reset", key="reset_btn"):
            st.session_state["has_run"] = False
            st.session_state["last_query_stats"] = ""
            st.session_state["last_year"] = int(years[-1])
            st.session_state["last_county_fips"] = "06037"
            st.session_state["last_roll_window"] = 5
            st.session_state["pending_year_select"] = int(years[-1])
            st.session_state["pending_county_fips"] = "06037"
            st.session_state["pending_roll_window"] = 5
            sync_query_params()
            st.rerun()

    with c_clear:
        if st.button("Clear cache", key="clear_cache_btn"):
            st.cache_data.clear()
            st.success("Cleared Streamlit cache. Re-run Explorer.")


# Always operate on last applied filters to avoid confusing reruns
year = int(st.session_state.get("last_year", int(year)))
county_fips = str(st.session_state.get("last_county_fips", county_fips))
roll_window = int(st.session_state.get("last_roll_window", int(roll_window)))

if st.session_state["has_run"] and not is_valid_fips(county_fips):
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
section_intro(
    "Explorer Summary",
    "Use the KPI row first, then read County View for trend context and Year View for ranking and anomalies.",
)
filter_badges(
    f"Year = {year}",
    f"County FIPS = {county_fips}",
    f"Rolling window = {roll_window}y",
    f"Max rows = {cfg.max_rows}",
)

render_kpi_cards(
    [
        ("Athena workgroup", cfg.workgroup),
        ("Gold database", cfg.database),
        ("Last run (UTC)", st.session_state["last_run_utc"] or "Not run yet"),
    ]
)

render_hero_insight(
    "Primary Brief",
    f"Review {year} first, then drill into county {county_fips}",
    "This app is strongest when used as a guided read: confirm the active slice, inspect the dominant view in each tab, and only then open supporting diagnostics.",
    [
        "County View is optimized for structural vs realized trend comparison.",
        "Year View is optimized for ranking, anomaly detection, and geographic pattern spotting.",
        "Diagnostics and raw tables are intentionally lower in the layout so the main read stays clear.",
    ],
)

overview_left, overview_right = st.columns([1.4, 1])
with overview_left:
    st.markdown("##### Start Here")
    st.markdown(
        """
1. Review the KPI row to confirm the active slice and environment.
2. Use **County View** to compare structural baseline vs observed outcomes for one county.
3. Use **Year View** to scan the map first, then verify exact ordering in the ranked table.
"""
    )
with overview_right:
    st.markdown("##### Design Focus")
    st.caption("Primary insights stay visible; diagnostics and raw data live lower in the layout.")
    if st.session_state["last_query_stats"]:
        st.caption(f"Most recent query: {st.session_state['last_query_stats']}")

page = st.radio(
    "Page",
    page_options,
    horizontal=True,
    key="current_page",
    label_visibility="collapsed",
)
if st.session_state.get("last_synced_page") != page:
    st.session_state["last_synced_page"] = page
    sync_query_params()

with st.expander("How to interpret these metrics (structural vs realized)", expanded=True):
    st.markdown(
    """
**Important:** the platform combines **structural** and **realized** signals:

- **Structural risk (NRI)** is usually a *baseline* index (often static across years).
- **Realized frequency (NOAA events)** is *annual* and can be 0 in high-risk counties.
- **Realized impact (FEMA registrations / damage)** is *annual* and captures observed outcomes.

**Why you may see NOAA=0 but high NRI:** NRI reflects long-term expected loss (hazard probability + exposure + vulnerability), while NOAA events are year-specific realizations and can be zero in a “quiet” year.

To reduce year-to-year noise, the County View also includes **rolling averages** for realized metrics.
"""
    )

# =============================================================================
# Results
# =============================================================================
# ============================================================
# County FIPS Directory
# ============================================================
if page == "County FIPS Directory":
    render_panel_label("County Lookup")
    st.markdown("## County FIPS Directory")
    st.caption(
        "Search the canonical county reference by state and county name, then use the returned 5-digit FIPS in the Risk Explorer."
    )

    with st.spinner("Loading county directory..."):
        df_dir, scanned_dir, exec_ms_dir = run_query_cached(
            region=cfg.region,
            database=cfg.database,
            workgroup=cfg.workgroup,
            output_s3=cfg.output_s3,
            sql=sql_county_directory(),
            max_rows=5000,
            timeout_seconds=timeout_seconds,
        )
    record_last_query_stats(scanned_dir, exec_ms_dir)

    if show_query_stats:
        st.caption(f"County directory • {format_scan_runtime(scanned_dir, exec_ms_dir)}")

    if df_dir.empty:
        render_notice(
            "Directory Unavailable",
            "The county directory query returned no rows. Confirm that `gold_hazard.county_centroids_current` is reachable.",
            tone="warning",
        )
    else:
        states = ["All"] + sorted(df_dir["state"].dropna().astype(str).unique().tolist())
        dir_c1, dir_c2 = st.columns([1, 1.5])
        with dir_c1:
            current_directory_state = st.session_state.get("directory_state", "All")
            if current_directory_state not in states:
                current_directory_state = "All"
                st.session_state["directory_state"] = "All"
            state_filter = st.selectbox(
                "State",
                states,
                index=states.index(current_directory_state),
                key="directory_state",
            )
        with dir_c2:
            county_search = st.text_input(
                "County name contains",
                key="directory_county_search",
                placeholder="Example: Los Angeles",
            ).strip()

        if (
            st.session_state.get("directory_state") != state_filter
            or st.session_state.get("directory_county_search", "").strip() != county_search
        ):
            st.session_state["directory_state"] = state_filter
            st.session_state["directory_county_search"] = county_search
            sync_query_params()

        df_dir_view = df_dir.copy()
        if state_filter != "All":
            df_dir_view = df_dir_view[df_dir_view["state"].astype(str) == state_filter]
        if county_search:
            df_dir_view = df_dir_view[
                df_dir_view["county_name"].astype(str).str.contains(county_search, case=False, na=False)
            ]

        df_dir_view = df_dir_view.sort_values(["state", "county_name"]).reset_index(drop=True)

        st.caption(f"Matches: {len(df_dir_view)}")
        st.caption("Browse the table, then select one of the filtered matches below to use its FIPS in the explorer.")
        selected_fips = None
        if not df_dir_view.empty:
            st.dataframe(
                df_dir_view,
                use_container_width=True,
                hide_index=True,
                column_config={"county_fips": st.column_config.TextColumn("county_fips")},
            )
            options = [
                f"{row.county_name}, {row.state} ({row.county_fips})"
                for row in df_dir_view[["county_name", "state", "county_fips"]].itertuples(index=False)
            ]
            selected_label = st.selectbox(
                "Select county from filtered matches",
                options,
                index=0,
                key="directory_selected_county",
            )
            selected_fips = selected_label.rsplit("(", 1)[-1].rstrip(")")
            selected_prefix = selected_label.rsplit("(", 1)[0].rstrip().rstrip(",")
            if "," in selected_prefix:
                selected_county, selected_state = [part.strip() for part in selected_prefix.rsplit(",", 1)]
            else:
                selected_county, selected_state = selected_prefix.strip(), "—"
            render_kpi_cards(
                [
                    ("Selected county FIPS", selected_fips),
                    ("Selected county", f"{selected_county}, {selected_state}"),
                    ("County search", county_search or "—"),
                ]
            )
            if st.button("Use this FIPS in Explorer", key="directory_apply_fips"):
                st.session_state["pending_county_fips"] = selected_fips
                st.session_state["last_county_fips"] = selected_fips
                sync_query_params()
                st.rerun()
        else:
            render_notice(
                "No Directory Matches",
                "Try a broader state filter or a shorter county-name search.",
            )

# ============================================================
# County View
# ============================================================
elif page == "County View":
    if not st.session_state["has_run"]:
        render_panel_label("Primary Analysis")
        st.markdown("## County View")
        render_notice("Ready to Run", "Set filters in the sidebar, then click `Run Explorer` to populate County View.")
    else:
        render_panel_label("Primary Analysis")
        st.markdown("## County View")
        st.caption("Single-county trend read. Start with the snapshot, then compare baseline vs realized annual movement.")

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
            c_meta1, c_meta2 = st.columns([1.8, 1])
            c_meta1.metric("Selected county", f"{meta.get('county_name','—')}, {meta.get('state','—')}")
            c_meta2.metric("County FIPS", meta.get("county_fips", "—"))
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
            render_notice(
                "No County Time Series",
                "No rows were returned for this county. Confirm the County FIPS format and that the Gold views are populated for this slice.",
                tone="warning",
            )
        else:
            latest = df_ts.dropna(subset=["year"]).sort_values("year").tail(1)
            if not latest.empty:
                row = latest.iloc[0].to_dict()
                st.markdown("### County snapshot (latest year in series)")
                render_kpi_cards(
                    [
                        (
                            "NRI (structural)",
                            f"{row.get('nri_risk_score', float('nan')):.2f}"
                            if pd.notna(row.get("nri_risk_score"))
                            else "—",
                        ),
                        (
                            "NOAA events",
                            f"{int(row.get('noaa_event_count'))}"
                            if pd.notna(row.get("noaa_event_count"))
                            else "—",
                        ),
                        (
                            "FEMA registrations",
                            f"{row.get('fema_valid_registrations', float('nan')):.0f}"
                            if pd.notna(row.get("fema_valid_registrations"))
                            else "—",
                        ),
                        ("FEMA damage", human_money(row.get("fema_total_damage"))),
                    ]
                )
                signal_notes = []
                if pd.notna(row.get("nri_risk_score")):
                    signal_notes.append(f"Structural baseline is {float(row.get('nri_risk_score')):.2f} in the latest year.")
                if pd.notna(row.get("noaa_event_count")) and pd.notna(row.get("fema_valid_registrations")):
                    signal_notes.append(
                        f"Observed {int(row.get('noaa_event_count'))} NOAA events and {float(row.get('fema_valid_registrations')):.0f} FEMA registrations."
                    )
                if "claims_per_event" in df_ts.columns:
                    latest_cpe = df_ts.dropna(subset=["year"]).sort_values("year").tail(1).iloc[0].get("claims_per_event")
                    if pd.notna(latest_cpe):
                        signal_notes.append(f"Claim intensity is {float(latest_cpe):.2f} registrations per NOAA event.")
                render_insight_block(
                    "Signature Read",
                    "Use this as the first interpretation checkpoint before scanning the detailed trend lines.",
                    signal_notes
                    or [
                        "The latest year snapshot is available, but not all derived metrics are populated for this county."
                    ],
                )

        primary_left, primary_right = st.columns([1.8, 1])
        with primary_left:
            st.altair_chart(
                line_chart(
                    df_ts.dropna(subset=["year", "nri_risk_score"]),
                    x="year",
                    y="nri_risk_score",
                    title="Is the structural baseline persistently elevated?",
                    y_title="NRI risk score",
                    height=260,
                ),
                use_container_width=True,
            )
        with primary_right:
            render_hero_insight(
                "How To Read This",
                "Start with baseline, then compare realized deviations",
                "This panel is the main interpretive anchor for the county view.",
                [
                    "Start with the structural baseline and note whether it stays elevated over time.",
                    "Compare annual NOAA event spikes against that baseline.",
                    "Use FEMA outcomes to see whether realized impacts track, lag, or overreact to hazard frequency.",
                ],
            )

        st.markdown("### Primary realized signals")
        primary_c1, primary_c2 = st.columns(2)
        with primary_c1:
            st.altair_chart(
                line_chart(
                    df_ts.dropna(subset=["year", "noaa_event_count"]),
                    x="year",
                    y="noaa_event_count",
                    title="When did realized hazard frequency spike?",
                    y_title="NOAA events",
                ),
                use_container_width=True,
            )
        with primary_c2:
            st.altair_chart(
                line_chart(
                    df_ts.dropna(subset=["year", "fema_valid_registrations"]),
                    x="year",
                    y="fema_valid_registrations",
                    title="When did realized outcomes materially increase?",
                    y_title="FEMA registrations",
                ),
                use_container_width=True,
            )

        with st.expander("Trend details (rolling averages)", expanded=False):
            st.caption("Use rolling averages only after the annual trend shapes are clear. They are for smoothing, not first-pass interpretation.")
            roll_col = f"noaa_events_roll{roll_window}"
            detail_c1, detail_c2 = st.columns(2)
            with detail_c1:
                if roll_col in df_ts.columns:
                    st.altair_chart(
                        line_chart(
                            df_ts.dropna(subset=["year", roll_col]),
                            x="year",
                            y=roll_col,
                            title=f"How does realized frequency look after {roll_window}y smoothing?",
                            y_title="NOAA events (rolling)",
                            height=240,
                        ),
                        use_container_width=True,
                    )
            roll_col = f"fema_regs_roll{roll_window}"
            with detail_c2:
                if roll_col in df_ts.columns:
                    st.altair_chart(
                        line_chart(
                            df_ts.dropna(subset=["year", roll_col]),
                            x="year",
                            y=roll_col,
                            title=f"How do realized outcomes look after {roll_window}y smoothing?",
                            y_title="Registrations (rolling)",
                            height=240,
                        ),
                        use_container_width=True,
                    )

        with st.expander("Secondary diagnostics", expanded=False):
            st.caption("These diagnostics support interpretation, but they are intentionally secondary to the primary trend read.")
            if "claims_per_event" in df_ts.columns:
                st.altair_chart(
                    line_chart(
                        df_ts.dropna(subset=["year", "claims_per_event"]),
                        x="year",
                        y="claims_per_event",
                        title="Did claim intensity rise faster than event volume?",
                        y_title="Registrations / event",
                        height=240,
                    ),
                    use_container_width=True,
                )

            roll_col = f"fema_damage_roll{roll_window}"
            diag_c1, diag_c2 = st.columns(2)
            with diag_c1:
                st.altair_chart(
                    line_chart(
                        df_ts.dropna(subset=["year", "fema_total_damage"]),
                        x="year",
                        y="fema_total_damage",
                        title="When did realized damage accelerate?",
                        y_title="Total damage",
                        height=240,
                    ),
                    use_container_width=True,
                )
            with diag_c2:
                if roll_col in df_ts.columns:
                    st.altair_chart(
                        line_chart(
                            df_ts.dropna(subset=["year", roll_col]),
                            x="year",
                            y=roll_col,
                            title=f"How does realized damage look after {roll_window}y smoothing?",
                            y_title="Damage (rolling)",
                            height=240,
                        ),
                        use_container_width=True,
                    )

        with st.expander("Raw county time series (table)", expanded=False):
            show_table(df_ts, column_config=dataframe_year_config())

        st.divider()

        with st.expander("Hazard breakdown (selected county + year)", expanded=False):
            st.caption("Observed NOAA hazard composition for the selected county/year. Start with event volume, then use the table for supporting metrics.")
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

            for c in ["event_count", "total_fatalities", "total_injuries", "avg_property_damage"]:
                if c in df_h.columns:
                    df_h[c] = pd.to_numeric(df_h[c], errors="coerce")

            if show_query_stats:
                st.caption(format_scan_runtime(scanned_h, exec_ms_h))

            if df_h.empty:
                render_notice(
                    "No Hazard Rows for This Slice",
                    "This county/year can legitimately be quiet with 0 realized events even when the structural baseline is elevated.",
                )
            else:
                if {"hazard_type", "event_count"}.issubset(df_h.columns):
                    st.altair_chart(
                        bar_chart(
                            df_h.dropna(subset=["hazard_type", "event_count"]),
                            x="hazard_type",
                            y="event_count",
                            title="Which hazard types drove the most observed events?",
                            x_title="Hazard type",
                            y_title="Event count",
                        ),
                        use_container_width=True,
                    )
            show_table(df_h)

# ============================================================
# Year View (All Counties)
# ============================================================
elif page == "Year View (All Counties)":
    if not st.session_state["has_run"]:
        render_panel_label("Decision Surface")
        st.markdown("## Year View (All Counties)")
        render_notice("Ready to Run", "Set filters in the sidebar, then click `Run Explorer` to populate Year View.")
        st.stop()
    else:
        render_panel_label("Decision Surface")
        st.markdown("## Year View (All Counties)")
        st.caption("Single-year ranking view. Lead with the ranked map, then open supporting anomaly tables only when needed.")
        st.metric("Selected year", str(year))

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

        render_kpi_cards(
            [
                ("% counties with NOAA=0", f"{pct_zero:.2f}%" if pct_zero is not None else "—"),
                ("Median NRI (NOAA=0)", f"{med_zero:.2f}" if med_zero is not None else "—"),
                ("Median NRI (NOAA>0)", f"{med_pos:.2f}" if med_pos is not None else "—"),
            ]
        )
    else:
        pct_zero = None

    st.divider()
    section_intro("Ranking Mode", "Switch between structural and realized ranking logic without changing the underlying Gold contract.")
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
        rank_explain = "Ranks by NRI, the structural baseline that is often static across years."
        rank_col = "nri_risk_score"
    elif rank_mode.startswith("Realized frequency"):
        base_sql = sql_ranked_risk_realized_frequency(year=year, limit=1000)
        rank_explain = "Ranks by NOAA event count, the realized annual frequency."
        rank_col = "noaa_event_count"
    elif rank_mode.startswith("Realized outcomes"):
        base_sql = sql_ranked_risk_realized_claims(year=year, limit=1000)
        rank_explain = "Ranks by FEMA registrations, a realized annual outcomes proxy."
        rank_col = "fema_valid_registrations"
    else:
        base_sql = sql_ranked_risk_realized_impact(year=year, limit=1000)
        rank_explain = "Ranks by FEMA total damage, a realized annual impact proxy."
        rank_col = "fema_total_damage"

    render_notice("Ranking Logic", rank_explain)
    if pct_zero is not None:
        st.caption(
            f"Signature read: {pct_zero:.2f}% of counties in {year} have 0 realized NOAA events, so the map is most useful for separating quiet-year realizations from structural baseline risk."
        )

    col_left, col_right = st.columns([0.85, 1.55])

    with col_left:
        render_panel_label("Secondary Diagnostics")
        st.markdown("### Supporting diagnostics")
        st.caption("These panels are secondary. Use them after the ranked map/table identifies an area worth investigating.")

        with st.expander("Top claim intensity (registrations per event)", expanded=False):
            st.caption("Use this to spot counties where observed registrations look unusually high relative to realized event count.")
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
            show_table(df_top, column_config=dataframe_year_config())

        with st.expander("NOAA=0 but high structural risk (NRI)", expanded=False):
            st.caption("Use this anomaly table to isolate structurally risky counties that stayed quiet in realized NOAA counts.")
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

            if df_n0.empty:
                render_notice(
                    "No NOAA=0 High-NRI Rows",
                    "No rows matched this signature for the selected year. That is unusual, but possible.",
                )
            else:
                show_table(df_n0, column_config=dataframe_year_config())

    with col_right:
        render_panel_label("Primary Ranking Module")
        st.markdown("### Primary ranking view")
        st.caption("This is the main decision surface. Use the controls below to rank counties, then inspect the map before scanning the table.")
        controls_a, controls_b = st.columns(2)
        with controls_a:
            map_mode = st.radio(
                "Map mode",
                ["Show map (requires county centroids view)", "Table only"],
                index=0,
                horizontal=True,
                key="map_mode",
            )

            metric = st.radio(
                "Map color metric",
                ["nri_risk_score", "noaa_event_count", "fema_valid_registrations", "fema_total_damage"],
                index=0,
                key="map_metric",
            )
        with controls_b:
            size_by = st.radio(
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
                render_notice(
                    "Map Fallback",
                    "County centroids could not be loaded for mapping, so the view has fallen back to table-only mode.",
                    tone="warning",
                )
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
        else:
            zero_pct = None

        # Apply filters / top-N
        df_view = df_r.copy()
        if only_noaa0 and "noaa_event_count" in df_view.columns:
            df_view = df_view[df_view["noaa_event_count"].fillna(0).astype(float) == 0.0]
        df_view = df_view.head(int(top_n))

        top_summary = []
        if not df_view.empty:
            lead = df_view.iloc[0].to_dict()
            lead_county = str(lead.get("county_name") or lead.get("county_fips") or "Top-ranked county")
            lead_state = str(lead.get("state") or "").strip()
            place = f"{lead_county}, {lead_state}" if lead_state else lead_county
            lead_value = lead.get(rank_col)
            if pd.notna(lead_value):
                top_summary.append(f"Top-ranked county is {place} with {rank_col} = {float(lead_value):.2f}.")
            else:
                top_summary.append(f"Top-ranked county is {place}.")
            if zero_pct is not None:
                top_summary.append(f"{zero_pct:.2f}% of visible counties have zero realized NOAA events in this year.")
            if only_noaa0:
                top_summary.append("The current view is filtered to NOAA=0 counties only.")
        render_insight_block(
            "Year Signature",
            "This summary should orient the ranking read before you inspect the map or open the supporting diagnostics.",
            top_summary or ["No ranked county rows were returned for the current ranking mode."],
        )

        render_hero_insight(
            "Map First",
            "Use the map to identify pattern, then use the table to verify exact ordering",
            "The controls define the ranking lens, but the map is the fastest way to understand distribution and outliers.",
            [
                f"Ranking basis: {rank_col}",
                f"Color encoding: {metric}",
                f"Size encoding: {size_by}",
            ],
        )

        # Map rendering (pydeck) if we have lat/lon
        if want_map and {"lat", "lon"}.issubset(df_view.columns):
            df_map = df_view.dropna(subset=["lat", "lon"]).copy()
            df_map["lat"] = pd.to_numeric(df_map["lat"], errors="coerce")
            df_map["lon"] = pd.to_numeric(df_map["lon"], errors="coerce")
            df_map = df_map.dropna(subset=["lat", "lon"])

            # Hard cap for performance
            df_map = df_map.head(int(map_points_cap))

            if df_map.empty:
                render_notice(
                    "No Mappable Points",
                    "After filters were applied, no county centroid coordinates remained to render on the map.",
                    tone="warning",
                )
            else:
                # Build size scalar
                if size_by == "(constant)":
                    df_map["_radius"] = 25000.0
                else:
                    sraw = pd.to_numeric(df_map.get(size_by), errors="coerce").fillna(0.0).clip(lower=0.0)
                    smax = float(sraw.max()) if len(sraw) else 0.0
                    df_map["_radius"] = 25000.0 if smax <= 0 else (2000.0 + 38000.0 * (sraw / smax)).astype(float)

                # Build color scalar using the app's default red-forward palette.
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
                        get_fill_color="[255, 165, 0, 220]",
                        pickable=True,
                        stroked=True,
                        get_line_color="[0, 0, 0, 220]",
                        line_width_min_pixels=2,
                    )
                    highlight_layers.append(highlight_layer)

                view_state = pdk.ViewState(latitude=39.5, longitude=-98.35, zoom=3, pitch=0)

                st.markdown("#### Map (county centroids)")
                st.caption(
                    f"Primary visual: counties are ranked by {rank_col}, colored by {metric}, and sized by {size_by}. Points shown: {len(df_map)} (cap={map_points_cap})."
                )
                render_map_color_scale(metric, df_map.get(metric, pd.Series(dtype=float)))
                st.pydeck_chart(pdk.Deck(layers=highlight_layers, initial_view_state=view_state, tooltip=tooltip))

        with st.expander(f"Ranked table (top 50 by {rank_col})", expanded=True):
            st.caption("Use this table to confirm the exact ordering after the map reveals the broad pattern.")
            show_table(df_view.head(50), column_config=dataframe_year_config())

            csv = df_view.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download CSV",
                data=csv,
                file_name=f"ranked_{rank_mode.split('(')[0].strip().replace(' ', '_').lower()}_{year}.csv",
                mime="text/csv",
            )

st.divider()
with st.expander("Data sources used (Gold `_current` views)", expanded=False):
    st.markdown(
        """
- `gold_hazard.risk_feature_mart_current`
- `gold_hazard.hazard_event_summary_current`
- `gold_hazard.county_centroids_current`
"""
    )

section_intro("Platform Guarantees", "The UI is polished, but the operational contract remains the same: stable views, bounded scans, and deterministic promotion behavior.")
st.markdown(
    """
- Reads from Gold `_current` views (stable downstream contract)
- Partition-aware queries (Year filter reduces scanned data and cost)
- Deterministic build → validate → promote pipeline supports stable analytics
- Hard-fail behavior (no silent data corruption)
"""
)

render_notice(
    "Demo Status",
    "This demo reads from Gold `_current` views and stays stable across rebuilds.",
    tone="success",
)
