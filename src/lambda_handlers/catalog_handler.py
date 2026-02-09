from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from agents.base import AgentContext
from agents.catalog_agent import CatalogAgent
from ops.config import ATHENA_DB_BRONZE, ATHENA_DB_SILVER, ATHENA_DB_GOLD


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def handler(event: Dict[str, Any], aws_context) -> Dict[str, Any]:
    run_id = event.get("run_id") or getattr(aws_context, "aws_request_id", "unknown")
    dag = event.get("dag") or "hazard-risk-agent-controller"
    mode = event.get("mode") or "monthly"
    run_dt = event.get("run_dt") or _today_utc()

    ctx = AgentContext(run_id=run_id, dag=dag, dataset=None, layer="catalog", mode=mode)
    agent = CatalogAgent()

    catalog_cfg = event.get("catalog") or {}
    crawlers: List[str] = catalog_cfg.get("crawlers") or []
    ensure_tables: List[Dict[str, str]] = catalog_cfg.get("ensure_tables") or []

    db_bronze = event.get("athena_db_bronze") or ATHENA_DB_BRONZE
    db_silver = event.get("athena_db_silver") or ATHENA_DB_SILVER
    db_gold = event.get("athena_db_gold") or ATHENA_DB_GOLD

    # Defaults if nothing is passed
    if not crawlers and not ensure_tables:
        crawlers = [
            "bronze-noaa-details",
            "bronze-noaa-fatalities",
            "bronze-noaa-locations",
            "bronze-fema-disaster-declarations",
            "bronze-fema-housing-assistance-owners",
            "bronze-fema-housing-assistance-renters",
            "bronze-nri-counties",
            "bronze-census-acs5-2022-b01001",
            "bronze-census-acs5-2022-b15003",
            "bronze-census-acs5-2022-b23025",
            "bronze-census-acs5-2022-b19013",
            "bronze-census-acs5-2022-b25077",
            "silver-noaa-events-clean",
            "silver-fema-disaster-declarations-clean",
            "silver-fema-claims-clean",
            "silver-nri-scores-clean",
            "silver-census-clean",
        ]

        ensure_tables = [
            {"database": db_bronze, "table": "details"},
            {"database": db_bronze, "table": "fatalities"},
            {"database": db_bronze, "table": "locations"},
            {"database": db_silver, "table": "noaa_events_clean"},
            
            {"database": db_gold, "table": "hazard_event_summary"},
            {"database": db_gold, "table": "risk_feature_mart"},
        ]

    results: Dict[str, Any] = {"crawlers": [], "tables_checked": []}

    for crawler in crawlers:
        agent.start_crawler_if_needed(ctx, crawler)
        agent.wait_for_crawler(ctx, crawler)
        results["crawlers"].append(crawler)

    for t in ensure_tables:
        db = t.get("database")
        tbl = t.get("table")
        if not db or not tbl:
            raise ValueError(f"Invalid ensure_tables entry: {t}")

        ok = agent.ensure_table_exists(ctx, db, tbl, hard_fail=True)
        results["tables_checked"].append({"database": db, "table": tbl, "exists": ok})

    summary_uri = agent.summarize(
        ctx,
        agent="CatalogAgent",
        summary={
            "status": "SUCCESS",
            "run_dt": run_dt,
            "crawlers": results["crawlers"],
            "tables_checked": results["tables_checked"],
        },
    )

    return {
        "run_id": run_id,
        "dag": dag,
        "mode": mode,
        "run_dt": run_dt,
        "catalog": results,
        "agent_summary_uri": summary_uri,
    }
