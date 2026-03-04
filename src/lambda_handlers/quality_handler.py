from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from agents.base import AgentContext
from agents.quality_agent import QualityAgent
from ops.config import (
    ATHENA_DB_BRONZE,
    ATHENA_DB_SILVER,
    ATHENA_DB_GOLD,
)
from ops.validators import load_checks


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def handler(event: Dict[str, Any], aws_context) -> Dict[str, Any]:
    """
    QualityAgent (Cross-layer)
    - Runs validation suites and emits pass/warn/fail
    - Labels failures as blocking vs non_blocking (via check severity)
    - Writes quality reports to ops S3

    Input supports:
      {
        "run_id": "...",
        "dag": "...",
        "mode": "monthly",
        "run_dt": "YYYY-MM-DD",

        "quality": {
          "runs": [
            {"layer": "bronze", "suite_name": "noaa_details", "sql_dir": "src/sql/validations/bronze", "database": "bronze_hazard_raw"},
            {"layer": "silver", "suite_name": "noaa_clean", "sql_dir": "src/sql/validations/silver", "database": "silver_hazard_cleaned"},
            {"layer": "gold",  "suite_name": "gold_marts", "sql_dir": "src/sql/validations/gold", "database": "gold_hazard"}
          ]
        }
      }

    If quality.runs is omitted, it runs sensible defaults based on your current sql folder layout.
    """

    run_id = event.get("run_id") or getattr(aws_context, "aws_request_id", "unknown")
    dag = event.get("dag") or "hazard-risk-agent-controller"
    mode = event.get("mode") or "monthly"
    run_dt = event.get("run_dt") or _today_utc()

    ctx = AgentContext(run_id=run_id, dag=dag, dataset=None, layer="quality", mode=mode)

    agent = QualityAgent()

    bronze_db = event.get("athena_db_bronze") or ATHENA_DB_BRONZE
    silver_db = event.get("athena_db_silver") or ATHENA_DB_SILVER
    gold_db = event.get("athena_db_gold") or ATHENA_DB_GOLD

    quality_cfg = event.get("quality") or {}
    runs: List[Dict[str, Any]] = quality_cfg.get("runs") or []

    # Default behavior: run the suites you currently have in the bundle.
    # (You can extend later without changing handler logic.)
    if not runs:
        runs = [
            {"layer": "bronze", "suite_name": "noaa_events_raw", "sql_dir": "src/sql/validations/bronze", "database": bronze_db},
            {"layer": "silver", "suite_name": "noaa_events_clean", "sql_dir": "src/sql/validations/silver", "database": silver_db},
            {"layer": "gold", "suite_name": "gold_marts", "sql_dir": "src/sql/validations/gold", "database": gold_db},
        ]

    reports: List[Dict[str, Any]] = []
    overall_status = "pass"
    block_downstream = False

    # Simple severity policy: by default, everything is blocking unless you override.
    # If you want per-check severities, pass a severities dict in the run item.
    for r in runs:
        layer = r.get("layer")
        suite_name = r.get("suite_name")
        sql_dir = r.get("sql_dir")
        database = r.get("database")

        if not layer or not suite_name or not sql_dir or not database:
            raise ValueError(f"Invalid quality run entry: {r}")

        severities = r.get("severities")  # optional dict
        descriptions = r.get("descriptions")  # optional dict

        checks = load_checks(
            sql_dir,
            default_severity="blocking",
            severities=severities,
            descriptions=descriptions,
            template_vars={
                "athena_db_bronze": bronze_db,
                "athena_db_silver": silver_db,
                "athena_db_gold": gold_db,
                "validation_table_hazard_event_summary": f"{gold_db}.hazard_event_summary_current",
                "validation_table_risk_feature_mart": f"{gold_db}.risk_feature_mart_current",
                "validation_table_county_year_universe": f"{gold_db}.county_year_universe",
            },
        )

        res = agent.validate(ctx, database=database, suite_name=f"{layer}__{suite_name}", checks=checks)

        reports.append(
            {
                "layer": layer,
                "suite_name": suite_name,
                "database": database,
                "status": res.get("status"),
                "block_downstream": res.get("block_downstream"),
                "report_uri": res.get("report_uri"),
            }
        )

        # Aggregate
        if res.get("status") == "fail":
            overall_status = "fail"
        elif res.get("status") == "warn" and overall_status != "fail":
            overall_status = "warn"

        if res.get("block_downstream"):
            block_downstream = True

    summary_uri = agent.summarize(
        ctx,
        agent="QualityAgent",
        summary={
            "status": "SUCCESS" if overall_status != "fail" else "FAILED",
            "run_dt": run_dt,
            "overall_status": overall_status,
            "block_downstream": block_downstream,
            "reports": reports,
        },
    )

    return {
        "run_id": run_id,
        "dag": dag,
        "mode": mode,
        "run_dt": run_dt,
        "overall_status": overall_status,
        "block_downstream": block_downstream,
        "reports": reports,
        "agent_summary_uri": summary_uri,
    }
