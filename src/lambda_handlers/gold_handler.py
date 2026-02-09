from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from agents.base import AgentContext
from agents.catalog_agent import CatalogAgent
from agents.gold_mart_agent import GoldMartAgent
from agents.quality_agent import QualityAgent
from ops.config import ATHENA_DB_GOLD, PLATFORM_S3_BUCKET
from ops.sql_templates import load_sql_file, render_sql


def _now_yyyymmdd() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def handler(event: Dict[str, Any], aws_context) -> Dict[str, Any]:
    run_id = event.get("run_id") or getattr(aws_context, "aws_request_id", "unknown")
    dag = event.get("dag") or os.environ.get("PIPELINE_NAME", "hazard-risk-agent-controller")
    mode = event.get("mode", "monthly")
    run_dt = event.get("run_dt") or _now_yyyymmdd()

    gold_agent = GoldMartAgent()
    quality_agent = QualityAgent(state=gold_agent.state)
    catalog_agent = CatalogAgent(state=gold_agent.state)

    ctx = AgentContext(run_id=run_id, dag=dag, dataset=None, layer="gold", mode=mode)

    gold_db = event.get("athena_db_gold") or ATHENA_DB_GOLD

    # 1) precheck silver health (recommended: pass from previous step output)
    required_inputs = event.get("silver_required_inputs") or []
    gold_agent.precheck_silver_health(ctx, required_inputs)

    # 2) plan
    marts = event.get("marts") or ["hazard_event_summary", "risk_feature_mart"]
    plan = gold_agent.build_gold_plan(
        ctx,
        marts=marts,
        run_dt=run_dt,
        rebuild_mode=event.get("rebuild_mode", "incremental"),
    )

    # Template vars for CTAS SQL files
    tmpl_vars = {
        "bucket": PLATFORM_S3_BUCKET,
        "run_ds": run_dt,                 # YYYY-MM-DD
        "run_ds_nodash": run_dt.replace("-", ""),  # YYYYMMDD
    }

    # SQL file map (build + update_current_view) per mart
    # These paths assume your repo has these files at these locations.
    sql_paths = {
        "hazard_event_summary": {
            "build": "src/sql/gold_ctas/build_hazard_event_summary.sql",
            "update_view": "src/sql/gold_ctas/update_hazard_event_summary_current_view.sql",
        },
        "risk_feature_mart": {
            "build": "src/sql/gold_ctas/build_risk_feature_mart.sql",
            "update_view": "src/sql/gold_ctas/update_risk_feature_mart_current_view.sql",
        },
    }

    builds: List[Dict[str, Any]] = []
    mart_reports: List[Dict[str, Any]] = []

    for mart in marts:
        if mart not in sql_paths:
            raise ValueError(f"Unsupported mart '{mart}'. Add it to sql_paths in gold_handler.py.")

        # 3/5) build mart (CTAS)
        build_tpl = load_sql_file(sql_paths[mart]["build"])
        build_sql = render_sql(build_tpl, tmpl_vars, strict=True)

        build_res = gold_agent.run_query(
            ctx,
            database=gold_db,
            sql=build_sql,
            name=f"build_{mart}",
        )
        builds.append(build_res)

        # Update "current" view to point to the versioned table created for this run
        view_tpl = load_sql_file(sql_paths[mart]["update_view"])
        view_sql = render_sql(view_tpl, tmpl_vars, strict=True)

        view_res = gold_agent.run_query(
            ctx,
            database=gold_db,
            sql=view_sql,
            name=f"update_{mart}_current_view",
        )
        builds.append(view_res)

        # 4/6) validate mart (checks may be provided; empty -> pass scaffold)
        checks = ((event.get("gold_validation_checks") or {}).get(mart)) or {}
        suite_name = f"validate_{mart}"
        q = quality_agent.validate(ctx, database=gold_db, suite_name=suite_name, checks=checks)

        mart_reports.append(
            {
                "mart": mart,
                "build_query_execution_id": build_res.get("query_execution_id"),
                "view_update_query_execution_id": view_res.get("query_execution_id"),
                "quality_status": q.get("status"),
                "block_downstream": q.get("block_downstream"),
                "quality_report_uri": q.get("report_uri"),
            }
        )

    # 7) catalog sync gold (optional)
    crawlers = event.get("gold_crawlers") or []
    ensure_tables = event.get("gold_tables") or []

    catalog_result: Dict[str, Any] = {"crawlers": [], "tables_checked": []}

    for c in crawlers:
        catalog_agent.start_crawler(ctx, c)
        catalog_agent.wait_for_crawler(ctx, c)
        catalog_result["crawlers"].append(c)

    for t in ensure_tables:
        ok = catalog_agent.ensure_table_exists(ctx, t["database"], t["table"], hard_fail=True)
        catalog_result["tables_checked"].append({**t, "exists": ok})

    # 8) publish gold health artifact
    overall_status = "pass"
    if any(r["quality_status"] == "fail" for r in mart_reports):
        overall_status = "fail"
    elif any(r["quality_status"] == "warn" for r in mart_reports):
        overall_status = "warn"

    health = gold_agent.publish_gold_health_artifact(
        ctx,
        overall_status=overall_status,
        mart_reports=mart_reports,
        known_issues=event.get("known_issues"),
    )

    # 9) record gold run metadata
    meta = gold_agent.record_gold_run_metadata(ctx, plan=plan, builds=builds, quality_summaries=mart_reports)

    summary_uri = gold_agent.summarize(
        ctx,
        agent="GoldMartAgent",
        summary={
            "status": "SUCCESS",
            "run_dt": run_dt,
            "overall_status": overall_status,
            "health_uri": health.get("uri"),
            "metadata_uri": meta.get("uri"),
            "marts": mart_reports,
        },
    )

    return {
        "run_id": run_id,
        "dag": dag,
        "mode": mode,
        "run_dt": run_dt,
        "plan_uri": plan.get("plan_uri"),
        "health_uri": health.get("uri"),
        "metadata_uri": meta.get("uri"),
        "catalog": catalog_result,
        "marts": mart_reports,
        "agent_summary_uri": summary_uri,
    }
