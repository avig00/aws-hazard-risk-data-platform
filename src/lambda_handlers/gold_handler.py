from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3

from agents.base import AgentContext
from agents.catalog_agent import CatalogAgent
from agents.gold_mart_agent import GoldMartAgent
from agents.quality_agent import QualityAgent
from ops.config import ATHENA_DB_GOLD, PLATFORM_S3_BUCKET
from ops.sql_templates import load_sql_file, render_sql


def _now_yyyymmdd() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _versioned_table_name(mart: str, run_dt: str) -> str:
    return f"{mart}__{run_dt.replace('-', '')}"


def _delete_s3_prefix(bucket: str, prefix: str) -> int:
    """
    Delete all objects under s3://{bucket}/{prefix}.
    Returns number of deleted objects.
    """
    s3 = boto3.client("s3")
    deleted = 0

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        if not contents:
            continue

        # S3 DeleteObjects supports up to 1000 keys per request
        for i in range(0, len(contents), 1000):
            chunk = contents[i : i + 1000]
            resp = s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in chunk], "Quiet": True},
            )
            deleted += len(resp.get("Deleted", []))

    return deleted


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

    required_inputs = event.get("silver_required_inputs") or []
    gold_agent.precheck_silver_health(ctx, required_inputs)

    marts = event.get("marts") or ["hazard_event_summary", "risk_feature_mart"]
    plan = gold_agent.build_gold_plan(
        ctx,
        marts=marts,
        run_dt=run_dt,
        rebuild_mode=event.get("rebuild_mode", "incremental"),
    )

    tmpl_vars = {
        "bucket": PLATFORM_S3_BUCKET,
        "run_ds": run_dt,
        "run_ds_nodash": run_dt.replace("-", ""),
    }

    sql_paths = {
        "hazard_event_summary": {
            "define_view": "src/sql/gold_ctas/define_hazard_event_summary_view.sql",
            "build": "src/sql/gold_ctas/build_hazard_event_summary.sql",
            "update_view": "src/sql/gold_ctas/update_hazard_event_summary_current_view.sql",
        },
        "risk_feature_mart": {
            "define_view": "src/sql/gold_ctas/define_risk_feature_mart_view.sql",
            "build": "src/sql/gold_ctas/build_risk_feature_mart.sql",
            "update_view": "src/sql/gold_ctas/update_risk_feature_mart_current_view.sql",
        },
    }

    builds: List[Dict[str, Any]] = []
    mart_reports: List[Dict[str, Any]] = []

    for mart in marts:
        if mart not in sql_paths:
            raise ValueError(f"Unsupported mart '{mart}'. Add it to sql_paths in gold_handler.py.")

        define_tpl = load_sql_file(sql_paths[mart]["define_view"])
        define_sql = render_sql(define_tpl, tmpl_vars, strict=True)

        define_res = gold_agent.run_query(ctx, database=gold_db, sql=define_sql, name=f"define_{mart}_view")
        builds.append(define_res)

        # --- Idempotency: drop table + clear external_location prefix ---
        versioned_table = _versioned_table_name(mart, run_dt)

        drop_res = gold_agent.run_query(
            ctx,
            database=gold_db,
            sql=f"DROP TABLE IF EXISTS {versioned_table}",
            name=f"drop_{versioned_table}",
        )
        builds.append(drop_res)

        # Your CTAS writes to: s3://{bucket}/hazard/gold/{mart}/run_dt={run_dt}/
        # If that prefix already exists, Athena fails with HIVE_PATH_ALREADY_EXISTS.
        gold_prefix = f"hazard/gold/{mart}/run_dt={run_dt}/"
        deleted = _delete_s3_prefix(PLATFORM_S3_BUCKET, gold_prefix)
        gold_agent.log(ctx, "gold_ctas_prefix_cleared", {"mart": mart, "s3_prefix": gold_prefix, "deleted_objects": deleted})
        # --- end idempotency block ---

        build_tpl = load_sql_file(sql_paths[mart]["build"])
        build_sql = render_sql(build_tpl, tmpl_vars, strict=True)

        build_res = gold_agent.run_query(ctx, database=gold_db, sql=build_sql, name=f"build_{mart}")
        builds.append(build_res)

        view_tpl = load_sql_file(sql_paths[mart]["update_view"])
        view_sql = render_sql(view_tpl, tmpl_vars, strict=True)

        view_res = gold_agent.run_query(ctx, database=gold_db, sql=view_sql, name=f"update_{mart}_current_view")
        builds.append(view_res)

        checks = ((event.get("gold_validation_checks") or {}).get(mart)) or {}
        suite_name = f"validate_{mart}"
        q = quality_agent.validate(ctx, database=gold_db, suite_name=suite_name, checks=checks)

        mart_reports.append(
            {
                "mart": mart,
                "define_view_query_execution_id": define_res.get("query_execution_id"),
                "build_query_execution_id": build_res.get("query_execution_id"),
                "view_update_query_execution_id": view_res.get("query_execution_id"),
                "quality_status": q.get("status"),
                "block_downstream": q.get("block_downstream"),
                "quality_report_uri": q.get("report_uri"),
            }
        )

    crawlers = event.get("gold_crawlers") or []
    ensure_tables = event.get("gold_tables") or []

    catalog_result: Dict[str, Any] = {"crawlers": [], "tables_checked": []}

    for c in crawlers:
        catalog_agent.start_crawler_if_needed(ctx, c)
        catalog_agent.wait_for_crawler(ctx, c)
        catalog_result["crawlers"].append(c)

    for t in ensure_tables:
        ok = catalog_agent.ensure_table_exists(ctx, t["database"], t["table"], hard_fail=True)
        catalog_result["tables_checked"].append({**t, "exists": ok})

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
