from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from agents.base import AgentContext
from agents.catalog_agent import CatalogAgent
from agents.quality_agent import QualityAgent
from agents.transform_agent import TransformAgent
from ops.config import ATHENA_DB_SILVER


def _now_yyyymmdd() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def handler(event: Dict[str, Any], aws_context) -> Dict[str, Any]:
    run_id = event.get("run_id") or getattr(aws_context, "aws_request_id", "unknown")
    dag = event.get("dag") or os.environ.get("PIPELINE_NAME", "hazard-risk-agent-controller")
    mode = event.get("mode", "monthly")
    run_dt = event.get("run_dt") or _now_yyyymmdd()

    datasets: List[str] = event.get("datasets") or json.loads(
        os.environ.get("DATASETS_JSON", '["noaa","fema","nri","census"]')
    )

    silver_job_map = event.get("silver_job_map") or json.loads(
        os.environ.get(
            "SILVER_JOB_MAP_JSON",
            '{"noaa":"silver_noaa_details_clean","fema_disaster":"silver_fema_disaster_declarations_clean","fema_claims":"silver_fema_claims_clean","nri":"silver_nri_counties_clean","census":"silver_census_clean"}',
        )
    )

    dataset_to_jobs = event.get("silver_jobs_by_dataset") or {
        "noaa": [silver_job_map["noaa"]],
        "fema": [silver_job_map["fema_disaster"], silver_job_map["fema_claims"]],
        "nri": [silver_job_map["nri"]],
        "census": [silver_job_map["census"]],
    }

    bronze_quality_by_dataset = event.get("bronze_quality_by_dataset") or {}

    transform_agent = TransformAgent()
    quality_agent = QualityAgent(state=transform_agent.state)
    catalog_agent = CatalogAgent(state=transform_agent.state)

    outputs = {"run_id": run_id, "dag": dag, "mode": mode, "run_dt": run_dt, "datasets": []}

    silver_db = event.get("athena_db_silver") or ATHENA_DB_SILVER

    for ds in datasets:
        ctx = AgentContext(run_id=run_id, dag=dag, dataset=ds, layer="silver", mode=mode)

        latest_bronze_quality = bronze_quality_by_dataset.get(ds, {"status": "unknown", "block_downstream": False})
        transform_agent.precheck_bronze_health(ctx, ds, latest_bronze_quality)

        plan = transform_agent.build_transform_plan(ctx, ds, run_dt=run_dt)

        job_results = []
        for job_name in dataset_to_jobs.get(ds, []):
            job_results.append(transform_agent.run_silver_transform(ctx, ds, job_name, plan))

        checks = ((event.get("silver_validation_checks") or {}).get(ds)) or {}
        suite_name = f"validate_silver_{ds}"
        quality_report = quality_agent.validate(ctx, database=silver_db, suite_name=suite_name, checks=checks)

        crawlers = ((event.get("silver_crawlers") or {}).get(ds)) or []
        ensure_tables = ((event.get("silver_tables") or {}).get(ds)) or []
        catalog_result: Dict[str, Any] = {"crawlers": [], "tables_checked": []}

        for c in crawlers:
            catalog_agent.start_crawler_if_needed(ctx, c)
            catalog_agent.wait_for_crawler(ctx, c)
            catalog_result["crawlers"].append(c)

        for t in ensure_tables:
            ok = catalog_agent.ensure_table_exists(ctx, t["database"], t["table"], hard_fail=True)
            catalog_result["tables_checked"].append({**t, "exists": ok})

        meta = transform_agent.record_silver_run_metadata(
            ctx,
            ds,
            run_dt=run_dt,
            plan=plan,
            transform_result={"jobs": job_results},
            quality_report=quality_report,
            catalog_result=catalog_result,
        )

        outputs["datasets"].append(
            {
                "dataset": ds,
                "plan_uri": plan.get("plan_uri"),
                "quality_report_uri": quality_report.get("report_uri"),
                "metadata_uri": meta.get("uri"),
                "quality_status": quality_report.get("status"),
                "block_downstream": quality_report.get("block_downstream"),
                "job_run_ids": [jr.get("job_run_id") for jr in job_results],
            }
        )

    summary_uri = transform_agent.summarize(
        AgentContext(run_id=run_id, dag=dag, dataset=None, layer="silver", mode=mode),
        agent="TransformAgent",
        summary={"status": "SUCCESS", "run_dt": run_dt, "datasets": outputs["datasets"]},
    )
    outputs["agent_summary_uri"] = summary_uri
    return outputs
