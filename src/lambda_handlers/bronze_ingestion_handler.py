from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from agents.base import AgentContext
from agents.bronze_ingestion_agent import BronzeIngestionAgent
from ops.config import ATHENA_DB_BRONZE


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

    job_map = event.get("bronze_job_map") or json.loads(
        os.environ.get(
            "BRONZE_JOB_MAP_JSON",
            '{"noaa":"bronze_ingest_noaa","fema":"bronze_ingest_fema","nri":"bronze_ingest_nri","census":"bronze_ingest_census"}',
        )
    )

    agent = BronzeIngestionAgent()
    outputs = {"run_id": run_id, "dag": dag, "mode": mode, "run_dt": run_dt, "datasets": []}

    bronze_db = event.get("athena_db_bronze") or ATHENA_DB_BRONZE

    for ds in datasets:
        ctx = AgentContext(run_id=run_id, dag=dag, dataset=ds, layer="bronze", mode=mode)

        readiness_spec = (event.get("readiness_specs") or {}).get(ds, {"type": "implicit"})
        agent.check_source_freshness(ctx, ds, readiness_spec)

        plan = agent.build_ingestion_plan(ctx, ds, run_dt, mode=mode)

        glue_job = job_map[ds]
        ingest_result = agent.run_bronze_ingestion(ctx, ds, glue_job, plan)

        checks = ((event.get("bronze_validation_checks") or {}).get(ds)) or {}
        suite_name = f"validate_bronze_{ds}"
        quality_report = agent.validate_bronze(ctx, ds, database=bronze_db, suite_name=suite_name, checks=checks)

        crawlers = ((event.get("bronze_crawlers") or {}).get(ds)) or []
        ensure_tables = ((event.get("bronze_tables") or {}).get(ds)) or []
        catalog_result = agent.sync_glue_catalog_bronze(ctx, ds, crawlers=crawlers, ensure_tables=ensure_tables)

        meta = agent.record_bronze_run_metadata(
            ctx,
            ds,
            run_dt=run_dt,
            plan=plan,
            ingestion_result=ingest_result,
            quality_report=quality_report,
            catalog_result=catalog_result,
        )

        outputs["datasets"].append(
            {
                "dataset": ds,
                "plan_uri": plan.get("plan_uri"),
                "quality_report_uri": quality_report.get("report_uri"),
                "metadata_uri": meta.get("uri"),
                "glue_job_run_id": ingest_result.get("job_run_id"),
                "quality_status": quality_report.get("status"),
                "block_downstream": quality_report.get("block_downstream"),
            }
        )

    summary_uri = agent.summarize(
        AgentContext(run_id=run_id, dag=dag, dataset=None, layer="bronze", mode=mode),
        agent="IngestionAgent",
        summary={"status": "SUCCESS", "run_dt": run_dt, "datasets": outputs["datasets"]},
    )
    outputs["agent_summary_uri"] = summary_uri
    return outputs
