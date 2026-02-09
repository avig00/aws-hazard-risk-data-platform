from __future__ import annotations

from typing import Any, Dict, List, Optional

import boto3

from agents.base import BaseAgent, AgentContext
from agents.catalog_agent import CatalogAgent
from agents.quality_agent import QualityAgent
from aws.glue import GlueClient


class BronzeIngestionAgent(BaseAgent):
    """
    IngestionAgent (Bronze), per dataset.

    1) check_source_freshness
    2) build_ingestion_plan -> run_plan.json
    3) run_bronze_ingestion (Glue)
    4) validate_bronze (QualityAgent) -> quality_report
    5) sync_glue_catalog_bronze (CatalogAgent)
    6) record_bronze_run_metadata -> run_metadata.json
    """

    def __init__(self, state=None, region_name: Optional[str] = None) -> None:
        super().__init__(state=state)
        self.glue = GlueClient()
        self.catalog = CatalogAgent(state=self.state, region_name=region_name)
        self.quality = QualityAgent(state=self.state)
        self._s3 = boto3.client("s3", region_name=region_name)

    def check_source_freshness(self, ctx: AgentContext, dataset: str, readiness_spec: Dict[str, Any]) -> Dict[str, Any]:
        ctx.dataset = dataset
        ctx.layer = "bronze"

        rtype = readiness_spec.get("type")
        if rtype == "s3_prefix":
            bucket = readiness_spec["bucket"]
            prefix = readiness_spec["prefix"]
            resp = self._s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            ready = resp.get("KeyCount", 0) > 0
            out = {"dataset": dataset, "ready": ready, "type": rtype, "bucket": bucket, "prefix": prefix}
        else:
            out = {"dataset": dataset, "ready": True, "type": rtype or "implicit"}

        self.log(ctx, "bronze_source_freshness", out)
        if not out["ready"]:
            raise RuntimeError(f"Source not ready for dataset={dataset}: {out}")
        return out

    def build_ingestion_plan(
        self,
        ctx: AgentContext,
        dataset: str,
        run_dt: str,
        *,
        mode: str = "monthly",
        partitions: Optional[List[str]] = None,
        full_refresh: bool = False,
        expected_schema_hash: Optional[str] = None,
        extra_args: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        ctx.dataset = dataset
        ctx.layer = "bronze"
        ctx.mode = mode

        plan = {
            "dataset": dataset,
            "layer": "bronze",
            "run_dt": run_dt,
            "mode": mode,
            "full_refresh": full_refresh,
            "partitions": partitions or [run_dt],
            "expected_schema_hash": expected_schema_hash,
            "glue_args": extra_args or {},
        }

        uri = self.state.write_run_plan(ctx.run_id, "bronze", f"ingestion_plan_{dataset}", plan)
        self.log(ctx, "bronze_ingestion_plan_built", {"dataset": dataset, "plan_uri": uri})
        return {**plan, "plan_uri": uri}

    def run_bronze_ingestion(
        self,
        ctx: AgentContext,
        dataset: str,
        glue_job_name: str,
        plan: Dict[str, Any],
        *,
        timeout_seconds: int = 3600,
    ) -> Dict[str, Any]:
        ctx.dataset = dataset
        ctx.layer = "bronze"

        args = dict(plan.get("glue_args", {}))
        args.update({"--RUN_DT": plan["run_dt"], "--DATASET": dataset})

        def _op():
            return self.glue.start_job(glue_job_name, arguments=args)

        job_run_id = self.with_retry(ctx, f"start_glue_bronze_{dataset}", _op, max_attempts=3, base_delay_seconds=2.0)
        self.log(ctx, "bronze_job_started", {"dataset": dataset, "glue_job": glue_job_name, "job_run_id": job_run_id, "args": args})

        result = self.glue.wait(glue_job_name, job_run_id, timeout_seconds=timeout_seconds)
        self.log(ctx, "bronze_job_finished", {"dataset": dataset, "glue_job": glue_job_name, "job_run_id": job_run_id, "state": result.state})

        if result.state != "SUCCEEDED":
            raise RuntimeError(f"Bronze Glue job failed: dataset={dataset} job={glue_job_name} run={job_run_id} state={result.state}")

        return {"dataset": dataset, "glue_job": glue_job_name, "job_run_id": job_run_id, "state": result.state}

    def validate_bronze(
        self,
        ctx: AgentContext,
        dataset: str,
        database: str,
        suite_name: str,
        checks: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        ctx.dataset = dataset
        ctx.layer = "bronze"
        return self.quality.validate(ctx, database=database, suite_name=suite_name, checks=checks)

    def sync_glue_catalog_bronze(
        self,
        ctx: AgentContext,
        dataset: str,
        *,
        crawlers: Optional[List[str]] = None,
        ensure_tables: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        ctx.dataset = dataset
        ctx.layer = "bronze"

        out: Dict[str, Any] = {"dataset": dataset, "crawlers": [], "tables_checked": []}

        if crawlers:
            for c in crawlers:
                self.catalog.start_crawler(ctx, c)
                self.catalog.wait_for_crawler(ctx, c)
                out["crawlers"].append(c)

        if ensure_tables:
            for t in ensure_tables:
                ok = self.catalog.ensure_table_exists(ctx, t["database"], t["table"], hard_fail=True)
                out["tables_checked"].append({**t, "exists": ok})

        return out

    def record_bronze_run_metadata(
        self,
        ctx: AgentContext,
        dataset: str,
        *,
        run_dt: str,
        plan: Dict[str, Any],
        ingestion_result: Dict[str, Any],
        quality_report: Dict[str, Any],
        catalog_result: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        ctx.dataset = dataset
        ctx.layer = "bronze"

        meta = {
            "dataset": dataset,
            "layer": "bronze",
            "run_dt": run_dt,
            "plan_uri": plan.get("plan_uri"),
            "ingestion": ingestion_result,
            "quality": {
                "status": quality_report.get("status"),
                "block_downstream": quality_report.get("block_downstream"),
                "report_uri": quality_report.get("report_uri"),
                "suite": quality_report.get("suite"),
            },
            "catalog": catalog_result or {},
        }

        uri = self.state.write_run_metadata(ctx.run_id, "bronze", f"run_metadata_{dataset}", meta)
        self.log(ctx, "bronze_run_metadata_written", {"dataset": dataset, "uri": uri})
        return {"dataset": dataset, "uri": uri}
