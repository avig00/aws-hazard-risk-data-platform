from __future__ import annotations

from typing import Any, Dict, Optional

from agents.base import BaseAgent, AgentContext
from aws.glue import GlueClient


class TransformAgent(BaseAgent):
    """
    TransformAgent (Silver), per dataset.
    """

    def __init__(self, state=None) -> None:
        super().__init__(state=state)
        self.glue = GlueClient()

    def precheck_bronze_health(self, ctx: AgentContext, dataset: str, latest_bronze_quality: Dict[str, Any]) -> Dict[str, Any]:
        ctx.dataset = dataset
        ctx.layer = "silver"

        block = bool(latest_bronze_quality.get("block_downstream"))
        self.log(ctx, "silver_precheck_bronze_health", {"dataset": dataset, "block": block, "bronze_status": latest_bronze_quality.get("status")})

        if block:
            raise RuntimeError(f"Blocking Bronze failures for dataset={dataset}. Silver transform blocked.")
        return {"dataset": dataset, "blocked": False}

    def build_transform_plan(
        self,
        ctx: AgentContext,
        dataset: str,
        *,
        run_dt: str,
        partitions: Optional[list[str]] = None,
        schema_contract_version: Optional[str] = None,
        extra_args: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        ctx.dataset = dataset
        ctx.layer = "silver"

        plan = {
            "dataset": dataset,
            "layer": "silver",
            "run_dt": run_dt,
            "partitions": partitions or [run_dt],
            "schema_contract_version": schema_contract_version,
            "glue_args": extra_args or {},
        }

        uri = self.state.write_run_plan(ctx.run_id, "silver", f"transform_plan_{dataset}", plan)
        self.log(ctx, "silver_transform_plan_built", {"dataset": dataset, "plan_uri": uri})
        return {**plan, "plan_uri": uri}

    def run_silver_transform(
        self,
        ctx: AgentContext,
        dataset: str,
        glue_job_name: str,
        plan: Dict[str, Any],
        *,
        timeout_seconds: int = 7200,
    ) -> Dict[str, Any]:
        ctx.dataset = dataset
        ctx.layer = "silver"

        args = dict(plan.get("glue_args", {}))
        args.update({"--RUN_DT": plan["run_dt"], "--DATASET": dataset})

        def _op():
            return self.glue.start_job(glue_job_name, arguments=args)

        job_run_id = self.with_retry(ctx, f"start_glue_silver_{dataset}", _op, max_attempts=3, base_delay_seconds=3.0)
        self.log(ctx, "silver_job_started", {"dataset": dataset, "glue_job": glue_job_name, "job_run_id": job_run_id, "args": args})

        result = self.glue.wait(glue_job_name, job_run_id, timeout_seconds=timeout_seconds)
        self.log(ctx, "silver_job_finished", {"dataset": dataset, "glue_job": glue_job_name, "job_run_id": job_run_id, "state": result.state})

        if result.state != "SUCCEEDED":
            raise RuntimeError(f"Silver Glue job failed: dataset={dataset} job={glue_job_name} run={job_run_id} state={result.state}")

        return {"dataset": dataset, "glue_job": glue_job_name, "job_run_id": job_run_id, "state": result.state}

    def record_silver_run_metadata(
        self,
        ctx: AgentContext,
        dataset: str,
        *,
        run_dt: str,
        plan: Dict[str, Any],
        transform_result: Dict[str, Any],
        quality_report: Dict[str, Any],
        catalog_result: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        ctx.dataset = dataset
        ctx.layer = "silver"

        meta = {
            "dataset": dataset,
            "layer": "silver",
            "run_dt": run_dt,
            "plan_uri": plan.get("plan_uri"),
            "transform": transform_result,
            "quality": {
                "status": quality_report.get("status"),
                "block_downstream": quality_report.get("block_downstream"),
                "report_uri": quality_report.get("report_uri"),
                "suite": quality_report.get("suite"),
            },
            "catalog": catalog_result or {},
        }

        uri = self.state.write_run_metadata(ctx.run_id, "silver", f"run_metadata_{dataset}", meta)
        self.log(ctx, "silver_run_metadata_written", {"dataset": dataset, "uri": uri})
        return {"dataset": dataset, "uri": uri}
