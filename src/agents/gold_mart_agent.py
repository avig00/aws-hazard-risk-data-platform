from __future__ import annotations

from typing import Any, Dict, List, Optional

from agents.base import BaseAgent, AgentContext
from aws.athena import AthenaClient
from ops.config import ATHENA_WORKGROUP, ATHENA_RESULTS_S3


class GoldMartAgent(BaseAgent):
    """
    GoldMartAgent (Gold)
    """

    def __init__(self, state=None) -> None:
        super().__init__(state=state)
        self.athena = AthenaClient(workgroup=ATHENA_WORKGROUP, results_s3=ATHENA_RESULTS_S3)

    def precheck_silver_health(self, ctx: AgentContext, required_inputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        ctx.layer = "gold"
        blocking = [x for x in required_inputs if x.get("block_downstream")]
        self.log(ctx, "gold_precheck_silver_health", {"required_count": len(required_inputs), "blocking_count": len(blocking)})
        if blocking:
            raise RuntimeError(f"Gold blocked due to blocking Silver inputs: {blocking}")
        return {"blocked": False}

    def build_gold_plan(
        self,
        ctx: AgentContext,
        *,
        marts: List[str],
        run_dt: str,
        rebuild_mode: str = "incremental",
        year_ranges: Optional[List[Dict[str, int]]] = None,
    ) -> Dict[str, Any]:
        ctx.layer = "gold"

        plan = {
            "layer": "gold",
            "run_dt": run_dt,
            "rebuild_mode": rebuild_mode,
            "marts": marts,
            "year_ranges": year_ranges or [],
        }

        uri = self.state.write_run_plan(ctx.run_id, "gold", "gold_plan", plan)
        self.log(ctx, "gold_plan_built", {"plan_uri": uri, "marts": marts, "rebuild_mode": rebuild_mode})
        return {**plan, "plan_uri": uri}

    def run_query(self, ctx: AgentContext, database: str, sql: str, name: str) -> Dict[str, Any]:
        ctx.layer = "gold"

        def _start():
            return self.athena.start_query(sql=sql, database=database)

        qid = self.with_retry(ctx, f"athena_start_{name}", _start, max_attempts=3, base_delay_seconds=2.0)
        self.log(ctx, "gold_query_started", {"name": name, "database": database, "query_execution_id": qid})

        execn = self.athena.wait(qid)
        self.log(ctx, "gold_query_finished", {"name": name, "database": database, "query_execution_id": qid, "state": execn.state})

        if execn.state != "SUCCEEDED":
            raise RuntimeError(f"Gold build failed for {name}. QueryExecutionId={qid} (state={execn.state})")

        return {"name": name, "query_execution_id": qid, "state": execn.state}

    def publish_gold_health_artifact(
        self,
        ctx: AgentContext,
        *,
        overall_status: str,
        mart_reports: List[Dict[str, Any]],
        known_issues: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        ctx.layer = "gold"

        artifact = {
            "layer": "gold",
            "overall_status": overall_status,
            "marts": mart_reports,
            "known_issues": known_issues or [],
        }

        uri = self.state.write_run_metadata(ctx.run_id, "gold", "gold_health", artifact)
        self.log(ctx, "gold_health_published", {"uri": uri, "overall_status": overall_status})
        return {"uri": uri, "status": overall_status}

    def record_gold_run_metadata(
        self,
        ctx: AgentContext,
        *,
        plan: Dict[str, Any],
        builds: List[Dict[str, Any]],
        quality_summaries: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        ctx.layer = "gold"

        meta = {
            "layer": "gold",
            "plan_uri": plan.get("plan_uri"),
            "builds": builds,
            "quality": quality_summaries,
        }

        uri = self.state.write_run_metadata(ctx.run_id, "gold", "run_metadata_gold", meta)
        self.log(ctx, "gold_run_metadata_written", {"uri": uri})
        return {"uri": uri}
