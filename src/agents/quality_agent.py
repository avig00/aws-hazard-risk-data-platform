from __future__ import annotations

from typing import Any, Dict, List

from agents.base import BaseAgent, AgentContext
from ops.validation_runner import ValidationRunner


class QualityAgent(BaseAgent):
    """
    Cross-layer QualityAgent.
    Produces:
      - quality_report.json (pass/warn/fail)
      - block_downstream decision (True if any blocking failures)
    """

    def __init__(self, state=None) -> None:
        super().__init__(state=state)
        self.runner = ValidationRunner()

    def validate(
        self,
        ctx: AgentContext,
        database: str,
        suite_name: str,
        checks: Dict[str, Dict[str, Any]],
        *,
        default_severity: str = "blocking",
        raise_on_blocking_failure: bool = True,
    ) -> Dict[str, Any]:
        ctx.layer = ctx.layer or "unknown"

        if not checks:
            report = {
                "database": database,
                "suite": suite_name,
                "status": "pass",
                "block_downstream": False,
                "blocking_failures": 0,
                "non_blocking_failures": 0,
                "results": [],
                "note": "No checks provided; treated as pass.",
            }
            uri = self.state.write_quality_report(ctx.run_id, ctx.layer, suite_name, report)
            self.log(ctx, f"quality_end_{suite_name}", {"status": "pass", "block_downstream": False, "report_uri": uri})
            return {**report, "report_uri": uri}

        self.log(ctx, f"quality_start_{suite_name}", {"database": database, "count": len(checks)})

        sql_map = {name: spec["sql"] for name, spec in checks.items()}
        results = self.runner.run_validations(database=database, validations=sql_map)

        enriched: List[Dict[str, Any]] = []
        blocking_failures = 0
        non_blocking_failures = 0

        for r in results:
            spec = checks.get(r.name, {})
            severity = spec.get("severity", default_severity)
            desc = spec.get("description")

            row = {**r.__dict__, "severity": severity}
            if desc:
                row["description"] = desc
            enriched.append(row)

            if not r.passed:
                if severity == "blocking":
                    blocking_failures += 1
                else:
                    non_blocking_failures += 1

        passed_all = (blocking_failures == 0 and non_blocking_failures == 0)
        warn_only = (blocking_failures == 0 and non_blocking_failures > 0)

        status = "pass" if passed_all else ("warn" if warn_only else "fail")
        block_downstream = blocking_failures > 0

        report: Dict[str, Any] = {
            "database": database,
            "suite": suite_name,
            "status": status,
            "block_downstream": block_downstream,
            "blocking_failures": blocking_failures,
            "non_blocking_failures": non_blocking_failures,
            "results": enriched,
        }

        uri = self.state.write_quality_report(ctx.run_id, ctx.layer, suite_name, report)
        self.log(ctx, f"quality_end_{suite_name}", {"status": status, "block_downstream": block_downstream, "report_uri": uri})

        if raise_on_blocking_failure and block_downstream:
            raise RuntimeError(f"Blocking quality failures in suite={suite_name}. Report: {uri}")

        return {**report, "report_uri": uri}
