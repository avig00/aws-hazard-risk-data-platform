from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from aws.s3 import S3Client
from ops.config import PLATFORM_S3_BUCKET, OPS_PREFIX


class StateStore:
    """
    S3-based state store for Phase 5 (no DynamoDB).

    Design goals:
      - One JSON per task/event for idempotency/debugging
      - Standard artifacts for "agentic ops":
          - run plans
          - run metadata
          - quality reports
          - agent summaries
      - Stable, human-browsable S3 layout under OPS_PREFIX
    """

    def __init__(self, s3: Optional[S3Client] = None):
        self.s3 = s3 or S3Client()

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _base(self) -> str:
        # OPS_PREFIX may be "hazard/ops" or "hazard/ops/" — normalize.
        return OPS_PREFIX.strip("/")

    def _run_root(self, run_id: str) -> str:
        # Canonical run root: <OPS_PREFIX>/run_id=<run_id>/
        return f"{self._base()}/run_id={run_id}"

    # ---------------------------------------------------------------------
    # Event stream (per-task JSON objects)
    # ---------------------------------------------------------------------
    def write_run_event(self, run_id: str, dag: str, event: Dict[str, Any]) -> str:
        """
        Writes a single event JSON object for idempotency.

        Note: 'dag' is kept for backward compatibility; it's your orchestration name/pipeline name now.
        """
        task = event.get("task", "unknown")
        event_key = f"{self._run_root(run_id)}/events/dag={dag}/{task}.json"

        row = {
            "ts": self._now(),
            "run_id": run_id,
            "dag": dag,
            **event,
        }

        self.s3.put_json(PLATFORM_S3_BUCKET, event_key, row)
        return f"s3://{PLATFORM_S3_BUCKET}/{event_key}"

    # ---------------------------------------------------------------------
    # Plans / Metadata / Summaries
    # ---------------------------------------------------------------------
    def write_run_plan(self, run_id: str, layer: str, name: str, plan: Dict[str, Any]) -> str:
        key = f"{self._run_root(run_id)}/plans/layer={layer}/{name}.json"
        payload = {"ts": self._now(), "run_id": run_id, "layer": layer, "name": name, **plan}
        self.s3.put_json(PLATFORM_S3_BUCKET, key, payload)
        return f"s3://{PLATFORM_S3_BUCKET}/{key}"

    def write_run_metadata(self, run_id: str, layer: str, name: str, metadata: Dict[str, Any]) -> str:
        key = f"{self._run_root(run_id)}/metadata/layer={layer}/{name}.json"
        payload = {"ts": self._now(), "run_id": run_id, "layer": layer, "name": name, **metadata}
        self.s3.put_json(PLATFORM_S3_BUCKET, key, payload)
        return f"s3://{PLATFORM_S3_BUCKET}/{key}"

    def write_agent_summary(self, run_id: str, agent: str, summary: Dict[str, Any]) -> str:
        key = f"{self._run_root(run_id)}/agent={agent}/summary.json"
        payload = {"ts": self._now(), "run_id": run_id, "agent": agent, **summary}
        self.s3.put_json(PLATFORM_S3_BUCKET, key, payload)
        return f"s3://{PLATFORM_S3_BUCKET}/{key}"

    # ---------------------------------------------------------------------
    # Quality report (keep signature unchanged)
    # ---------------------------------------------------------------------
    def write_quality_report(self, run_id: str, layer: str, name: str, report: Dict[str, Any]) -> str:
        """
        Signature preserved.
        Canonical location:
          <OPS_PREFIX>/run_id=<run_id>/quality/layer=<layer>/<name>.json
        """
        key = f"{self._run_root(run_id)}/quality/layer={layer}/{name}.json"
        payload = {"ts": self._now(), "run_id": run_id, "layer": layer, "name": name, **report}
        self.s3.put_json(PLATFORM_S3_BUCKET, key, payload)
        return f"s3://{PLATFORM_S3_BUCKET}/{key}"
