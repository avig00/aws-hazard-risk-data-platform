from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Optional, TypeVar

from ops.state_store import StateStore

T = TypeVar("T")


@dataclass
class AgentContext:
    run_id: str
    dag: str
    dataset: Optional[str]
    layer: str
    mode: str


def _json_sanitize(obj: Any) -> Any:
    """
    Convert common non-JSON-serializable objects into JSON-safe equivalents.
    """
    if obj is None:
        return None

    if isinstance(obj, (str, int, float, bool)):
        return obj

    if isinstance(obj, Decimal):
        return float(obj)

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")

    if isinstance(obj, dict):
        return {str(k): _json_sanitize(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [_json_sanitize(v) for v in obj]

    # dataclasses
    try:
        return _json_sanitize(asdict(obj))  # type: ignore[arg-type]
    except Exception:
        pass

    return str(obj)


class BaseAgent:
    """
    Shared agent base:
    - structured logging to ops/state store
    - summary artifact writer
    - simple retry helper
    """

    def __init__(self, state: Optional[StateStore] = None) -> None:
        self.state = state or StateStore()

    def log(self, ctx: AgentContext, event_type: str, payload: Dict[str, Any], *, task: Optional[str] = None) -> str:
        """
        Writes one event JSON via StateStore.write_run_event(run_id, dag, event).
        Returns the S3 URI.
        """
        event: Dict[str, Any] = {
            # StateStore uses event["task"] to name the file; keep it stable.
            "task": task or event_type,
            "dataset": ctx.dataset,
            "layer": ctx.layer,
            "mode": ctx.mode,
            "event_type": event_type,
            "payload": payload,
        }

        event = _json_sanitize(event)
        return self.state.write_run_event(ctx.run_id, ctx.dag, event)

    def summarize(self, ctx: AgentContext, *, agent: str, summary: Dict[str, Any]) -> str:
        """
        Writes canonical agent summary to:
          <OPS_PREFIX>/run_id=<run_id>/agent=<agent>/summary.json
        Returns the S3 URI.
        """
        summary_payload = _json_sanitize(summary)
        uri = self.state.write_agent_summary(ctx.run_id, agent, summary_payload)

        # Also emit an event pointer (so the event stream shows where the summary is)
        self.log(
            ctx,
            "agent_summary",
            {"agent": agent, "summary_uri": uri},
            task=f"agent_summary_{agent}",
        )
        return uri

    def with_retry(
        self,
        ctx: AgentContext,
        name: str,
        fn: Callable[[], T],
        *,
        max_attempts: int = 3,
        base_delay_seconds: float = 1.0,
    ) -> T:
        last_err: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                return fn()
            except Exception as e:
                last_err = e
                self.log(
                    ctx,
                    "retry",
                    {"name": name, "attempt": attempt, "max_attempts": max_attempts, "error": repr(e)},
                    task=f"retry_{name}",
                )
                if attempt < max_attempts:
                    time.sleep(base_delay_seconds * attempt)

        assert last_err is not None
        raise last_err
