from __future__ import annotations

import time
from typing import Any, Dict, Optional, Tuple

import boto3

from agents.base import AgentContext, BaseAgent


class CatalogAgent(BaseAgent):
    """
    Cross-layer CatalogAgent.
    Ensures Glue tables/partitions are correct after each successful write.
    """

    def __init__(self, state=None, region_name: Optional[str] = None) -> None:
        super().__init__(state=state)
        self.glue = boto3.client("glue", region_name=region_name)

    def ensure_table_exists(
        self,
        ctx: AgentContext,
        database: str,
        table: str,
        *,
        hard_fail: bool = True,
        expected_s3_location_prefix: Optional[str] = None,
    ) -> bool:
        try:
            resp = self.glue.get_table(DatabaseName=database, Name=table)
            location = resp["Table"]["StorageDescriptor"].get("Location")

            ok = True
            if expected_s3_location_prefix and location:
                ok = location.startswith(expected_s3_location_prefix)

            self.log(
                ctx,
                "catalog_table_check",
                {"database": database, "table": table, "exists": True, "location": location, "location_ok": ok},
            )

            if expected_s3_location_prefix and not ok and hard_fail:
                raise RuntimeError(
                    f"Glue table location mismatch for {database}.{table}: {location} != {expected_s3_location_prefix}"
                )

            return True

        except self.glue.exceptions.EntityNotFoundException:
            self.log(ctx, "catalog_table_check", {"database": database, "table": table, "exists": False})
            if hard_fail:
                raise RuntimeError(f"Glue table missing: {database}.{table}")
            return False

    def _get_crawler_state(self, crawler_name: str) -> Tuple[str, Optional[str], Dict[str, Any]]:
        resp = self.glue.get_crawler(Name=crawler_name)
        crawler = resp["Crawler"]
        state = crawler.get("State") or "UNKNOWN"
        last = crawler.get("LastCrawl") or {}
        last_status = last.get("Status")
        return state, last_status, crawler

    def start_crawler_if_needed(self, ctx: AgentContext, crawler_name: str) -> str:
        """
        Don't start crawlers unless needed:
        - If already RUNNING/STOPPING -> no-op
        - If READY and last crawl SUCCEEDED -> no-op
        - Else start
        """
        state, last_status, _ = self._get_crawler_state(crawler_name)

        if state in ("RUNNING", "STOPPING"):
            self.log(ctx, "catalog_crawler_start", {"crawler": crawler_name, "note": f"no_op_state={state}"})
            return crawler_name

        if state == "READY" and last_status == "SUCCEEDED":
            self.log(ctx, "catalog_crawler_start", {"crawler": crawler_name, "note": "no_op_already_succeeded"})
            return crawler_name

        def _op():
            self.glue.start_crawler(Name=crawler_name)
            return crawler_name

        try:
            out = self.with_retry(ctx, f"start_crawler_{crawler_name}", _op, max_attempts=3, base_delay_seconds=2.0)
            self.log(ctx, "catalog_crawler_start", {"crawler": crawler_name, "note": "started"})
            return out
        except self.glue.exceptions.CrawlerRunningException:
            self.log(ctx, "catalog_crawler_start", {"crawler": crawler_name, "note": "already_running"})
            return crawler_name

    def wait_for_crawler(
        self,
        ctx: AgentContext,
        crawler_name: str,
        poll_seconds: int = 10,
        timeout_seconds: int = 60 * 30,
        stopping_grace_seconds: int = 90,
    ) -> str:
        """
        Strong wait semantics:
        - RUNNING  -> wait until READY (or timeout)
        - READY    -> success unless LastCrawl FAILED/CANCELLED
        - STOPPING -> if LastCrawl SUCCEEDED, treat as success; else allow grace then fail
        - Fail fast if LastCrawl FAILED/CANCELLED observed anytime
        """
        start = time.time()
        first_stopping_at: Optional[float] = None

        while True:
            state, last_status, crawler = self._get_crawler_state(crawler_name)

            # log safe: only primitives
            self.log(
                ctx,
                "catalog_crawler_poll",
                {"crawler": crawler_name, "state": state, "last_status": last_status},
            )

            # fail fast on explicit failure statuses
            if last_status in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"Crawler failed: {crawler_name} (LastCrawl.Status={last_status})")

            if state == "READY":
                return "READY"

            if state == "STOPPING":
                if last_status == "SUCCEEDED":
                    # treat as success (crawler is effectively done, AWS is just transitioning state)
                    return "READY"

                if first_stopping_at is None:
                    first_stopping_at = time.time()

                if time.time() - first_stopping_at > stopping_grace_seconds:
                    raise RuntimeError(
                        f"Crawler stuck STOPPING without SUCCEEDED: {crawler_name} (LastCrawl.Status={last_status})"
                    )

            if time.time() - start > timeout_seconds:
                raise TimeoutError(f"Crawler {crawler_name} timed out after {timeout_seconds}s")

            time.sleep(poll_seconds)
