import time
import boto3
from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class GlueRun:
    job_name: str
    run_id: str
    state: str

class GlueClient:
    def __init__(self, region_name: str = None):
        self.client = boto3.client("glue", region_name=region_name)

    def start_job(self, job_name: str, arguments: Optional[Dict[str, str]] = None) -> str:
        resp = self.client.start_job_run(JobName=job_name, Arguments=arguments or {})
        return resp["JobRunId"]

    def wait(self, job_name: str, run_id: str, poll_seconds: int = 10, timeout_seconds: int = 60 * 60) -> GlueRun:
        start = time.time()
        while True:
            resp = self.client.get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=False)
            state = resp["JobRun"]["JobRunState"]
            if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
                return GlueRun(job_name=job_name, run_id=run_id, state=state)
            if time.time() - start > timeout_seconds:
                raise TimeoutError(f"Glue job {job_name} run {run_id} timed out after {timeout_seconds}s")
            time.sleep(poll_seconds)
