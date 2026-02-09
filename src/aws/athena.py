import time
import boto3
from dataclasses import dataclass
from typing import Optional

@dataclass
class AthenaExecution:
    query_execution_id: str
    state: str
    output_s3: Optional[str] = None

class AthenaClient:
    def __init__(self, workgroup: str, results_s3: str, region_name: str = None):
        self.client = boto3.client("athena", region_name=region_name)
        self.workgroup = workgroup
        self.results_s3 = results_s3

    def start_query(self, sql: str, database: str) -> str:
        resp = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": self.results_s3},
            WorkGroup=self.workgroup,
        )
        return resp["QueryExecutionId"]

    def wait(self, qid: str, poll_seconds: int = 3, timeout_seconds: int = 60 * 20) -> AthenaExecution:
        start = time.time()
        while True:
            resp = self.client.get_query_execution(QueryExecutionId=qid)
            qe = resp["QueryExecution"]
            state = qe["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                out = qe.get("ResultConfiguration", {}).get("OutputLocation")
                return AthenaExecution(query_execution_id=qid, state=state, output_s3=out)
            if time.time() - start > timeout_seconds:
                raise TimeoutError(f"Athena query {qid} timed out after {timeout_seconds}s")
            time.sleep(poll_seconds)
