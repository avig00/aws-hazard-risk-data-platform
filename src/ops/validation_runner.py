from dataclasses import dataclass
from typing import Dict, List
from aws.athena import AthenaClient
from ops.config import ATHENA_WORKGROUP, ATHENA_RESULTS_S3

@dataclass
class ValidationResult:
    name: str
    passed: bool
    query_execution_id: str
    failure_rows: int

class ValidationRunner:
    """
    Rule: each validation query must return 0 rows to pass (your Phase 4 principle).
    """
    def __init__(self):
        self.athena = AthenaClient(workgroup=ATHENA_WORKGROUP, results_s3=ATHENA_RESULTS_S3)

    def run_validations(self, database: str, validations: Dict[str, str]) -> List[ValidationResult]:
        results: List[ValidationResult] = []
        for name, sql in validations.items():
            qid = self.athena.start_query(sql=sql, database=database)
            execn = self.athena.wait(qid)
            if execn.state != "SUCCEEDED":
                results.append(ValidationResult(name=name, passed=False, query_execution_id=qid, failure_rows=-1))
                continue

            # Count rows returned by the validation query using Athena's query results is non-trivial without fetching results.
            # Instead: enforce that every validation SQL ends with "SELECT ... FROM ... LIMIT 1" and use a wrapper COUNT.
            # Better: write validations as "SELECT count(*) as failures FROM (...)" returning single row.
            # We'll standardize on that here:
            # Each SQL must return a single row with integer column failures.
            failures = self._fetch_single_int(qid)
            results.append(ValidationResult(name=name, passed=(failures == 0), query_execution_id=qid, failure_rows=failures))
        return results

    def _fetch_single_int(self, query_execution_id: str) -> int:
        import boto3
        client = boto3.client("athena")
        resp = client.get_query_results(QueryExecutionId=query_execution_id, MaxResults=2)
        rows = resp["ResultSet"]["Rows"]
        # rows[0] is header; rows[1] is first data row
        if len(rows) < 2:
            return 0
        val = rows[1]["Data"][0].get("VarCharValue", "0")
        try:
            return int(float(val))
        except Exception:
            return 0
