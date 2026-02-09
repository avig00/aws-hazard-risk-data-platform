import json
import boto3
from typing import Any, Dict, List

class S3Client:
    def __init__(self, region_name: str = None):
        self.client = boto3.client("s3", region_name=region_name)

    def put_json(self, bucket: str, key: str, payload: Dict[str, Any]) -> None:
        self.client.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode("utf-8"))

    def put_jsonl(self, bucket: str, key: str, rows: List[Dict[str, Any]]) -> None:
        body = "\n".join(json.dumps(r) for r in rows) + "\n"
        self.client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))

    def head_prefix_exists(self, bucket: str, prefix: str) -> bool:
        resp = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return "Contents" in resp
