from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3

_s3 = boto3.client("s3")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def head_exists(bucket: str, key: str) -> bool:
    try:
        _s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def put_json(bucket: str, key: str, payload: Dict[str, Any]) -> None:
    _s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def upload_file(bucket: str, key: str, path: str, content_type: Optional[str] = None) -> None:
    extra = {}
    if content_type:
        extra["ContentType"] = content_type
    if extra:
        _s3.upload_file(path, bucket, key, ExtraArgs=extra)
    else:
        _s3.upload_file(path, bucket, key)


def bronze_key(bronze_prefix: str, dataset_path: str, run_dt: str, filename: str) -> str:
    """
    <bronze_prefix>/<dataset_path>/run_dt=YYYY-MM-DD/<filename>
    """
    return f"{bronze_prefix}/{dataset_path}/run_dt={run_dt}/{filename}"


def ops_key(ops_prefix: str, component: str, dataset: str, run_dt: str, filename: str) -> str:
    """
    <ops_prefix>/<component>/<dataset>/run_dt=YYYY-MM-DD/<filename>
    """
    return f"{ops_prefix}/{component}/{dataset}/run_dt={run_dt}/{filename}"


def stage_bytes_to_tmp(data: bytes, suffix: str) -> str:
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    with open(path, "wb") as f:
        f.write(data)
    return path
