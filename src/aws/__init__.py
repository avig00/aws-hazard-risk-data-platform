# src/aws/__init__.py
"""
AWS tool wrappers.

Purpose:
- Provide small, testable, deterministic wrappers around AWS service APIs.
- Keep agents focused on orchestration decisions, not boto3 details.

Modules:
- athena.py: start query, wait for completion
- glue.py: start job, wait for completion
- s3.py: write JSON/JSONL artifacts, check prefixes
"""
from .athena import AthenaClient, AthenaExecution
from .glue import GlueClient, GlueRun
from .s3 import S3Client

__all__ = ["AthenaClient", "AthenaExecution", "GlueClient", "GlueRun", "S3Client"]
