# src/ops/__init__.py
"""
Operational utilities (agentic ops foundation).

Includes:
- config.py: env var configuration shared across DAGs/agents
- state_store.py: writes run events + quality reports to S3
- validators.py: load SQL validation suites from disk
- validation_runner.py: executes validation queries in Athena and interprets results

Design principles:
- Deterministic + auditable (every check is a query execution with an ID)
- Append-only artifacts in S3 (easy to inspect, easy to consume downstream)
"""
from .config import (
    PLATFORM_S3_BUCKET,
    OPS_PREFIX,
    ATHENA_WORKGROUP,
    ATHENA_RESULTS_S3,
    ATHENA_DB_BRONZE,
    ATHENA_DB_SILVER,
    ATHENA_DB_GOLD,
)
from .state_store import StateStore
from .validators import load_suite
from .validation_runner import ValidationRunner, ValidationResult

__all__ = [
    "PLATFORM_S3_BUCKET",
    "OPS_PREFIX",
    "ATHENA_WORKGROUP",
    "ATHENA_RESULTS_S3",
    "ATHENA_DB_BRONZE",
    "ATHENA_DB_SILVER",
    "ATHENA_DB_GOLD",
    "StateStore",
    "load_suite",
    "ValidationRunner",
    "ValidationResult",
]
