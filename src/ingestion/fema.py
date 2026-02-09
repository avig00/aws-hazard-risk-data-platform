# src/ingestion/fema.py
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import boto3
import requests


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _download_to_tmp(url: str, tmp_path: Path, timeout: int = 240) -> Tuple[int, str]:
    """
    Stream-download a URL to tmp_path. Returns (bytes_written, content_type).
    Raises for HTTP errors.
    """
    tmp_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        content_type = r.headers.get("Content-Type", "application/octet-stream")

        bytes_written = 0
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    bytes_written += len(chunk)

    return bytes_written, content_type


def _s3_put_file(bucket: str, key: str, local_path: Path, content_type: str | None = None) -> None:
    s3 = boto3.client("s3")
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    s3.upload_file(
        Filename=str(local_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs=extra_args if extra_args else None,
    )


def ingest_fema(bucket: str, bronze_prefix: str, run_dt: str) -> Dict[str, Any]:
    """
    FEMA Bronze ingestion (raw bulk CSVs from OpenFEMA v2).

    Downloads:
      - DisasterDeclarationsSummaries.csv
      - HousingAssistanceOwners.csv
      - HousingAssistanceRenters.csv

    Writes each to S3 under:
      {bronze_prefix}/fema/<dataset_name>/run_dt={run_dt}/<file>.csv

    Returns a manifest of written objects.
    """
    sources = {
        "disaster_declarations": "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries.csv",
        "housing_assistance_owners": "https://www.fema.gov/api/open/v2/HousingAssistanceOwners.csv",
        "housing_assistance_renters": "https://www.fema.gov/api/open/v2/HousingAssistanceRenters.csv",
    }

    objects_written: List[str] = []
    downloads: List[Dict[str, Any]] = []

    for name, url in sources.items():
        local_path = Path("/tmp") / f"fema_{name}_{run_dt}.csv"
        bytes_written, content_type = _download_to_tmp(url, local_path, timeout=240)

        # Force a sane content type for Athena-friendly CSV
        ct = "text/csv"

        key = f"{bronze_prefix}/fema/{name}/run_dt={run_dt}/{name}.csv"
        _s3_put_file(bucket=bucket, key=key, local_path=local_path, content_type=ct)

        s3_uri = f"s3://{bucket}/{key}"
        objects_written.append(s3_uri)
        downloads.append(
            {
                "name": name,
                "source": url,
                "bytes": bytes_written,
                "s3_uri": s3_uri,
            }
        )

        # Best effort cleanup
        try:
            local_path.unlink(missing_ok=True)  # Python 3.8+
        except Exception:
            pass

    return {
        "dataset": "fema",
        "run_dt": run_dt,
        "ts": _utc_now_iso(),
        "mode": "bulk_csv_v2",
        "downloads": downloads,
        "objects_written": objects_written,
    }
