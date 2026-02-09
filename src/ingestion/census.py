from __future__ import annotations

import csv
import os
import tempfile
from typing import Any, Dict, List

from ingestion.http import get_json
from ingestion.s3_io import bronze_key, head_exists, put_json, upload_file, sha256_file, utc_now_iso

CENSUS_BASE = "https://api.census.gov/data/2022/acs/acs5"

GROUP_TABLES = ["B01001", "B15003", "B23025"]
SINGLE_TABLES = {
    "B19013": ["B19013_001E"],
    "B25077": ["B25077_001E"],
}


def _write_rows(tmp_path: str, rows: List[List[Any]]) -> int:
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerows(rows)
    return max(0, len(rows) - 1)


def ingest_census(
    bucket: str,
    bronze_prefix: str,
    run_dt: str,
    timeout: int = 240,
) -> Dict[str, Dict]:
    """
    Ingest ACS 2022 5-year county:* tables into S3 Bronze.

    Output paths:
      hazard/bronze/census/acs5_2022_<TABLE>/run_dt=.../acs5_2022_<TABLE>_county.csv

    Returns:
      manifest keyed by output filename -> status/rows/s3_key
    """
    manifest: Dict[str, Dict] = {}

    def ingest_one(table: str, params: Dict[str, str], dataset_path: str, filename: str) -> None:
        s3_key = bronze_key(bronze_prefix, dataset_path, run_dt, filename)

        if head_exists(bucket, s3_key):
            manifest[filename] = {"status": "SKIPPED_EXISTS", "s3_key": s3_key}
            return

        payload = get_json(
            CENSUS_BASE,
            params=params,
            timeout=timeout,
            headers={"User-Agent": "aws-hazard-risk-agent/1.0"},
        )

        if not payload or len(payload) < 2:
            manifest[filename] = {"status": "FAILED", "error": "Empty Census payload", "s3_key": s3_key}
            return

        fd, tmp_path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        try:
            rows = _write_rows(tmp_path, payload)
            upload_file(bucket, s3_key, tmp_path, content_type="text/csv")

            meta_key = bronze_key(bronze_prefix, dataset_path, run_dt, f"{filename}.metadata.json")
            put_json(bucket, meta_key, {
                "source": "census",
                "asset": table,
                "run_dt": run_dt,
                "fetched_at": utc_now_iso(),
                "url": CENSUS_BASE,
                "params": params,
                "row_count": rows,
                "sha256": sha256_file(tmp_path),
                "s3_key": s3_key,
            })

            manifest[filename] = {"status": "OK", "rows": rows, "s3_key": s3_key}
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    for t in GROUP_TABLES:
        params = {"get": f"NAME,group({t})", "for": "county:*"}
        dataset_path = f"census/acs5_2022_{t}"
        filename = f"acs5_2022_{t}_county.csv"
        ingest_one(t, params, dataset_path, filename)

    for t, vars_ in SINGLE_TABLES.items():
        params = {"get": "NAME," + ",".join(vars_), "for": "county:*"}
        dataset_path = f"census/acs5_2022_{t}"
        filename = f"acs5_2022_{t}_county.csv"
        ingest_one(t, params, dataset_path, filename)

    return manifest
