from __future__ import annotations

import re
from typing import Dict, List, Optional

from ingestion.http import get_text, download_bytes, default_headers
from ingestion.s3_io import (
    bronze_key,
    head_exists,
    put_json,
    upload_file,
    sha256_file,
    stage_bytes_to_tmp,
    utc_now_iso,
)

BASE_NOAA_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
NOAA_TYPES = ["details", "fatalities", "locations"]


def _pick_latest_filename_from_listing(html: str, prefix: str) -> Optional[str]:
    """
    Matches href="StormEvents_details-ftp_v1.0_d2000_c20250520.csv.gz"
    then picks the max cYYYYMMDD.
    """
    pattern = r'href="(' + re.escape(prefix) + r'_c\d{8}\.csv\.gz)"'
    matches = re.findall(pattern, html)
    if not matches:
        return None
    return sorted(matches)[-1]


def resolve_noaa_years(run_dt: str, mode: str, backfill_start: int, backfill_end: int, monthly_offset: int = 0) -> List[int]:
    """
    Option 2:
      - monthly: ingest only year(run_dt) + offset
      - backfill: ingest start..end inclusive
    """
    mode = (mode or "monthly").strip().lower()
    if mode == "backfill":
        if backfill_end < backfill_start:
            raise ValueError("NOAA backfill invalid: end < start")
        return list(range(backfill_start, backfill_end + 1))

    year = int(run_dt[:4]) + int(monthly_offset)
    return [year]


def ingest_noaa(
    bucket: str,
    bronze_prefix: str,
    run_dt: str,
    mode: str = "monthly",
    backfill_start_year: int = 2000,
    backfill_end_year: int = 2023,
    monthly_year_offset: int = 0,
    types: Optional[List[str]] = None,
    timeout: int = 120,
    retries: int = 3,
) -> Dict[str, Dict]:
    """
    Ingest NOAA StormEvents raw data into S3 Bronze.

    Output:
      hazard/bronze/noaa/<type>/run_dt=.../StormEvents_<type>-ftp_v1.0_dYYYY.csv.gz
      plus .metadata.json alongside.

    Idempotent:
      Skips if the target object exists.

    Returns:
      manifest keyed by "{type}:{year}".
    """
    html = get_text(f"{BASE_NOAA_URL}/", timeout=60, headers=default_headers())
    years = resolve_noaa_years(run_dt, mode, backfill_start_year, backfill_end_year, monthly_year_offset)
    types = types or NOAA_TYPES

    manifest: Dict[str, Dict] = {}

    for year in years:
        for filetype in types:
            prefix = f"StormEvents_{filetype}-ftp_v1.0_d{year}"
            filename_latest = _pick_latest_filename_from_listing(html, prefix)
            key_id = f"{filetype}:{year}"

            if not filename_latest:
                manifest[key_id] = {"status": "MISSING_REMOTE", "prefix": prefix}
                continue

            url = f"{BASE_NOAA_URL}/{filename_latest}"
            out_name = f"{prefix}.csv.gz"
            s3_key = bronze_key(bronze_prefix, f"noaa/{filetype}", run_dt, out_name)

            if head_exists(bucket, s3_key):
                manifest[key_id] = {"status": "SKIPPED_EXISTS", "url": url, "s3_key": s3_key}
                continue

            try:
                data = download_bytes(url, timeout=timeout, retries=retries)
                tmp_path = stage_bytes_to_tmp(data, suffix=".csv.gz")

                upload_file(bucket, s3_key, tmp_path, content_type="application/gzip")

                meta_key = bronze_key(bronze_prefix, f"noaa/{filetype}", run_dt, f"{out_name}.metadata.json")
                put_json(bucket, meta_key, {
                    "source": "noaa",
                    "asset": filetype,
                    "year": year,
                    "run_dt": run_dt,
                    "fetched_at": utc_now_iso(),
                    "url": url,
                    "bytes": len(data),
                    "sha256": sha256_file(tmp_path),
                    "s3_key": s3_key,
                })

                manifest[key_id] = {"status": "OK", "url": url, "s3_key": s3_key}
            except Exception as e:
                manifest[key_id] = {"status": "FAILED", "url": url, "s3_key": s3_key, "error": str(e)}

    return manifest
