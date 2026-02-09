from __future__ import annotations

import csv
import os
import tempfile
from typing import Dict, List

from ingestion.http import get_json, default_headers
from ingestion.s3_io import bronze_key, head_exists, put_json, upload_file, sha256_file, utc_now_iso

NRI_ARCGIS_URL = (
    "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/"
    "National_Risk_Index_Counties/FeatureServer/0/query"
)


def ingest_nri(
    bucket: str,
    bronze_prefix: str,
    run_dt: str,
    page_size: int = 2000,
    timeout: int = 240,
) -> Dict[str, Dict]:
    """
    Ingest FEMA National Risk Index Counties via ArcGIS FeatureServer paging.

    Output:
      hazard/bronze/nri/counties/run_dt=.../nri_counties.csv (+metadata)

    Returns:
      manifest keyed by 'nri_counties.csv'.
    """
    dataset_path = "nri/counties"
    filename = "nri_counties.csv"
    s3_key = bronze_key(bronze_prefix, dataset_path, run_dt, filename)

    if head_exists(bucket, s3_key):
        return {filename: {"status": "SKIPPED_EXISTS", "s3_key": s3_key}}

    fd, tmp_path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)

    row_count = 0
    try:
        # First page to infer columns
        offset = 0
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": page_size,
            "returnGeometry": "false",
        }
        payload = get_json(NRI_ARCGIS_URL, params=params, timeout=timeout, headers=default_headers())
        features = payload.get("features", [])
        if not features:
            raise RuntimeError("NRI returned no features on first page")

        first_attrs = features[0].get("attributes", {}) or {}
        columns: List[str] = list(first_attrs.keys())

        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()

            while True:
                params = {
                    "where": "1=1",
                    "outFields": "*",
                    "f": "json",
                    "resultOffset": offset,
                    "resultRecordCount": page_size,
                    "returnGeometry": "false",
                }
                payload = get_json(NRI_ARCGIS_URL, params=params, timeout=timeout, headers=default_headers())
                feats = payload.get("features", [])
                if not feats:
                    break

                for ft in feats:
                    writer.writerow((ft.get("attributes") or {}))
                    row_count += 1

                if not payload.get("exceededTransferLimit", False):
                    break

                offset += page_size

        upload_file(bucket, s3_key, tmp_path, content_type="text/csv")

        meta_key = bronze_key(bronze_prefix, dataset_path, run_dt, f"{filename}.metadata.json")
        put_json(bucket, meta_key, {
            "source": "nri",
            "asset": "counties",
            "run_dt": run_dt,
            "fetched_at": utc_now_iso(),
            "url": NRI_ARCGIS_URL,
            "row_count": row_count,
            "sha256": sha256_file(tmp_path),
            "s3_key": s3_key,
        })

        return {filename: {"status": "OK", "s3_key": s3_key, "row_count": row_count}}

    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
