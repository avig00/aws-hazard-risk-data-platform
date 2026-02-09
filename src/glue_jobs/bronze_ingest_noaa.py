# bronze_ingest_noaa.py
"""
Glue Job: bronze_ingest_noaa
"""

from __future__ import annotations

import os
import sys
from urllib.parse import urlparse

import boto3
from awsglue.utils import getResolvedOptions


def _bootstrap_extra_pyfiles_to_syspath() -> None:
    """
    Glue PythonShell sometimes does not reliably add --extra-py-files to sys.path.
    This function makes it deterministic by:
      - reading --extra-py-files from sys.argv
      - downloading any s3://...zip to /tmp
      - adding the local zip path to sys.path (zipimport supported)
    """
    if "--extra-py-files" not in sys.argv:
        return

    idx = sys.argv.index("--extra-py-files")
    if idx + 1 >= len(sys.argv):
        return

    value = sys.argv[idx + 1]
    if not value:
        return

    uris = [u.strip() for u in value.split(",") if u.strip()]
    s3 = boto3.client("s3")

    for uri in uris:
        if not uri.startswith("s3://"):
            # If it's already a local path, just add it.
            if uri not in sys.path:
                sys.path.insert(0, uri)
            continue

        parsed = urlparse(uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        local_path = f"/tmp/{os.path.basename(key) or 'extra_pyfiles.zip'}"

        try:
            s3.download_file(bucket, key, local_path)
        except Exception as e:
            print(f"[WARN] Failed to download extra pyfile {uri}: {e}")
            continue

        if local_path not in sys.path:
            sys.path.insert(0, local_path)


def main() -> None:
    _bootstrap_extra_pyfiles_to_syspath()

    # Import AFTER bootstrap so 'ingestion' can be found
    from ingestion.noaa import ingest_noaa
    from ingestion.s3_io import put_json, ops_key

    args = getResolvedOptions(
        sys.argv,
        [
            "PLATFORM_S3_BUCKET",
            "BRONZE_PREFIX",
            "OPS_PREFIX",
            "RUN_DT",
            "NOAA_INGEST_MODE",
            "NOAA_BACKFILL_START_YEAR",
            "NOAA_BACKFILL_END_YEAR",
            "NOAA_MONTHLY_YEAR_OFFSET",
        ],
    )

    bucket = args["PLATFORM_S3_BUCKET"]
    bronze_prefix = args["BRONZE_PREFIX"]
    ops_prefix = args["OPS_PREFIX"]
    run_dt = args["RUN_DT"]

    ingest_mode = args["NOAA_INGEST_MODE"]
    backfill_start_year = int(args["NOAA_BACKFILL_START_YEAR"])
    backfill_end_year = int(args["NOAA_BACKFILL_END_YEAR"])
    monthly_year_offset = int(args["NOAA_MONTHLY_YEAR_OFFSET"])

    manifest = ingest_noaa(
        bucket=bucket,
        bronze_prefix=bronze_prefix,
        run_dt=run_dt,
        ingest_mode=ingest_mode,
        backfill_start_year=backfill_start_year,
        backfill_end_year=backfill_end_year,
        monthly_year_offset=monthly_year_offset,
    )

    manifest_key = ops_key(ops_prefix, "bronze_ingestion", "noaa", run_dt, "manifest.json")
    put_json(bucket, manifest_key, manifest)

    print(f"[OK] NOAA Bronze ingestion complete. Manifest: s3://{bucket}/{manifest_key}")


if __name__ == "__main__":
    main()
