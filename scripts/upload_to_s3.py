#!/usr/bin/env python3
"""
scripts/upload_to_s3.py

Uploads Phase 1 local raw data to S3 Bronze prefixes.

- NOAA: uploads to .../noaa/<type>/year=YYYY/<filename>.csv.gz
- FEMA: uploads to .../fema/<dataset>/<filename>.csv
- NRI: uploads to .../nri/counties/<filename>.csv
- Census: uploads to .../census/<table>/<filename>.csv
- Audit: uploads to .../_audits/<filename>.json

Behavior:
- Idempotent by default: skips upload if object exists with same ContentLength
- --dry-run prints actions only (no uploads)
- --force uploads even if same-size object exists

Usage:
  python scripts/upload_to_s3.py --config config/bronze_paths.json
  python scripts/upload_to_s3.py --config config/bronze_paths.json --dry-run
  python scripts/upload_to_s3.py --config config/bronze_paths.json --force
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
DOCS_DIR = PROJECT_ROOT / "docs"

NOAA_PATTERN = re.compile(
    r"StormEvents_(details|fatalities|locations)-ftp_v1\.0_d(\d{4})\.csv\.gz$"
)

def load_config(path: Path) -> dict:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    if not cfg.get("bucket"):
        raise ValueError("Config missing 'bucket'")
    return cfg

def s3_head_object(s3, bucket: str, key: str) -> Optional[dict]:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise

def upload_file(
    s3,
    local_path: Path,
    bucket: str,
    key: str,
    dry_run: bool = False,
    force: bool = False,
) -> Tuple[bool, str]:
    """
    Returns (uploaded?, message).
    Default behavior: skip upload if object exists with same ContentLength.
    """
    size = local_path.stat().st_size

    if not force:
        existing = s3_head_object(s3, bucket, key)
        if existing and int(existing.get("ContentLength", -1)) == size:
            return False, f"[SKIP] s3://{bucket}/{key} (same size already exists)"

    if dry_run:
        return True, f"[DRYRUN] Would upload {local_path} -> s3://{bucket}/{key}"

    s3.upload_file(
        Filename=str(local_path),
        Bucket=bucket,
        Key=key,
    )
    return True, f"[UPLOADED] {local_path.relative_to(PROJECT_ROOT)} -> s3://{bucket}/{key}"

def ensure_prefix(prefix: str) -> str:
    return prefix.strip("/")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to bronze_paths.json")
    ap.add_argument("--dry-run", action="store_true", help="Print actions without uploading")
    ap.add_argument("--force", action="store_true", help="Upload even if same-size object exists")
    args = ap.parse_args()

    cfg = load_config(Path(args.config))
    bucket = cfg["bucket"]

    s3 = boto3.client("s3")

    # 1) NOAA (partitioned by year)
    noaa_dir = RAW_DIR / "noaa"
    if not noaa_dir.exists():
        raise FileNotFoundError(f"Missing local NOAA dir: {noaa_dir}")

    for f in sorted(noaa_dir.glob("StormEvents_*_d????.csv.gz")):
        m = NOAA_PATTERN.match(f.name)
        if not m:
            continue
        ftype, year = m.group(1), m.group(2)
        base_prefix = ensure_prefix(cfg["noaa"][ftype])
        key = f"{base_prefix}/year={year}/{f.name}"
        _, msg = upload_file(s3, f, bucket, key, dry_run=args.dry_run, force=args.force)
        print(msg)

    # 2) FEMA
    fema_dir = RAW_DIR / "fema"
    fema_map = {
        "disaster_declarations.csv": "disaster_declarations",
        "housing_assistance_owners.csv": "housing_assistance_owners",
        "housing_assistance_renters.csv": "housing_assistance_renters",
    }

    for fname, ds_key in fema_map.items():
        p = fema_dir / fname
        if not p.exists():
            raise FileNotFoundError(f"Missing FEMA file: {p}")
        base_prefix = ensure_prefix(cfg["fema"][ds_key])
        key = f"{base_prefix}/{p.name}"
        _, msg = upload_file(s3, p, bucket, key, dry_run=args.dry_run, force=args.force)
        print(msg)

    # 3) NRI
    nri_file = RAW_DIR / "nri" / "nri_counties.csv"
    if not nri_file.exists():
        raise FileNotFoundError(f"Missing NRI file: {nri_file}")
    base_prefix = ensure_prefix(cfg["nri"]["counties"])
    key = f"{base_prefix}/{nri_file.name}"
    _, msg = upload_file(s3, nri_file, bucket, key, dry_run=args.dry_run, force=args.force)
    print(msg)

    # 4) Census (5)
    census_dir = RAW_DIR / "census"
    census_files = {
        "acs5_2022_B01001_county.csv": "acs5_2022_B01001",
        "acs5_2022_B15003_county.csv": "acs5_2022_B15003",
        "acs5_2022_B23025_county.csv": "acs5_2022_B23025",
        "acs5_2022_B19013_county.csv": "acs5_2022_B19013",
        "acs5_2022_B25077_county.csv": "acs5_2022_B25077",
    }

    for fname, table_key in census_files.items():
        p = census_dir / fname
        if not p.exists():
            raise FileNotFoundError(f"Missing Census file: {p}")
        base_prefix = ensure_prefix(cfg["census"][table_key])
        key = f"{base_prefix}/{p.name}"
        _, msg = upload_file(s3, p, bucket, key, dry_run=args.dry_run, force=args.force)
        print(msg)

    # 5) Audit summary (optional)
    audit_file = DOCS_DIR / "data_audit_phase1_summary.json"
    if audit_file.exists():
        base_prefix = ensure_prefix(cfg["audits"])
        key = f"{base_prefix}/{audit_file.name}"
        _, msg = upload_file(s3, audit_file, bucket, key, dry_run=args.dry_run, force=args.force)
        print(msg)
    else:
        print(f"[WARN] Audit file not found (skipping): {audit_file}")

    print("\n Upload step complete.")

if __name__ == "__main__":
    main()
