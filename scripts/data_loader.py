"""
scripts/data_loader.py

Downloads raw datasets for the AWS Hazard & Risk Data Platform into:
  <repo_root>/data/raw/<dataset>/

Datasets (Project 1 / Phase 1):
- NOAA Storm Events (details, fatalities, locations) for years 2000–2023 (CSV.GZ)
- FEMA OpenFEMA:
    - DisasterDeclarationsSummaries.csv
    - HousingAssistanceOwners.csv (v2)
    - HousingAssistanceRenters.csv (v2)
- FEMA National Risk Index (NRI) Counties (via ArcGIS FeatureServer -> CSV)
- US Census ACS 2022 5-year (county:*) via OFFICIAL Census Data API (5 output files)

Run:
  python scripts/data_loader.py

Exit code:
  0 if all validations pass
  1 if any validations fail

"""

from __future__ import annotations

import csv
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests

import json
from datetime import datetime, timezone


# NRI + Census writes use pandas for convenience, but tests will still run without pandas.
try:
    import pandas as pd
except ImportError:
    pd = None

# --------------------------------------------------------------------------------------
# Paths (anchored to repo root)
# --------------------------------------------------------------------------------------
# This file lives at: <repo_root>/scripts/data_loader.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# --------------------------------------------------------------------------------------
# Phase 1 expectations
# --------------------------------------------------------------------------------------
NOAA_YEARS = list(range(2000, 2024))  # 2000–2023 inclusive
NOAA_TYPES = ["details", "fatalities", "locations"]

FEMA_EXPECTED = [
    "disaster_declarations.csv",
    "housing_assistance_owners.csv",
    "housing_assistance_renters.csv",
]

CENSUS_EXPECTED = [
    "acs5_2022_B01001_county.csv",
    "acs5_2022_B15003_county.csv",
    "acs5_2022_B23025_county.csv",
    "acs5_2022_B19013_county.csv",
    "acs5_2022_B25077_county.csv",
]

# -----------------------------
# Helpers
# -----------------------------
def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def _default_headers() -> dict:
    return {
        "User-Agent": "Mozilla/5.0 (compatible; aws-hazard-risk-data-platform/1.0)",
        "Accept": "*/*",
    }

def download_file(
    url: str,
    dest_path: Path,
    timeout: int = 90,
    retries: int = 3,
    backoff_s: float = 1.5,
) -> bool:
    """
    Stream-download a file to disk. Returns True on success, False on failure.
    Uses an atomic write ('.part' then rename) to avoid partial files on errors.
    """
    if dest_path.exists() and dest_path.stat().st_size > 0:
        print(f"[SKIP] {dest_path} already exists.")
        return True

    ensure_dir(dest_path.parent)

    for attempt in range(1, retries + 1):
        print(f"[DOWNLOAD] {url} (attempt {attempt}/{retries})")
        try:
            r = requests.get(
                url,
                stream=True,
                headers=_default_headers(),
                timeout=timeout,
                allow_redirects=True,
            )
        except requests.RequestException as e:
            print(f"[ERROR] Request failed: {e}")
            if attempt < retries:
                time.sleep(backoff_s * attempt)
            continue

        if r.status_code != 200:
            print(f"[ERROR] Failed to download (status={r.status_code}): {url}")
            if attempt < retries:
                time.sleep(backoff_s * attempt)
            continue

        tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
        try:
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            os.replace(tmp_path, dest_path)
            size_mb = dest_path.stat().st_size / 1_000_000
            print(f"[SAVED] {dest_path} ({size_mb:.2f} MB)")
            return True
        except OSError as e:
            print(f"[ERROR] Failed writing file: {e}")
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass
            if attempt < retries:
                time.sleep(backoff_s * attempt)

    return False

def count_csv_rows_fast(path: Path) -> int:
    """
    Count rows without pandas (includes all data rows, excludes header).
    """
    with open(path, "r", newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        # Skip header
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)

# ======================================================================================
# 1) NOAA Storm Events CSVs (2000–2023)
# ======================================================================================

def download_noaa() -> None:
    """
    NOAA bulk files live in:
      https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/

    Filenames look like:
      StormEvents_details-ftp_v1.0_d2000_c20250520.csv.gz

    We download the latest "cYYYYMMDD" file per year and filetype and save as:
      StormEvents_<type>-ftp_v1.0_dYYYY.csv.gz
    """
    noaa_dir = BASE_DATA_DIR / "noaa"
    ensure_dir(noaa_dir)

    BASE_NOAA_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
    listing_url = f"{BASE_NOAA_URL}/"

    print(f"[INFO] Fetching NOAA directory listing: {listing_url}")
    try:
        html = requests.get(listing_url, headers=_default_headers(), timeout=60).text
    except requests.RequestException as e:
        print(f"[ERROR] Could not fetch NOAA listing: {e}")
        return

    def pick_latest_filename(prefix: str) -> str | None:
        """
        Match href="StormEvents_details-ftp_v1.0_d2000_c20250520.csv.gz"
        and pick max creation date.
        """
        pattern = r'href="(' + re.escape(prefix) + r'_c\d{8}\.csv\.gz)"'
        matches = re.findall(pattern, html)
        if not matches:
            return None
        return sorted(matches)[-1]

    for year in NOAA_YEARS:
        for filetype in NOAA_TYPES:
            prefix = f"StormEvents_{filetype}-ftp_v1.0_d{year}"
            print(f"[INFO] Searching for NOAA file for {year} ({filetype}) …")
            filename = pick_latest_filename(prefix)

            if not filename:
                print(f"[WARN] No NOAA file found for {prefix}")
                continue

            file_url = f"{BASE_NOAA_URL}/{filename}"
            dest_path = noaa_dir / f"{prefix}.csv.gz"

            ok = download_file(file_url, dest_path)
            if not ok:
                print(f"[WARN] NOAA download failed for {prefix}")

# ======================================================================================
# 2) FEMA Open Data (OpenFEMA)
# ======================================================================================

def download_fema() -> None:
    fema_dir = BASE_DATA_DIR / "fema"
    ensure_dir(fema_dir)

    FEMA_FILES = {
        "disaster_declarations.csv":
            "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries.csv",
        "housing_assistance_owners.csv":
            "https://www.fema.gov/api/open/v2/HousingAssistanceOwners.csv",
        "housing_assistance_renters.csv":
            "https://www.fema.gov/api/open/v2/HousingAssistanceRenters.csv",
    }

    for filename, url in FEMA_FILES.items():
        ok = download_file(url, fema_dir / filename, timeout=240, retries=3)
        if not ok:
            print(f"[WARN] FEMA download failed: {filename}")

# ======================================================================================
# 3) NRI DATA (County-level) via ArcGIS FeatureServer
# ======================================================================================

def download_nri() -> None:
    """
    Download FEMA National Risk Index Counties via ArcGIS FeatureServer paging.
    Writes: <repo_root>/data/raw/nri/nri_counties.csv
    """
    nri_dir = BASE_DATA_DIR / "nri"
    ensure_dir(nri_dir)

    out_csv = nri_dir / "nri_counties.csv"
    if out_csv.exists() and out_csv.stat().st_size > 0:
        print(f"[SKIP] {out_csv} already exists.")
        return

    if pd is None:
        print("[ERROR] pandas is required for NRI download. Install with: pip install pandas")
        return

    base = (
        "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/"
        "National_Risk_Index_Counties/FeatureServer/0/query"
    )

    all_rows: List[dict] = []
    result_offset = 0
    page_size = 2000

    print("[INFO] Downloading NRI counties from ArcGIS FeatureServer …")

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "json",
            "resultOffset": result_offset,
            "resultRecordCount": page_size,
            "returnGeometry": "false",
        }

        try:
            r = requests.get(base, params=params, headers=_default_headers(), timeout=240)
        except requests.RequestException as e:
            print(f"[ERROR] NRI request failed at offset={result_offset}: {e}")
            return

        if r.status_code != 200:
            print(f"[ERROR] NRI request failed (status={r.status_code}) at offset={result_offset}")
            return

        payload = r.json()
        features = payload.get("features", [])
        if not features:
            break

        all_rows.extend([ft.get("attributes", {}) for ft in features])

        exceeded = payload.get("exceededTransferLimit", False)
        print(f"[INFO] Pulled {len(features)} rows (offset={result_offset}). exceededTransferLimit={exceeded}")

        if not exceeded:
            break

        result_offset += page_size

    df = pd.DataFrame(all_rows)
    df.to_csv(out_csv, index=False)
    print(f"[SAVED] {out_csv} ({len(df):,} rows)")

# ======================================================================================
# 4) Census ACS County Demographics via OFFICIAL Census Data API
# ======================================================================================

def download_census() -> None:
    """
    CensusReporter download endpoint may 404. Use the official Census Data API instead.

    Outputs (raw) to <repo_root>/data/raw/census/:
      acs5_2022_B01001_county.csv
      acs5_2022_B15003_county.csv
      acs5_2022_B23025_county.csv
      acs5_2022_B19013_county.csv
      acs5_2022_B25077_county.csv

    Notes:
    - group() returns all columns for the table
    - single-estimate tables use explicit variables
    """
    census_dir = BASE_DATA_DIR / "census"
    ensure_dir(census_dir)

    base = "https://api.census.gov/data/2022/acs/acs5"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; aws-hazard-risk-data-platform/1.0)"}

    def fetch_to_csv(params: dict, out_path: Path, timeout: int = 240) -> bool:
        if out_path.exists() and out_path.stat().st_size > 0:
            print(f"[SKIP] {out_path} already exists.")
            return True

        try:
            r = requests.get(base, params=params, headers=headers, timeout=timeout)
        except requests.RequestException as e:
            print(f"[ERROR] Census API request failed: {e}")
            return False

        if r.status_code != 200:
            print(f"[ERROR] Census API download failed (status={r.status_code}) params={params}")
            return False

        try:
            data = r.json()  # first row = headers, rest = rows
        except ValueError:
            print("[ERROR] Census API returned non-JSON response.")
            return False

        if not data or len(data) < 2:
            print("[ERROR] Census API returned empty dataset.")
            return False

        # Write without requiring pandas
        tmp = out_path.with_suffix(out_path.suffix + ".part")
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(data)

        os.replace(tmp, out_path)
        size_mb = out_path.stat().st_size / 1_000_000
        print(f"[SAVED] {out_path} ({size_mb:.2f} MB, {len(data)-1:,} rows)")
        return True

    print("[INFO] Downloading ACS 2022 5-year (county:*) from Census Data API …")

    # Multi-column tables via group()
    group_tables = ["B01001", "B15003", "B23025"]
    for t in group_tables:
        params = {"get": f"NAME,group({t})", "for": "county:*"}
        out_path = census_dir / f"acs5_2022_{t}_county.csv"
        fetch_to_csv(params, out_path)

    # Single-estimate tables
    singles = {
        "B19013": ["B19013_001E"],  # Median household income
        "B25077": ["B25077_001E"],  # Median home value
    }
    for t, vars_ in singles.items():
        params = {"get": "NAME," + ",".join(vars_), "for": "county:*"}
        out_path = census_dir / f"acs5_2022_{t}_county.csv"
        fetch_to_csv(params, out_path)

# ======================================================================================
# Post-download validations ("tests")
# ======================================================================================

def validate_noaa() -> Tuple[bool, str]:
    noaa_dir = BASE_DATA_DIR / "noaa"
    if not noaa_dir.exists():
        return False, f"NOAA directory missing: {noaa_dir}"

    pattern = re.compile(r"StormEvents_(details|fatalities|locations)-ftp_v1\.0_d(\d{4})\.csv\.gz$")
    found = set()

    for p in noaa_dir.glob("StormEvents_*_d????.csv.gz"):
        m = pattern.match(p.name)
        if m:
            found.add((m.group(1), int(m.group(2))))

    missing: List[Tuple[str, int]] = []
    for y in NOAA_YEARS:
        for t in NOAA_TYPES:
            if (t, y) not in found:
                missing.append((t, y))

    expected_count = len(NOAA_YEARS) * len(NOAA_TYPES)
    actual_count = len(found)

    if missing:
        preview = ", ".join([f"{t}:{y}" for t, y in missing[:20]])
        return False, (
            f"NOAA coverage incomplete. Expected {expected_count} (type,year) pairs, got {actual_count}. "
            f"Missing examples: {preview}" + (" ..." if len(missing) > 20 else "")
        )

    return True, f"NOAA coverage complete: {actual_count}/{expected_count} files (2000–2023 x 3 types)"

def validate_fema() -> Tuple[bool, str]:
    fema_dir = BASE_DATA_DIR / "fema"
    if not fema_dir.exists():
        return False, f"FEMA directory missing: {fema_dir}"

    missing = []
    zero = []
    for fname in FEMA_EXPECTED:
        p = fema_dir / fname
        if not p.exists():
            missing.append(fname)
        elif p.stat().st_size == 0:
            zero.append(fname)

    if missing or zero:
        msg = []
        if missing:
            msg.append(f"missing={missing}")
        if zero:
            msg.append(f"zero_size={zero}")
        return False, "FEMA validation failed: " + "; ".join(msg)

    return True, f"FEMA files present: {len(FEMA_EXPECTED)}/{len(FEMA_EXPECTED)}"

def validate_nri() -> Tuple[bool, str]:
    nri_path = BASE_DATA_DIR / "nri" / "nri_counties.csv"
    if not nri_path.exists() or nri_path.stat().st_size == 0:
        return False, f"NRI file missing or empty: {nri_path}"

    # Row count sanity (county-scale)
    try:
        rows = count_csv_rows_fast(nri_path)
    except Exception as e:
        return False, f"NRI row count failed: {e}"

    if rows < 3000:
        return False, f"NRI row count too low ({rows}). Expected ~3,000+ for counties/equivalents."

    return True, f"NRI file present with {rows:,} rows"

def validate_census() -> Tuple[bool, str]:
    census_dir = BASE_DATA_DIR / "census"
    if not census_dir.exists():
        return False, f"Census directory missing: {census_dir}"

    missing = []
    zero = []
    low_rows = []

    for fname in CENSUS_EXPECTED:
        p = census_dir / fname
        if not p.exists():
            missing.append(fname)
            continue
        if p.stat().st_size == 0:
            zero.append(fname)
            continue

        try:
            rows = count_csv_rows_fast(p)
        except Exception:
            rows = -1

        # County-scale sanity
        if rows != -1 and rows < 3000:
            low_rows.append((fname, rows))

    if missing or zero or low_rows:
        msg = []
        if missing:
            msg.append(f"missing={missing}")
        if zero:
            msg.append(f"zero_size={zero}")
        if low_rows:
            msg.append("low_rows=" + str(low_rows[:5]) + (" ..." if len(low_rows) > 5 else ""))
        return False, "Census validation failed: " + "; ".join(msg)

    return True, f"Census files present: {len(CENSUS_EXPECTED)}/{len(CENSUS_EXPECTED)} (county-scale row counts look OK)"

# def run_validations() -> bool:
#     print("\n=== RUNNING PHASE 1 VALIDATIONS ===")

#     checks = [
#         ("NOAA", validate_noaa),
#         ("FEMA", validate_fema),
#         ("NRI", validate_nri),
#         ("CENSUS", validate_census),
#     ]

#     all_ok = True
#     for name, fn in checks:
#         ok, msg = fn()
#         status = "PASS" if ok else "FAIL"
#         print(f"[{status}] {name}: {msg}")
#         all_ok = all_ok and ok

#     if all_ok:
#         print("\n PHASE 1 PASS: All datasets downloaded and validated.")
#     else:
#         print("\n PHASE 1 FAIL: One or more validations failed. Fix issues and rerun.")
#     return all_ok

def run_validations() -> tuple[bool, dict]:
    print("\n=== RUNNING PHASE 1 VALIDATIONS ===")

    checks = [
        ("NOAA", validate_noaa),
        ("FEMA", validate_fema),
        ("NRI", validate_nri),
        ("CENSUS", validate_census),
    ]

    all_ok = True
    details = {}

    for name, fn in checks:
        ok, msg = fn()
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {name}: {msg}")
        details[name] = {"status": status, "message": msg}
        all_ok = all_ok and ok

    if all_ok:
        print("\n PHASE 1 PASS: All datasets downloaded and validated.")
    else:
        print("\n PHASE 1 FAIL: One or more validations failed. Fix issues and rerun.")

    return all_ok, details


# ======================================================================================
# RUN ALL DOWNLOADS + VALIDATIONS
# ======================================================================================

def write_lightweight_audit_summary(
    validation_passed: bool,
    validation_details: dict,
    out_path: Path | None = None,
    sample_filenames: int = 10,
) -> Path:
    """
    Writes a lightweight JSON audit summary (no checksums, no row counts).

    Includes:
      - timestamp
      - validation status + per-check messages
      - file counts per dataset
      - total bytes/MB per dataset
      - optional sample filenames per dataset
    """
    docs_dir = PROJECT_ROOT / "docs"
    ensure_dir(docs_dir)

    if out_path is None:
        out_path = docs_dir / "data_audit_phase1_summary.json"

    def list_files(ds: str) -> list[Path]:
        if ds == "noaa":
            p = BASE_DATA_DIR / "noaa"
            return sorted(p.glob("StormEvents_*_d????.csv.gz")) if p.exists() else []
        if ds == "fema":
            p = BASE_DATA_DIR / "fema"
            return sorted(p.glob("*.csv")) if p.exists() else []
        if ds == "nri":
            p = BASE_DATA_DIR / "nri"
            return sorted(p.glob("*.csv")) if p.exists() else []
        if ds == "census":
            p = BASE_DATA_DIR / "census"
            return sorted(p.glob("*.csv")) if p.exists() else []
        return []

    datasets = ["noaa", "fema", "nri", "census"]
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "project_root": str(PROJECT_ROOT),
        "base_data_dir": str(BASE_DATA_DIR),
        "phase": "phase_1_local_load",
        "validation": {
            "status": "PASS" if validation_passed else "FAIL",
            "details": validation_details,  # { "NOAA": {"status": "...", "message": "..."}, ... }
        },
        "datasets": {},
    }

    for ds in datasets:
        files = list_files(ds)
        total_bytes = sum(f.stat().st_size for f in files)
        sample = [str(f.relative_to(PROJECT_ROOT)) for f in files[:sample_filenames]]

        summary["datasets"][ds] = {
            "file_count": len(files),
            "total_bytes": total_bytes,
            "total_mb": round(total_bytes / 1_000_000, 2),
            "sample_files": sample,
        }

    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[AUDIT] Wrote lightweight Phase 1 audit summary to: {out_path}")
    return out_path


if __name__ == "__main__":
    print("\n=== DOWNLOADING NOAA DATA ===")
    try:
        download_noaa()
    except Exception as e:
        print(f"[FATAL-WARN] NOAA step crashed but continuing: {e}")

    print("\n=== DOWNLOADING FEMA DATA ===")
    try:
        download_fema()
    except Exception as e:
        print(f"[FATAL-WARN] FEMA step crashed but continuing: {e}")

    print("\n=== DOWNLOADING NRI DATA ===")
    try:
        download_nri()
    except Exception as e:
        print(f"[FATAL-WARN] NRI step crashed but continuing: {e}")

    print("\n=== DOWNLOADING CENSUS DATA ===")
    try:
        download_census()
    except Exception as e:
        print(f"[FATAL-WARN] Census step crashed but continuing: {e}")

    print("\n=== ALL DOWNLOADS COMPLETE (WITH WARNINGS IF ANY) ===")

    ok, details = run_validations()
    write_lightweight_audit_summary(
        validation_passed=ok,
        validation_details=details,
        sample_filenames=10
    )
    sys.exit(0 if ok else 1)

