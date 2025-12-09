import os
import requests
import zipfile
from io import BytesIO

BASE_DATA_DIR = "data/raw"

# -----------------------------
# Helpers
# -----------------------------
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def download_file(url, dest_path):
    if os.path.exists(dest_path):
        print(f"[SKIP] {dest_path} already exists.")
        return

    print(f"[DOWNLOAD] {url}")
    response = requests.get(url, stream=True)

    if response.status_code != 200:
        print(f"[ERROR] Failed to download {url}")
        return

    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    print(f"[SAVED] {dest_path} ({os.path.getsize(dest_path)/1_000_000:.2f} MB)")


# ======================================================================================
# 1) NOAA Storm Events CSVs (2000–2023)
# ======================================================================================

def download_noaa():
    noaa_dir = f"{BASE_DATA_DIR}/noaa"
    ensure_dir(noaa_dir)

    BASE_NOAA_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"

    years = range(2000, 2024)  # 2000–2023

    for year in years:
        for filetype in ["details", "fatalities", "locations"]:
            # NOAA filenames follow this pattern:
            # StormEvents_{type}-ftp_v1.0_dYYYY_c*.csv.gz
            filename_prefix = f"StormEvents_{filetype}-ftp_v1.0_d{year}"
            dest_path = f"{noaa_dir}/{filename_prefix}.csv.gz"

            # We don't know the _cXXXX suffix ahead of time, so use wildcard support:
            # NOAA server lists only exact filenames — build full pattern.
            # The wildcard query '?C=M;O=D' sorts latest first.
            # We'll fetch the directory listing and pick the file.
            listing_url = f"{BASE_NOAA_URL}/?C=M;O=D"

            print(f"[INFO] Searching for NOAA file for {year} ({filetype}) …")

            html = requests.get(listing_url).text
            # Find full file name in HTML
            candidates = [
                line.split('href="')[1].split('"')[0]
                for line in html.splitlines()
                if filename_prefix in line and line.endswith(".csv.gz</a>")
            ]

            if not candidates:
                print(f"[WARN] No NOAA file found for {filename_prefix}")
                continue

            filename = candidates[0]
            file_url = f"{BASE_NOAA_URL}/{filename}"

            download_file(file_url, dest_path)


# ======================================================================================
# 2) FEMA Open Data
# ======================================================================================

def download_fema():
    fema_dir = f"{BASE_DATA_DIR}/fema"
    ensure_dir(fema_dir)

    FEMA_FILES = {
        "disaster_declarations.csv":
            "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries.csv",
        "housing_assistance.csv":
            "https://www.fema.gov/api/open/v2/IpawsRegistrantsHousingAssistance.csv"
    }

    for filename, url in FEMA_FILES.items():
        download_file(url, f"{fema_dir}/{filename}")


# ======================================================================================
# 3) NRI DATA
# ======================================================================================

def download_nri():
    nri_dir = f"{BASE_DATA_DIR}/nri"
    ensure_dir(nri_dir)

    nri_zip_url = "https://hazards.fema.gov/static/data/nri/downloads/NRI_Table_Counties.zip"
    zip_path = f"{nri_dir}/NRI_Table_Counties.zip"

    download_file(nri_zip_url, zip_path)

    # Extract ZIP
    print(f"[UNZIP] Extracting {zip_path} …")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(nri_dir)
    print(f"[DONE] Extracted NRI CSVs into {nri_dir}")


# ======================================================================================
# 4) Census ACS County Demographics (CensusReporter API)
# ======================================================================================

def download_census():
    census_dir = f"{BASE_DATA_DIR}/census"
    ensure_dir(census_dir)

    # Pre-selected useful ACS tables:
    # B01001 (Age)
    # B19013 (Income)
    # B15003 (Education)
    # B23025 (Employment)
    # B25077 (Median home value)

    census_url = (
        "https://api.censusreporter.org/1.0/download/acs2022_5yr?"
        "table_ids=B01001,B19013,B15003,B23025,B25077&geo_ids=county:*"
    )

    download_file(census_url, f"{census_dir}/acs_county_demographics_2022.csv")


# ======================================================================================
# RUN ALL DOWNLOADS
# ======================================================================================

if __name__ == "__main__":

    print("\n=== DOWNLOADING NOAA DATA ===")
    download_noaa()

    print("\n=== DOWNLOADING FEMA DATA ===")
    download_fema()

    print("\n=== DOWNLOADING NRI DATA ===")
    download_nri()

    print("\n=== DOWNLOADING CENSUS DATA ===")
    download_census()

    print("\n=== ALL DOWNLOADS COMPLETE ===")
