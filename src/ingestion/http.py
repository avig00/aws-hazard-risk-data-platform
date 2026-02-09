from __future__ import annotations

import time
from typing import Any, Dict, Optional

import requests


def default_headers() -> Dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (compatible; aws-hazard-risk-agent/1.0)",
        "Accept": "*/*",
    }


def get_text(url: str, timeout: int = 60, headers: Optional[Dict[str, str]] = None) -> str:
    h = headers or default_headers()
    r = requests.get(url, headers=h, timeout=timeout)
    r.raise_for_status()
    return r.text


def get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 120,
    headers: Optional[Dict[str, str]] = None,
) -> Any:
    h = headers or default_headers()
    r = requests.get(url, params=params, headers=h, timeout=timeout)
    r.raise_for_status()
    return r.json()


def download_bytes(
    url: str,
    timeout: int = 120,
    retries: int = 3,
    backoff_s: float = 1.5,
    headers: Optional[Dict[str, str]] = None,
) -> bytes:
    """
    Download response content with retries.

    For NOAA/FEMA bulk CSVs, this is fine in Glue PythonShell.
    """
    h = headers or default_headers()
    last_err: Optional[str] = None

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, stream=True, headers=h, timeout=timeout, allow_redirects=True)
            if r.status_code != 200:
                last_err = f"status={r.status_code}"
                if attempt < retries:
                    time.sleep(backoff_s * attempt)
                continue
            return r.content
        except Exception as e:
            last_err = str(e)
            if attempt < retries:
                time.sleep(backoff_s * attempt)

    raise RuntimeError(f"Failed to download {url} after {retries} attempts ({last_err})")
