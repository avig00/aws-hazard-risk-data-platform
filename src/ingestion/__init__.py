"""
.ingestion

Raw-source ingestion into S3 Bronze.

Each ingest_* function:
- downloads raw artifacts from source of truth
- lands into S3: hazard/bronze/<source>/<dataset>/run_dt=YYYY-MM-DD/<file>
- writes metadata JSON next to each artifact
- returns a manifest dict for audit

Designed to run in Glue PythonShell (recommended) or MWAA (small runs).
"""

from .noaa import ingest_noaa
from .fema import ingest_fema
from .nri import ingest_nri
from .census import ingest_census

__all__ = ["ingest_noaa", "ingest_fema", "ingest_nri", "ingest_census"]
