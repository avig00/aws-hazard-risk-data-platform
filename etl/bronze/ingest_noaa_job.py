def ingest_noaa_events(year: int):
    """
    Steps:
    1. Download raw NOAA CSV for given year.
    2. Load via PySpark.
    3. Write to S3 in Bronze partition: year=YYYY.
    4. Update Glue Data Catalog.
    """
    pass
