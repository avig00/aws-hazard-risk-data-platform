"""
Glue PythonShell entrypoints for Bronze ingestion.

These scripts are used as Glue Job `script_location` targets.
They call ingestion.* to fetch raw data and land it in S3 Bronze.
"""
