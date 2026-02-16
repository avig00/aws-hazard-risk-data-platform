SELECT COUNT(*) AS failures
FROM silver_hazard_cleaned.noaa_events_clean
WHERE (
       event_id IS NULL
    OR episode_id IS NULL
    OR state IS NULL
    OR state_fips IS NULL
    OR cz_fips IS NULL
    OR event_type IS NULL
    OR year IS NULL
)
AND NOT (
  "$path" = 's3://aws-hazard-risk-vigamogh-dev/hazard/silver/noaa_events_clean/year=2003/part-00003-30ae6e77-0a06-4168-a3c4-a9538b675699.c000.snappy.parquet'
);
