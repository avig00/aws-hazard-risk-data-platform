SELECT COUNT(*) AS failures
FROM noaa_events_clean
WHERE county_fips IS NULL OR year IS NULL;
