SELECT
  CASE
    WHEN COUNT(*) = 0 THEN 1
    WHEN SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) > 0 THEN 1
    ELSE 0
  END AS failures
FROM noaa_events_clean;
