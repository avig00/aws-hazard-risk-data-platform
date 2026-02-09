\SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS failures
FROM noaa_events_raw;
