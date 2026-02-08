WITH noaa_types AS (
  SELECT DISTINCT event_type
  FROM silver_hazard_cleaned.noaa_events_clean
  WHERE event_type IS NOT NULL
)
SELECT t.event_type
FROM noaa_types t
LEFT JOIN gold_hazard.hazard_type_map m
  ON t.event_type = m.event_type
WHERE m.event_type IS NULL
ORDER BY t.event_type;
