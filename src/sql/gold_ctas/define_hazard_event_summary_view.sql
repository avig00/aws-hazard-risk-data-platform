-- hazard_event_summary_view
-- Defines the mart logic from Silver (noaa_events_clean)
-- Year window enforced: 2010–2023

CREATE OR REPLACE VIEW hazard_event_summary_view AS
WITH base AS (
  SELECT
    county_fips,
    CAST(year AS BIGINT) AS year,
    event_type AS hazard_type,
    COALESCE(category, 'UNKNOWN') AS hazard_category,
    CAST(COALESCE(deaths_direct, 0) + COALESCE(deaths_indirect, 0) AS BIGINT) AS fatalities,
    CAST(COALESCE(injuries_direct, 0) + COALESCE(injuries_indirect, 0) AS BIGINT) AS injuries,
    -- Parse NOAA damage_property strings like '10K', '2.5M', '1B', '0', '' into USD (DOUBLE)
    CASE
      WHEN damage_property IS NULL OR TRIM(damage_property) = '' THEN 0.0
      WHEN REGEXP_LIKE(UPPER(TRIM(damage_property)), '^[0-9]+(\.[0-9]+)?$')
        THEN CAST(TRIM(damage_property) AS DOUBLE)
      WHEN REGEXP_LIKE(UPPER(TRIM(damage_property)), '^[0-9]+(\.[0-9]+)?K$')
        THEN CAST(REGEXP_EXTRACT(UPPER(TRIM(damage_property)), '^([0-9]+(\.[0-9]+)?)K$', 1) AS DOUBLE) * 1000.0
      WHEN REGEXP_LIKE(UPPER(TRIM(damage_property)), '^[0-9]+(\.[0-9]+)?M$')
        THEN CAST(REGEXP_EXTRACT(UPPER(TRIM(damage_property)), '^([0-9]+(\.[0-9]+)?)M$', 1) AS DOUBLE) * 1000000.0
      WHEN REGEXP_LIKE(UPPER(TRIM(damage_property)), '^[0-9]+(\.[0-9]+)?B$')
        THEN CAST(REGEXP_EXTRACT(UPPER(TRIM(damage_property)), '^([0-9]+(\.[0-9]+)?)B$', 1) AS DOUBLE) * 1000000000.0
      ELSE 0.0
    END AS property_damage_usd
  FROM silver_hazard_cleaned.noaa_events_clean
  WHERE county_fips IS NOT NULL AND LENGTH(TRIM(county_fips)) = 5
    AND year IS NOT NULL
    AND CAST(year AS BIGINT) BETWEEN 2010 AND 2023
)
SELECT
  county_fips,
  hazard_type,
  hazard_category,
  COUNT(*) AS event_count,
  SUM(fatalities) AS total_fatalities,
  SUM(injuries) AS total_injuries,
  AVG(property_damage_usd) AS avg_property_damage,
  year
FROM base
GROUP BY 1, 2, 3, 8;
