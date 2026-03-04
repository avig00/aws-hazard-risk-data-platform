-- risk_feature_mart__nri_null_counties
-- Purpose:
--   Identify the counties contributing the most rows with nri_risk_score IS NULL.
--   This is useful for finding county-name mapping gaps that still need cleanup.
--
-- Template vars:
--   {{athena_db_gold}}
--   {{athena_db_silver}}

WITH mart_nulls AS (
  SELECT *
  FROM {{athena_db_gold}}.risk_feature_mart_current
  WHERE nri_risk_score IS NULL
),
nri_ref AS (
  SELECT
    TRIM(county_fips) AS county_fips,
    CAST(risk_score AS DOUBLE) AS risk_score
  FROM {{athena_db_silver}}.nri_scores_clean
  WHERE county_fips IS NOT NULL
)
SELECT
  m.county_fips,
  SUBSTR(m.county_fips, 1, 2) AS state_fips,
  COUNT(*) AS null_year_rows,
  MIN(m.year) AS first_year,
  MAX(m.year) AS last_year,
  SUM(COALESCE(m.noaa_event_count, 0)) AS total_noaa_events,
  SUM(COALESCE(m.fema_valid_registrations, 0)) AS total_fema_registrations,
  CASE
    WHEN n.county_fips IS NULL THEN 'missing_in_nri_reference'
    WHEN n.risk_score IS NULL THEN 'present_in_nri_but_score_null'
    ELSE 'unexpected_null_after_join'
  END AS nri_join_status
FROM mart_nulls m
LEFT JOIN nri_ref n
  ON m.county_fips = n.county_fips
GROUP BY 1, 2, 8
ORDER BY null_year_rows DESC, total_noaa_events DESC, total_fema_registrations DESC, county_fips;
