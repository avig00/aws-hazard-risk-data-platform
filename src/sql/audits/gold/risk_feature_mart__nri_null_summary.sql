-- risk_feature_mart__nri_null_summary
-- Purpose:
--   Summarize rows in the current risk feature mart where nri_risk_score is NULL.
--   This classifies the remaining rows by whether the county exists in the NRI
--   Silver reference and by the feature-family combinations present on the row.
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
),
classified AS (
  SELECT
    m.county_fips,
    m.year,
    CASE
      WHEN n.county_fips IS NULL THEN 'missing_in_nri_reference'
      WHEN n.risk_score IS NULL THEN 'present_in_nri_but_score_null'
      ELSE 'unexpected_null_after_join'
    END AS nri_join_status,
    CASE
      WHEN COALESCE(m.noaa_event_count, 0) > 0
       AND COALESCE(m.fema_valid_registrations, 0) > 0 THEN 'noaa_and_fema'
      WHEN COALESCE(m.noaa_event_count, 0) > 0 THEN 'noaa_only'
      WHEN COALESCE(m.fema_valid_registrations, 0) > 0 THEN 'fema_only'
      ELSE 'neither_noaa_nor_fema'
    END AS feature_presence,
    SUBSTR(m.county_fips, 1, 2) AS state_fips
  FROM mart_nulls m
  LEFT JOIN nri_ref n
    ON m.county_fips = n.county_fips
)
SELECT
  nri_join_status,
  feature_presence,
  state_fips,
  COUNT(*) AS rows
FROM classified
GROUP BY 1, 2, 3
ORDER BY rows DESC, nri_join_status, feature_presence, state_fips;
