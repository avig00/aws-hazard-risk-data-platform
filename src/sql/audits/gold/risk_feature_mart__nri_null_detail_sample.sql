-- risk_feature_mart__nri_null_detail_sample
-- Purpose:
--   Provide a detail sample of rows where nri_risk_score is NULL for manual review.
--   This is the fastest query for spot-checking whether the remaining rows are
--   acceptable edge cases or still-recoverable county mapping gaps.
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
  m.year,
  COALESCE(m.noaa_event_count, 0) AS noaa_event_count,
  COALESCE(m.fema_valid_registrations, 0) AS fema_valid_registrations,
  COALESCE(m.population_total, 0) AS population_total,
  CASE
    WHEN n.county_fips IS NULL THEN 'missing_in_nri_reference'
    WHEN n.risk_score IS NULL THEN 'present_in_nri_but_score_null'
    ELSE 'unexpected_null_after_join'
  END AS nri_join_status
FROM mart_nulls m
LEFT JOIN nri_ref n
  ON m.county_fips = n.county_fips
ORDER BY
  noaa_event_count DESC,
  fema_valid_registrations DESC,
  county_fips,
  year
LIMIT 100;
