WITH s AS (
  SELECT
    count(*) AS rows,
    100.0 * sum(CASE WHEN nri_risk_score IS NULL THEN 1 ELSE 0 END) / count(*) AS pct_null_nri_risk,
    100.0 * sum(CASE WHEN nri_resl_score IS NULL THEN 1 ELSE 0 END) / count(*) AS pct_null_nri_resl,
    100.0 * sum(CASE WHEN population_total IS NULL THEN 1 ELSE 0 END) / count(*) AS pct_null_population,
    100.0 * sum(CASE WHEN median_household_income IS NULL THEN 1 ELSE 0 END) / count(*) AS pct_null_mhi
  FROM gold_hazard.risk_feature_mart
)
SELECT *
FROM s
WHERE rows = 0
   OR pct_null_nri_risk > 5
   OR pct_null_nri_resl > 5
   OR pct_null_population > 5
   OR pct_null_mhi > 5;
