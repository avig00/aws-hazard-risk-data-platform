WITH s AS (
  SELECT
    count(*) AS rows,
    sum(CASE WHEN nri_risk_score IS NULL THEN 1 ELSE 0 END) AS null_risk_score,
    sum(CASE WHEN nri_eal_score  IS NULL THEN 1 ELSE 0 END) AS null_eal_score,
    sum(CASE WHEN nri_sovi_score IS NULL THEN 1 ELSE 0 END) AS null_sovi_score,
    sum(CASE WHEN nri_resl_score IS NULL THEN 1 ELSE 0 END) AS null_resl_score
  FROM gold_hazard._nri_county
)
SELECT
  *,
  100.0 * null_risk_score / nullif(rows, 0) AS pct_null_risk_score,
  100.0 * null_eal_score  / nullif(rows, 0) AS pct_null_eal_score,
  100.0 * null_sovi_score / nullif(rows, 0) AS pct_null_sovi_score,
  100.0 * null_resl_score / nullif(rows, 0) AS pct_null_resl_score
FROM s
WHERE rows = 0
   OR (100.0 * null_risk_score / nullif(rows, 0)) > 5
   OR (100.0 * null_eal_score  / nullif(rows, 0)) > 5
   OR (100.0 * null_sovi_score / nullif(rows, 0)) > 5
   OR (100.0 * null_resl_score / nullif(rows, 0)) > 5;
