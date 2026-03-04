-- NRI county-level view (static join)
CREATE OR REPLACE VIEW gold_hazard._nri_county AS
SELECT
  lpad(regexp_replace(county_fips, '[^0-9]', ''), 5, '0') AS county_fips,
  try_cast(risk_score AS double) AS nri_risk_score,
  try_cast(eal_score  AS double) AS nri_eal_score,
  try_cast(sovi_score AS double) AS nri_sovi_score,
  try_cast(resl_score AS double) AS nri_resl_score
FROM silver_hazard_cleaned.nri_scores_clean
WHERE county_fips IS NOT NULL
  AND length(trim(county_fips)) = 5
  AND substr(trim(county_fips), 1, 2) NOT IN ('00', '03')
  AND substr(trim(county_fips), 3, 3) <> '000';
