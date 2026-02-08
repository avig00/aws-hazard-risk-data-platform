-- FEMA rollups for Phase 4
-- Inputs:
--   silver_hazard_cleaned.fema_disaster_declarations_clean  (has county_fips, fydeclared, disasternumber)
--   silver_hazard_cleaned.fema_claims_clean                 (no county_fips; has disasternumber + metrics)
--
-- Strategy:
--   1) Declarations provide county_fips + year
--   2) Claims are aggregated by disasternumber
--   3) Claims are allocated equally across counties declared for that disasternumber-year
--      (prevents naive double counting across counties)

------------------------------------------------------------
-- A) FEMA Declarations: county-year counts (direct)
------------------------------------------------------------
CREATE OR REPLACE VIEW gold_hazard._fema_decls_county_year AS
SELECT
  lpad(regexp_replace(cast(county_fips AS varchar), '[^0-9]', ''), 5, '0') AS county_fips,
  cast(fydeclared AS integer) AS year,
  count(*) AS fema_declaration_count
FROM silver_hazard_cleaned.fema_disaster_declarations_clean
WHERE county_fips IS NOT NULL
  AND fydeclared IS NOT NULL
GROUP BY 1,2;

------------------------------------------------------------
-- B) FEMA Claims: county-year aggregates via disasternumber allocation
------------------------------------------------------------

-- 1) Claims aggregated to disaster level (single row per disaster)
CREATE OR REPLACE VIEW gold_hazard._fema_claims_by_disaster AS
SELECT
  cast(disasternumber AS bigint) AS disasternumber,
  sum(coalesce(cast(validregistrations AS double), 0)) AS validregistrations,
  sum(coalesce(cast(totaldamage AS double), 0)) AS totaldamage,
  sum(coalesce(cast(totalapprovedihpamount AS double), 0)) AS totalapprovedihpamount,
  sum(coalesce(cast(repairreplaceamount AS double), 0)) AS repairreplaceamount,
  sum(coalesce(cast(rentalamount AS double), 0)) AS rentalamount,
  sum(coalesce(cast(otherneedsamount AS double), 0)) AS otherneedsamount,
  sum(coalesce(cast(totalinspected AS double), 0)) AS totalinspected
FROM silver_hazard_cleaned.fema_claims_clean
WHERE disasternumber IS NOT NULL
GROUP BY 1;

-- 2) Disaster → counties mapping from declarations (distinct counties per disaster-year)
CREATE OR REPLACE VIEW gold_hazard._fema_disaster_county_map AS
SELECT DISTINCT
  cast(disasternumber AS bigint) AS disasternumber,
  lpad(regexp_replace(cast(county_fips AS varchar), '[^0-9]', ''), 5, '0') AS county_fips,
  cast(fydeclared AS integer) AS year
FROM silver_hazard_cleaned.fema_disaster_declarations_clean
WHERE disasternumber IS NOT NULL
  AND county_fips IS NOT NULL
  AND fydeclared IS NOT NULL;

-- 3) County counts per disaster-year (for equal allocation)
CREATE OR REPLACE VIEW gold_hazard._fema_disaster_county_counts AS
SELECT
  disasternumber,
  year,
  count(DISTINCT county_fips) AS county_cnt
FROM gold_hazard._fema_disaster_county_map
GROUP BY 1,2;

-- 4) Final county-year claims features (allocated)
CREATE OR REPLACE VIEW gold_hazard._fema_claims_county_year AS
SELECT
  m.county_fips,
  m.year,

  -- Core volume + severity outputs (allocated)
  sum(c.validregistrations / nullif(cc.county_cnt, 0)) AS fema_valid_registrations,
  sum(c.totaldamage / nullif(cc.county_cnt, 0)) AS fema_total_damage,
  sum(c.totalapprovedihpamount / nullif(cc.county_cnt, 0)) AS fema_total_approved_ihp_amount,

  -- Optional but useful decomposition fields (allocated)
  sum(c.repairreplaceamount / nullif(cc.county_cnt, 0)) AS fema_repair_replace_amount,
  sum(c.rentalamount / nullif(cc.county_cnt, 0)) AS fema_rental_amount,
  sum(c.otherneedsamount / nullif(cc.county_cnt, 0)) AS fema_other_needs_amount,
  sum(c.totalinspected / nullif(cc.county_cnt, 0)) AS fema_total_inspected

FROM gold_hazard._fema_disaster_county_map m
JOIN gold_hazard._fema_claims_by_disaster c
  ON m.disasternumber = c.disasternumber
JOIN gold_hazard._fema_disaster_county_counts cc
  ON m.disasternumber = cc.disasternumber
 AND m.year = cc.year
GROUP BY 1,2;
