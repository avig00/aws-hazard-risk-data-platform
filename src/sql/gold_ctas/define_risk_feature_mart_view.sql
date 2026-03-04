-- risk_feature_mart_view
-- One row per county_fips per year, no downstream joins needed.
-- Year window enforced: 2010–2023

CREATE OR REPLACE VIEW risk_feature_mart_view AS
WITH
-- NOAA county-year features (rolled up across all hazard types)
noaa_county_year AS (
  SELECT
    TRIM(county_fips) AS county_fips,
    CAST(year AS BIGINT) AS year,
    COUNT(*) AS noaa_event_count,
    SUM(CAST(COALESCE(deaths_direct, 0) + COALESCE(deaths_indirect, 0) AS BIGINT)) AS noaa_total_fatalities,
    SUM(CAST(COALESCE(injuries_direct, 0) + COALESCE(injuries_indirect, 0) AS BIGINT)) AS noaa_total_injuries,
    AVG(
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
      END
    ) AS noaa_avg_property_damage
  FROM {{athena_db_silver}}.noaa_events_clean
  WHERE county_fips IS NOT NULL AND LENGTH(TRIM(county_fips)) = 5
    AND substr(TRIM(county_fips), 1, 2) NOT IN ('00', '03')
    AND substr(TRIM(county_fips), 3, 3) <> '000'
    AND year IS NOT NULL
  GROUP BY 1, 2
),

-- FEMA claims mapped to county via ZIP crosswalk, using fydeclared as year (2010–2023)
fema_claims_county_year AS (
  WITH decl AS (
    -- Disaster/year universe, deduped
    SELECT DISTINCT
      disasternumber,
      TRIM(state) AS state,
      CAST(fydeclared AS BIGINT) AS year
    FROM {{athena_db_silver}}.fema_disaster_declarations_clean
    WHERE disasternumber IS NOT NULL
      AND state IS NOT NULL AND TRIM(state) <> ''
      AND fydeclared IS NOT NULL
      AND CAST(fydeclared AS BIGINT) BETWEEN 2010 AND 2023
  ),
  claims AS (
    -- Normalize zipcode to 5-char string (keeps leading zeros)
    -- NOTE: Use regex extraction to safely handle cases where zipcode is not strictly 5 digits.
    SELECT
      disasternumber,
      TRIM(state) AS state,
      LPAD(REGEXP_EXTRACT(TRIM(CAST(zipcode AS VARCHAR)), '([0-9]{5})', 1), 5, '0') AS zip5,
      CAST(COALESCE(validregistrations, 0) AS DOUBLE) AS validregistrations,
      CAST(COALESCE(totaldamage, 0.0) AS DOUBLE) AS totaldamage,
      CAST(COALESCE(totalapprovedihpamount, 0.0) AS DOUBLE) AS totalapprovedihpamount,
      CAST(COALESCE(repairreplaceamount, 0.0) AS DOUBLE) AS repairreplaceamount,
      CAST(COALESCE(rentalamount, 0.0) AS DOUBLE) AS rentalamount,
      CAST(COALESCE(otherneedsamount, 0.0) AS DOUBLE) AS otherneedsamount,
      CAST(COALESCE(totalinspected, 0) AS DOUBLE) AS totalinspected
    FROM {{athena_db_silver}}.fema_claims_clean
    WHERE disasternumber IS NOT NULL
      AND state IS NOT NULL AND TRIM(state) <> ''
      AND zipcode IS NOT NULL
  ),
  xwalk AS (
    SELECT
      TRIM(zip5) AS zip5,
      TRIM(county_fips) AS county_fips,
      CAST(year AS BIGINT) AS year,
      CAST(tot_ratio AS DOUBLE) AS tot_ratio
    FROM {{athena_db_silver}}.zip2county_xwalk_clean
    WHERE year BETWEEN '2010' AND '2023'
      AND county_fips IS NOT NULL AND LENGTH(TRIM(county_fips)) = 5
      -- Basic FIPS sanity (avoid placeholder/invalid)
      AND substr(TRIM(county_fips), 1, 2) NOT IN ('00', '03')
      AND substr(TRIM(county_fips), 3, 3) <> '000'
  )
  SELECT
    x.county_fips,
    d.year,
    SUM(COALESCE(c.validregistrations, 0.0) * COALESCE(x.tot_ratio, 1.0)) AS fema_valid_registrations,
    SUM(COALESCE(c.totaldamage, 0.0) * COALESCE(x.tot_ratio, 1.0)) AS fema_total_damage,
    SUM(COALESCE(c.totalapprovedihpamount, 0.0) * COALESCE(x.tot_ratio, 1.0)) AS fema_total_approved_ihp_amount,
    COUNT(DISTINCT d.disasternumber) AS fema_declaration_count,
    SUM(COALESCE(c.repairreplaceamount, 0.0) * COALESCE(x.tot_ratio, 1.0)) AS fema_repair_replace_amount,
    SUM(COALESCE(c.rentalamount, 0.0) * COALESCE(x.tot_ratio, 1.0)) AS fema_rental_amount,
    SUM(COALESCE(c.otherneedsamount, 0.0) * COALESCE(x.tot_ratio, 1.0)) AS fema_other_needs_amount,
    SUM(COALESCE(c.totalinspected, 0.0) * COALESCE(x.tot_ratio, 1.0)) AS fema_total_inspected
  FROM claims c
  JOIN decl d
    ON c.disasternumber = d.disasternumber
   AND c.state = d.state
  JOIN xwalk x
    ON c.zip5 = x.zip5
   AND d.year = x.year
  WHERE d.year BETWEEN 2010 AND 2023
  GROUP BY 1, 2
),

-- NRI county-level scores (NOTE: this table has both countyfips (3-digit) and county_fips (5-digit).
-- We MUST use county_fips for joins.
nri_county AS (
  SELECT
    TRIM(county_fips) AS county_fips,
    CAST(risk_score AS DOUBLE) AS nri_risk_score,
    CAST(eal_score  AS DOUBLE) AS nri_eal_score,
    CAST(sovi_score AS DOUBLE) AS nri_sovi_score,
    CAST(resl_score AS DOUBLE) AS nri_resl_score
  FROM {{athena_db_silver}}.nri_scores_clean
  WHERE county_fips IS NOT NULL AND LENGTH(TRIM(county_fips)) = 5
    AND substr(TRIM(county_fips), 1, 2) NOT IN ('00', '03')
    AND substr(TRIM(county_fips), 3, 3) <> '000'
),

-- Census latest ACS per county (assumes census_clean has these output columns + acs_year + county_fips)
census_latest AS (
  SELECT *
  FROM (
    SELECT
      TRIM(county_fips) AS county_fips,
      CAST(population_total AS BIGINT) AS population_total,
      CAST(median_household_income AS BIGINT) AS median_household_income,
      CAST(median_home_value AS BIGINT) AS median_home_value,
      CAST(employment_universe_total AS BIGINT) AS employment_universe_total,
      CAST(in_labor_force AS BIGINT) AS in_labor_force,
      CAST(unemployed AS BIGINT) AS unemployed,
      CAST(education_universe_total AS BIGINT) AS education_universe_total,
      CAST(high_school_grad AS BIGINT) AS high_school_grad,
      CAST(bachelors AS BIGINT) AS bachelors,
      CAST(graduate_degree AS BIGINT) AS graduate_degree,
      CAST(acs_year AS INT) AS census_acs_year,
      ROW_NUMBER() OVER (PARTITION BY TRIM(county_fips) ORDER BY CAST(acs_year AS INT) DESC) AS rn
    FROM {{athena_db_silver}}.census_clean
    WHERE county_fips IS NOT NULL AND LENGTH(TRIM(county_fips)) = 5
      AND substr(TRIM(county_fips), 1, 2) NOT IN ('00', '03')
      AND substr(TRIM(county_fips), 3, 3) <> '000'
      AND acs_year IS NOT NULL
  ) t
  WHERE rn = 1
),

-- Universe: include any county-year seen in NOAA or FEMA, limited to 2010–2023
universe AS (
  SELECT county_fips, year
  FROM noaa_county_year
  WHERE year BETWEEN 2010 AND 2023

  UNION

  SELECT county_fips, year
  FROM fema_claims_county_year
  WHERE year BETWEEN 2010 AND 2023
)

SELECT
  u.county_fips,

  -- NOAA
  COALESCE(n.noaa_event_count, 0) AS noaa_event_count,
  COALESCE(n.noaa_total_fatalities, 0) AS noaa_total_fatalities,
  COALESCE(n.noaa_total_injuries, 0) AS noaa_total_injuries,
  COALESCE(n.noaa_avg_property_damage, 0.0) AS noaa_avg_property_damage,

  -- FEMA
  COALESCE(f.fema_valid_registrations, 0.0) AS fema_valid_registrations,
  COALESCE(f.fema_total_damage, 0.0) AS fema_total_damage,
  COALESCE(f.fema_total_approved_ihp_amount, 0.0) AS fema_total_approved_ihp_amount,
  COALESCE(f.fema_declaration_count, 0) AS fema_declaration_count,
  COALESCE(f.fema_repair_replace_amount, 0.0) AS fema_repair_replace_amount,
  COALESCE(f.fema_rental_amount, 0.0) AS fema_rental_amount,
  COALESCE(f.fema_other_needs_amount, 0.0) AS fema_other_needs_amount,
  COALESCE(f.fema_total_inspected, 0.0) AS fema_total_inspected,

  -- Census (latest per county)
  c.population_total,
  c.median_household_income,
  c.median_home_value,
  c.employment_universe_total,
  c.in_labor_force,
  c.unemployed,
  c.education_universe_total,
  c.high_school_grad,
  c.bachelors,
  c.graduate_degree,
  c.census_acs_year,

  -- NRI
  r.nri_risk_score,
  r.nri_eal_score,
  r.nri_sovi_score,
  r.nri_resl_score,

  u.year
FROM universe u
LEFT JOIN noaa_county_year n
  ON u.county_fips = n.county_fips AND u.year = n.year
LEFT JOIN fema_claims_county_year f
  ON u.county_fips = f.county_fips AND u.year = f.year
LEFT JOIN census_latest c
  ON u.county_fips = c.county_fips
LEFT JOIN nri_county r
  ON u.county_fips = r.county_fips;
