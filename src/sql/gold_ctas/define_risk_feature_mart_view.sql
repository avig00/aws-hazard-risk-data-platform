-- risk_feature_mart_view
-- One row per county_fips per year, no downstream joins needed.

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
  FROM silver_hazard_cleaned.noaa_events_clean
  WHERE county_fips IS NOT NULL AND LENGTH(TRIM(county_fips)) = 5
    AND year IS NOT NULL
  GROUP BY 1, 2
),

-- FEMA claims mapped to county via declarations, using fydeclared as year
fema_claims_county_year AS (
  SELECT
    TRIM(d.county_fips) AS county_fips,
    CAST(d.fydeclared AS BIGINT) AS year,
    CAST(SUM(COALESCE(c.validregistrations, 0)) AS DOUBLE) AS fema_valid_registrations,
    CAST(SUM(COALESCE(c.totaldamage, 0.0)) AS DOUBLE) AS fema_total_damage,
    CAST(SUM(COALESCE(c.totalapprovedihpamount, 0.0)) AS DOUBLE) AS fema_total_approved_ihp_amount,
    COUNT(DISTINCT d.disasternumber) AS fema_declaration_count,
    CAST(SUM(COALESCE(c.repairreplaceamount, 0.0)) AS DOUBLE) AS fema_repair_replace_amount,
    CAST(SUM(COALESCE(c.rentalamount, 0.0)) AS DOUBLE) AS fema_rental_amount,
    CAST(SUM(COALESCE(c.otherneedsamount, 0.0)) AS DOUBLE) AS fema_other_needs_amount,
    CAST(SUM(COALESCE(c.totalinspected, 0)) AS DOUBLE) AS fema_total_inspected
  FROM silver_hazard_cleaned.fema_claims_clean c
  JOIN silver_hazard_cleaned.fema_disaster_declarations_clean d
    ON c.disasternumber = d.disasternumber
   AND c.state = d.state
  WHERE d.county_fips IS NOT NULL AND LENGTH(TRIM(d.county_fips)) = 5
    AND d.fydeclared IS NOT NULL
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
  FROM silver_hazard_cleaned.nri_scores_clean
  WHERE county_fips IS NOT NULL AND LENGTH(TRIM(county_fips)) = 5
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
    FROM silver_hazard_cleaned.census_clean
    WHERE county_fips IS NOT NULL AND LENGTH(TRIM(county_fips)) = 5
      AND acs_year IS NOT NULL
  ) t
  WHERE rn = 1
),

-- Universe: include any county-year seen in NOAA or FEMA
universe AS (
  SELECT county_fips, year FROM noaa_county_year
  UNION
  SELECT county_fips, year FROM fema_claims_county_year
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
