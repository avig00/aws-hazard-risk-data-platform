-- Census latest ACS snapshot per county (static join)
CREATE OR REPLACE VIEW gold_hazard._census_county_latest AS
WITH ranked AS (
  SELECT
    lpad(regexp_replace(cast(county_fips AS varchar), '[^0-9]', ''), 5, '0') AS county_fips,
    acs_year,
    population_total,
    median_household_income,
    median_home_value,
    employment_universe_total,
    in_labor_force,
    unemployed,
    education_universe_total,
    high_school_grad,
    bachelors,
    graduate_degree,
    row_number() OVER (
      PARTITION BY lpad(regexp_replace(cast(county_fips AS varchar), '[^0-9]', ''), 5, '0')
      ORDER BY acs_year DESC
    ) AS rn
  FROM silver_hazard_cleaned.census_clean
  WHERE county_fips IS NOT NULL
    AND acs_year IS NOT NULL
)
SELECT
  county_fips,
  acs_year,
  population_total,
  median_household_income,
  median_home_value,
  employment_universe_total,
  in_labor_force,
  unemployed,
  education_universe_total,
  high_school_grad,
  bachelors,
  graduate_degree
FROM ranked
WHERE rn = 1;
