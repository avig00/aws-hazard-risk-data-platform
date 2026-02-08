-- Gold Table 2: risk_feature_mart
-- Grain: county_fips, year
-- Scaffold-first: universe LEFT JOIN all sources (prevents silent dropping)
CREATE TABLE gold_hazard.risk_feature_mart
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://aws-hazard-risk-vigamogh-dev/hazard/gold/risk_feature_mart/',
  partitioned_by = ARRAY['year']
) AS
SELECT
  u.county_fips,

  -- NOAA (default to 0)
  coalesce(n.noaa_event_count, 0) AS noaa_event_count,
  coalesce(n.noaa_total_fatalities, 0) AS noaa_total_fatalities,
  coalesce(n.noaa_total_injuries, 0) AS noaa_total_injuries,
  coalesce(n.noaa_avg_property_damage, 0) AS noaa_avg_property_damage,

  -- FEMA (default to 0)
  coalesce(fc.fema_valid_registrations, 0) AS fema_valid_registrations,
  coalesce(fc.fema_total_damage, 0) AS fema_total_damage,
  coalesce(fc.fema_total_approved_ihp_amount, 0) AS fema_total_approved_ihp_amount,
  coalesce(fd.fema_declaration_count, 0) AS fema_declaration_count,
  coalesce(fc.fema_repair_replace_amount, 0) AS fema_repair_replace_amount,
  coalesce(fc.fema_rental_amount, 0) AS fema_rental_amount,
  coalesce(fc.fema_other_needs_amount, 0) AS fema_other_needs_amount,
  coalesce(fc.fema_total_inspected, 0) AS fema_total_inspected,

  -- Census (static; leave NULL if missing)
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
  c.acs_year AS census_acs_year,

  -- NRI (static; leave NULL if missing)
  r.nri_risk_score,
  r.nri_eal_score,
  r.nri_sovi_score,
  r.nri_resl_score,

  -- Partition column must be last
  u.year
FROM gold_hazard.county_year_universe u
LEFT JOIN gold_hazard._noaa_county_year n
  ON u.county_fips = n.county_fips
 AND u.year = n.year
LEFT JOIN gold_hazard._fema_claims_county_year fc
  ON u.county_fips = fc.county_fips
 AND u.year = fc.year
LEFT JOIN gold_hazard._fema_decls_county_year fd
  ON u.county_fips = fd.county_fips
 AND u.year = fd.year
LEFT JOIN gold_hazard._census_county_latest c
  ON u.county_fips = c.county_fips
LEFT JOIN gold_hazard._nri_county r
  ON u.county_fips = r.county_fips;
