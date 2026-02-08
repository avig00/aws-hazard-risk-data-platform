-- Gold Table 5: county_year_universe (scaffold)
CREATE TABLE gold_hazard.county_year_universe
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://aws-hazard-risk-vigamogh-dev/hazard/gold/county_year_universe/',
  partitioned_by = ARRAY['year']
) AS
SELECT
  d.county_fips,
  y.year
FROM gold_hazard.county_dim d
CROSS JOIN gold_hazard._years_in_scope y;
