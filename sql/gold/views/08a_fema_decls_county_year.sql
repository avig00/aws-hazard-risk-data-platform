-- FEMA Declarations: county-year counts (direct)
CREATE OR REPLACE VIEW gold_hazard._fema_decls_county_year AS
SELECT
  lpad(regexp_replace(cast(county_fips AS varchar), '[^0-9]', ''), 5, '0') AS county_fips,
  cast(fydeclared AS integer) AS year,
  count(*) AS fema_declaration_count
FROM silver_hazard_cleaned.fema_disaster_declarations_clean
WHERE county_fips IS NOT NULL
  AND fydeclared IS NOT NULL
GROUP BY 1,2;
