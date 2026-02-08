-- Disaster → counties mapping from declarations (distinct counties per disaster-year)
CREATE OR REPLACE VIEW gold_hazard._fema_disaster_county_map AS
SELECT DISTINCT
  cast(disasternumber AS bigint) AS disasternumber,
  lpad(regexp_replace(cast(county_fips AS varchar), '[^0-9]', ''), 5, '0') AS county_fips,
  cast(fydeclared AS integer) AS year
FROM silver_hazard_cleaned.fema_disaster_declarations_clean
WHERE disasternumber IS NOT NULL
  AND county_fips IS NOT NULL
  AND fydeclared IS NOT NULL;
