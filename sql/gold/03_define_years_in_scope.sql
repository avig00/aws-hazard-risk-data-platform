-- Helper view: years in scope (NOAA + FEMA declarations)
CREATE OR REPLACE VIEW gold_hazard._years_in_scope AS
WITH years AS (
  SELECT DISTINCT year(begin_date_time) AS year
  FROM silver_hazard_cleaned.noaa_events_clean
  WHERE begin_date_time IS NOT NULL

  UNION

  SELECT DISTINCT cast(fydeclared AS integer) AS year
  FROM silver_hazard_cleaned.fema_disaster_declarations_clean
  WHERE fydeclared IS NOT NULL
)
SELECT year
FROM years
WHERE year IS NOT NULL
ORDER BY year;
