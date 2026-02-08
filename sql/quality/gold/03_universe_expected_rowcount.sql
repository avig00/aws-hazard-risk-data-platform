WITH c AS (SELECT count(*) AS counties FROM gold_hazard.county_dim),
     y AS (SELECT count(*) AS years FROM gold_hazard._years_in_scope),
     u AS (SELECT count(*) AS universe_rows FROM gold_hazard.county_year_universe)
SELECT
  c.counties,
  y.years,
  (c.counties * y.years) AS expected_universe_rows,
  u.universe_rows
FROM c CROSS JOIN y CROSS JOIN u
WHERE (c.counties * y.years) <> u.universe_rows;
