WITH u AS (SELECT count(*) AS universe_rows FROM gold_hazard.county_year_universe),
     m AS (SELECT count(*) AS mart_rows FROM gold_hazard.risk_feature_mart)
SELECT u.universe_rows, m.mart_rows
FROM u CROSS JOIN m
WHERE u.universe_rows <> m.mart_rows;
