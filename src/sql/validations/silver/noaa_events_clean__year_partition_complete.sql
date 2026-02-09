WITH params AS (
  SELECT
    2000 AS min_year,
    CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER) AS max_year
),
expected_years AS (
  SELECT y AS year
  FROM params
  CROSS JOIN UNNEST(SEQUENCE(min_year, max_year)) AS t(y)
),
actual_years AS (
  SELECT DISTINCT CAST(year AS INTEGER) AS year
  FROM noaa_events_clean
  WHERE year IS NOT NULL
),
missing_years AS (
  SELECT e.year
  FROM expected_years e
  LEFT JOIN actual_years a
    ON e.year = a.year
  WHERE a.year IS NULL
)
SELECT COUNT(*) AS failures
FROM missing_years;
