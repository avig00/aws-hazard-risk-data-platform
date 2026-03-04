-- risk_feature_mart__year_window_2010_2023
-- PASS condition: returns 0 rows
-- FAIL condition: returns 1 row if any out-of-window rows exist

SELECT 1 AS fail
WHERE EXISTS (
  SELECT 1
  FROM {{validation_table_risk_feature_mart}}
  WHERE CAST(year AS BIGINT) < 2010
     OR CAST(year AS BIGINT) > 2023
);
