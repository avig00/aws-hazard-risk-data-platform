-- build_risk_feature_mart.sql
-- CTAS build for the versioned Gold table.
-- Reads from risk_feature_mart_view (which defines the ZIP-weighted FEMA logic).
-- Hard-enforce year window at build time: 2010–2023.

CREATE TABLE {{athena_db_gold}}.risk_feature_mart__{{run_ds_nodash}}
WITH (
  external_location = 's3://{{bucket}}/hazard/gold/risk_feature_mart/run_dt={{run_ds}}/',
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
) AS
SELECT *
FROM {{athena_db_gold}}.risk_feature_mart_view
WHERE year BETWEEN 2010 AND 2023;
