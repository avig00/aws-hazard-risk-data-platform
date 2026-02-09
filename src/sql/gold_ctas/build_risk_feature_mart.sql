-- Versioned physical table per run.
-- Template vars: {{bucket}}, {{run_ds}}, {{run_ds_nodash}}

CREATE TABLE risk_feature_mart__{{run_ds_nodash}}
WITH (
  format='PARQUET',
  external_location='s3://{{bucket}}/hazard/gold/risk_feature_mart/run_dt={{run_ds}}/',
  write_compression='SNAPPY'
) AS
SELECT
  *
FROM risk_feature_mart_view;
