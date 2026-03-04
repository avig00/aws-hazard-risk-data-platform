-- Versioned physical table per run.
-- Template vars: {{bucket}}, {{run_ds}}, {{run_ds_nodash}}

CREATE TABLE hazard_event_summary__{{run_ds_nodash}}
WITH (
  format='PARQUET',
  external_location='s3://{{bucket}}/{{gold_prefix_root}}/hazard_event_summary/run_dt={{run_ds}}/',
  write_compression='SNAPPY'
) AS
SELECT
  county_fips,
  hazard_type,
  hazard_category,
  event_count,
  total_fatalities,
  total_injuries,
  avg_property_damage,
  year
FROM hazard_event_summary_view;
