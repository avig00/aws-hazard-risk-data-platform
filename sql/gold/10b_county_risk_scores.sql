-- Gold Table 6: county_risk_scores (placeholder for next project outputs)
CREATE EXTERNAL TABLE gold_hazard.county_risk_scores (
  county_fips string,
  year int,
  risk_score double,
  risk_bucket string,
  model_name string,
  model_version string,
  scored_at timestamp,
  top_features_json string,
  shap_summary_json string
)
STORED AS PARQUET
LOCATION 's3://aws-hazard-risk-vigamogh-dev/hazard/gold/county_risk_scores/';
