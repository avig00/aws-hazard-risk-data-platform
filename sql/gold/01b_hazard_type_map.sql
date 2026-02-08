-- Gold Table 4: hazard_type_map 
CREATE EXTERNAL TABLE gold_hazard.hazard_type_map (
  event_type       string,
  hazard_group     string,
  hazard_subgroup  string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar'     = '"',
  'escapeChar'    = '\\'
)
LOCATION 's3://aws-hazard-risk-vigamogh-dev/hazard/gold/_seeds/hazard_type_map/'
TBLPROPERTIES ('skip.header.line.count'='1');
