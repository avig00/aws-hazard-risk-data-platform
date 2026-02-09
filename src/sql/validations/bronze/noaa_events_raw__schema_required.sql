WITH required_cols AS (
  SELECT col_name
  FROM (
    VALUES
      ('event_id'),
      ('episode_id'),
      ('state'),
      ('state_fips'),
      ('cz_fips'),
      ('event_type'),
      ('begin_date_time'),
      ('end_date_time'),
      ('year_col')
  ) AS t(col_name)
),
present_cols AS (
  SELECT LOWER(column_name) AS col_name
  FROM information_schema.columns
  WHERE table_schema = current_schema
    AND table_name   = 'noaa_events_raw'
),
missing AS (
  SELECT r.col_name
  FROM required_cols r
  LEFT JOIN present_cols p
    ON r.col_name = p.col_name
  WHERE p.col_name IS NULL
)
SELECT COUNT(*) AS failures
FROM missing;
