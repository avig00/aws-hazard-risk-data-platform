-- Gold Table 1: hazard_event_summary
-- Grain: county_fips, year, hazard_type (hazard_type = event_type)
CREATE TABLE gold_hazard.hazard_event_summary
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://aws-hazard-risk-vigamogh-dev/hazard/gold/hazard_event_summary/',
  partitioned_by = ARRAY['year']
) AS
WITH typed AS (
  SELECT
    lpad(regexp_replace(cast(county_fips AS varchar), '[^0-9]', ''), 5, '0') AS county_fips,
    year(begin_date_time) AS year,
    event_type AS hazard_type,
    COALESCE(category, 'UNKNOWN') AS hazard_category,

    coalesce(cast(deaths_direct   AS integer), 0) AS deaths_direct,
    coalesce(cast(deaths_indirect AS integer), 0) AS deaths_indirect,
    coalesce(cast(injuries_direct   AS integer), 0) AS injuries_direct,
    coalesce(cast(injuries_indirect AS integer), 0) AS injuries_indirect,

    -- Parse damage_property: supports K/M/B plus plain numeric; strips $ and commas.
    CASE
      WHEN damage_property IS NULL OR trim(damage_property) = '' THEN NULL
      ELSE
        CASE upper(substr(trim(regexp_replace(damage_property, '[$,]', '')), -1))
          WHEN 'K' THEN try_cast(
                        substr(trim(regexp_replace(damage_property, '[$,]', '')), 1,
                               length(trim(regexp_replace(damage_property, '[$,]', ''))) - 1
                        ) AS double
                      ) * 1000
          WHEN 'M' THEN try_cast(
                        substr(trim(regexp_replace(damage_property, '[$,]', '')), 1,
                               length(trim(regexp_replace(damage_property, '[$,]', ''))) - 1
                        ) AS double
                      ) * 1000000
          WHEN 'B' THEN try_cast(
                        substr(trim(regexp_replace(damage_property, '[$,]', '')), 1,
                               length(trim(regexp_replace(damage_property, '[$,]', ''))) - 1
                        ) AS double
                      ) * 1000000000
          ELSE try_cast(trim(regexp_replace(damage_property, '[$,]', '')) AS double)
        END
    END AS property_damage_usd
  FROM silver_hazard_cleaned.noaa_events_clean
  WHERE county_fips IS NOT NULL
    AND begin_date_time IS NOT NULL
    AND event_type IS NOT NULL
)
SELECT
  county_fips,
  hazard_type,
  hazard_category,
  count(*) AS event_count,
  sum(deaths_direct + deaths_indirect) AS total_fatalities,
  sum(injuries_direct + injuries_indirect) AS total_injuries,
  avg(property_damage_usd) AS avg_property_damage,
  year
FROM typed
GROUP BY 1,2,3,8;
