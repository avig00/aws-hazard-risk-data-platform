# Data Contracts — Bronze, Silver, Gold

## Bronze Layer (Raw Zone)

### Rules
- Data stored exactly as received
- No schema modification
- One S3 prefix per dataset
- Partition only where inherent (NOAA by year)

### Bronze Prefixes
- bronze/noaa/year=YYYY/
- bronze/fema/
- bronze/nri/
- bronze/census/

### Bronze Exit Criteria
- Files present in S3
- Glue tables created
- Athena can query raw data

---

## Silver Layer (Curated Zone)

### Global Rules
- Column names in snake_case
- Correct data types (dates, numeric fields)
- Standardized county_fips
- Duplicate records removed
- Invalid records logged or dropped

### Silver Tables
- silver.noaa_events_clean
- silver.fema_claims_clean
- silver.nri_scores_clean
- silver.census_clean

### Silver Exit Criteria
- Parquet output
- Glue table registered
- Athena queryable
- Row counts validated
- county_fips join success > 99%

---

## Gold Layer (Analytics / ML)

### gold.hazard_event_summary
Derived from NOAA Silver

Columns (core):
- county_fips
- year
- hazard_type
- event_count
- total_fatalities
- total_injuries
- avg_property_damage

Grain:
- county_fips + year + hazard_type

---

### gold.risk_feature_mart
Joined from:
- NOAA hazard_event_summary
- FEMA aggregated claims
- NRI scores
- Census socioeconomic features

Grain:
- county_fips + year

Rules:
- Exactly one row per county-year
- No joins required downstream

---

## Validation Rules
- No null county_fips in Silver or Gold
- No duplicate primary keys in Gold
- Numeric columns within expected ranges
