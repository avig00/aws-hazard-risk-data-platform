# Medallion Architecture Plan

---

## Bronze Layer — Raw Zone
Rules:
- Store raw as-is
- Partition where available (e.g., NOAA by year)
- One prefix per dataset

Sources:
- NOAA CSVs
- FEMA CSV/JSON
- NRI CSV
- Census CSV

---

## Silver Layer — Curated Zone

Transforms:
- Normalize headers
- Fix dtype issues
- Clean nulls
- Remove duplicates
- Standardize FIPS codes
- Create surrogate keys

Outputs:
- silver.noaa_events_clean
- silver.fema_claims_clean
- silver.nri_scores_clean
- silver.census_clean

---

## Gold Layer — Aggregated & ML-Ready Zone

Tables:

### 1. hazard_event_summary
- event_count
- total_fatalities
- total_injuries
- property_damage
- hazard_type
- county_fips
- year

### 2. risk_feature_mart
Combined:
- hazard_event_summary
- fema_claims_summary
- nri_scores
- census socioeconomic features

Purpose:
- ML feature store
- BI-ready analytics
