# Datasets Used in This Project

This platform integrates four major public datasets that together form a realistic multi-source hazard and risk analytics system.

---

## 1. NOAA Storm Events Database
URL: https://www.ncei.noaa.gov/products/storm-events-database  
Format: CSV files per year

Includes:
- Tornado, Hail, Flood, Wildfire, Wind, Lightning, etc.
- Event descriptions, damages, fatalities, injuries

Used For:
- Hazard time-series
- Event-level aggregation in Gold layer

---

## 2. FEMA Disaster Declarations + Housing Assistance
URL: https://www.fema.gov/about/openfema/data-sets

Tables Used:
- Disaster Declarations Summary
- Housing Assistance Registrants

Used For:
- County-level disaster exposure
- Claim counts and assistance amounts

---

## 3. National Risk Index (NRI)
URL: https://hazards.fema.gov/nri/

Includes:
- Expected annual loss
- Exposure values
- Community resilience
- Social vulnerability

Used For:
- ML feature engineering in Gold layer

---

## 4. US Census ACS 5-Year Estimates
URL: https://www.census.gov/data.html

Includes:
- Population demographics
- Socioeconomic indicators
- Housing characteristics

Used For:
- ML feature enrichment
- County-level normalization
