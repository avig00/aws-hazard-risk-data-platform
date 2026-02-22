# Datasets Used in This Project

This platform integrates multiple public datasets that together form a realistic multi-source hazard and risk analytics system.

All datasets are standardized to a consistent geographic grain (county-level, via `county_fips`) and annual time dimension to support downstream analytics and ML modeling.

---

## 1. NOAA Storm Events Database

URL: https://www.ncei.noaa.gov/products/storm-events-database  
Format: CSV files per year  

### Includes:
- Tornado, Hail, Flood, Wildfire, Wind, Lightning, etc.
- Event narratives and descriptions
- Property damage estimates
- Fatalities and injuries
- Event dates and locations

### Used For:
- Hazard event time-series construction
- County-year hazard aggregation
- Gold-layer event summary tables
- Feature engineering for ML risk modeling

---

## 2. FEMA Disaster Declarations + Housing Assistance

URL: https://www.fema.gov/about/openfema/data-sets  

### Tables Used:
- Disaster Declarations Summary
- Housing Assistance Registrants

### Includes:
- Disaster types and declaration metadata
- Incident start and end dates
- Registrant counts
- Assistance amounts

### Used For:
- County-level disaster exposure metrics
- Claims and assistance aggregation
- Socioeconomic impact modeling
- Feature enrichment in Gold risk mart

---

## 3. National Risk Index (NRI)

URL: https://hazards.fema.gov/nri/  

### Includes:
- Expected Annual Loss (EAL)
- Exposure values
- Community resilience indicators
- Social vulnerability indices
- Hazard-specific risk scores

### Used For:
- Baseline risk benchmarking
- ML feature engineering
- Risk normalization inputs
- Comparison against modeled outputs

---

## 4. US Census ACS 5-Year Estimates

URL: https://www.census.gov/data.html  

### Includes:
- Population totals
- Income and poverty indicators
- Housing characteristics
- Demographic breakdowns
- Education and employment statistics

### Used For:
- Socioeconomic feature enrichment
- County-level normalization
- Exposure denominator calculations
- Risk segmentation analysis

---

## 5. ZIP Code to County Mapping (Harvard Dataverse)

Source: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/0U2TCB  
Persistent DOI: 10.7910/DVN/0U2TCB  

### Includes:
- ZIP Code to County FIPS crosswalk
- Geographic linkage metadata
- Standardized county identifiers

### Used For:
- Mapping ZIP-level data to county-level grain
- Standardizing geographic joins across datasets
- Enabling consistent aggregation to the canonical key (`county_fips`)
- Supporting deterministic Bronze → Silver → Gold transformations

---

## Geographic Standardization Strategy

All datasets are transformed to a unified analytical grain:

- **Geographic key:** `county_fips`
- **Time grain:** `year`
- **Hazard segmentation:** `hazard_type` (where applicable)

This ensures:
- Deterministic joins
- No downstream join requirements in Gold marts
- Stable ML-ready feature tables
- Reproducible and idempotent ETL design
- Cost-efficient Athena queries via partition pruning

The resulting Gold tables provide:
- Hazard event summaries
- County-year risk feature mart
- ML-ready extracts without additional joins