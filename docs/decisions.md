# Project Decisions — Phase 0 Lock

## Project Name
AWS Hazard & Risk Data Platform

## Objective
Build a production-style AWS data platform that ingests multi-source public hazard and exposure data, curates it using a medallion architecture, and produces analytics- and ML-ready gold tables.

This project intentionally mirrors real catastrophe risk / climate analytics pipelines.

---

## Scope

### Included Datasets
1. NOAA Storm Events Database (2000–2023, CSV per year)
2. FEMA OpenFEMA
   - Disaster Declarations Summaries
   - Housing Assistance Registrants
3. FEMA National Risk Index (NRI) — County level
4. US Census ACS 5-Year Estimates (County level, via CensusReporter API)

### Geography
- United States
- County-level granularity

### Primary Join Key
- `county_fips`
- Type: STRING
- Format: 5 characters, zero-padded (e.g. "06037")

All Silver and Gold tables must enforce this standard.

---

## Data Grain Decisions

### Silver Layer
- Dataset-native grain (event-level, record-level)

### Gold Layer
1. `gold.hazard_event_summary`
   - Grain: county_fips + year + hazard_type

2. `gold.risk_feature_mart`
   - Grain: county_fips + year
   - Exactly one row per county-year

Duplicate rows at this grain are considered a failure.

---

## Architecture Decisions
- Medallion architecture: Bronze → Silver → Gold
- Storage: Amazon S3 (Parquet for Silver/Gold)
- Metadata: AWS Glue Data Catalog
- Query: Amazon Athena
- Orchestration: MWAA (Airflow)
- IaC: Terraform

---

## Non-Goals (Explicitly Out of Scope)
- Real-time streaming ingestion
- Sub-county geospatial modeling
- ML training or prediction
- RAG / LLM integration
- Cost optimization beyond basic partitioning

These are deferred to later projects.

---

## Definition of Done 
- Gold tables queryable in Athena
- Gold tables documented and validated
- Data is consumable by downstream ML pipelines
