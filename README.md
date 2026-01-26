# AWS Hazard & Risk Data Platform

## Overview
This project builds a **production-style, serverless AWS data engineering platform** for multi-source **hazard, exposure, and risk analytics** using public U.S. datasets.

The platform ingests raw hazard and risk data, curates it using a **Bronze → Silver → Gold medallion architecture**, and produces **analytics- and ML-ready tables** for downstream modeling and decision support.

This project is intentionally designed to mirror **real-world catastrophe risk and climate analytics pipelines** used in insurance, public policy, and risk research.

---

## Core Objectives
- Ingest heterogeneous public datasets (NOAA, FEMA, NRI, Census)
- Standardize and conform them to a shared geographic key
- Produce clean, validated **Gold data marts**
- Enable downstream ML, BI, and RAG use cases (future projects)

---

## Dataset Scope 

All datasets are ingested in their entirety; no pre-filtering is applied at ingestion time.
This project includes **exactly four data sources**:

1. **NOAA Storm Events Database** (2000–2023)  
   - Event-level hazard data (tornado, flood, wildfire, etc.)
2. **FEMA OpenFEMA**
   - Disaster Declarations Summaries
   - Housing Assistance Registrants
3. **FEMA National Risk Index (NRI)**  
   - County-level risk, exposure, and resilience scores
4. **US Census ACS 5-Year Estimates**  
   - County-level socioeconomic and housing indicators

> No additional datasets are included in this project.

---

## Geography & Primary Key 

- **Geography:** United States counties
- **Primary join key:** `county_fips`
  - Type: `STRING`
  - Format: 5 characters, zero-padded (e.g. `"06037"`)

All Silver and Gold tables **must enforce this standard**.

---

## Data Architecture

### Medallion Layers

**Bronze (Raw)**
- Data stored exactly as received
- One S3 prefix per dataset
- Partitioned where inherent (NOAA by year)

**Silver (Curated)**
- snake_case column names
- Correct data types
- Standardized `county_fips`
- Deduplication
- Joinable, clean datasets

**Gold (Analytics / ML)**
- Aggregated and joined tables
- Business-ready grain
- No downstream joins required

---

## Target Tables

### Silver Tables
- `silver.noaa_events_clean`
- `silver.fema_claims_clean`
- `silver.nri_scores_clean`
- `silver.census_clean`

### Gold Tables

#### `gold.hazard_event_summary`
- **Grain:** `county_fips + year + hazard_type`
- Metrics:
  - event_count
  - total_fatalities
  - total_injuries
  - avg_property_damage

#### `gold.risk_feature_mart`
- **Grain:** `county_fips + year`
- One row per county-year
- Combines:
  - Hazard aggregates (NOAA)
  - FEMA exposure signals
  - NRI risk metrics
  - Census socioeconomic features

Duplicate rows at this grain are considered a failure.

---

## AWS Stack

This project uses a **fully serverless AWS architecture**:

- **Amazon S3** – Bronze / Silver / Gold storage
- **AWS Glue** – PySpark ETL jobs + Data Catalog
- **Amazon Athena** – SQL query layer
- **MWAA (Airflow)** – Pipeline orchestration
- **Terraform** – Infrastructure as Code
- **CloudWatch** – Logging and monitoring

---

