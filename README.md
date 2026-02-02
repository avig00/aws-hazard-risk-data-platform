# AWS Hazard & Risk Data Platform

## Overview
This project implements a **production-style, serverless AWS data engineering platform** for multi-source **hazard, exposure, and risk analytics** using public U.S. datasets.

It follows a **Bronze → Silver → Gold medallion architecture**, transforming raw public data into **validated, analytics- and ML-ready tables** suitable for catastrophe risk modeling, climate analytics, and insurance use cases.

The design mirrors real-world data pipelines used by catastrophe modeling teams, insurers, and public-sector risk organizations.

---

## Project Status

| Phase | Status |
|-----|------|
| Phase 1 – Infrastructure (Terraform) | ✅ Complete |
| Phase 2 – Bronze Ingestion | ✅ Complete |
| Phase 3 – Silver Curation | ✅ Complete |
| Phase 4 – Gold Analytics | ⬜ Next |

All Silver Glue jobs and crawlers have successfully completed and are queryable via Athena.

---

## Core Objectives
- Ingest heterogeneous public hazard and risk datasets
- Standardize them to a shared geographic and temporal grain
- Apply repeatable validation checks at each layer
- Produce Gold tables with **no downstream joins required**

---

## Dataset Scope

This platform ingests **exactly four public data domains**, in full, without pre-filtering.

### 1. NOAA Storm Events Database
- Event-level hazard data (tornado, flood, wildfire, etc.)
- Coverage: ~2000–2023
- Source for hazard frequency and severity metrics

### 2. FEMA OpenFEMA
- Disaster declarations
- Claims and assistance records

### 3. FEMA National Risk Index (NRI)
- County-level risk, exposure, and resilience scores

### 4. US Census ACS 5-Year Estimates (2022)
- County-level socioeconomic and housing indicators

No additional datasets are included.

---

## Geography & Join Strategy

### Primary Geography
- **United States counties**

### Canonical Join Key
- `county_fips`
  - Type: `STRING`
  - Format: 5 characters, zero-padded (e.g. `"06037"`)

### FEMA Claims Caveat
The `fema_claims_clean` Silver table **does not contain `county_fips`**.

This is expected and not a data quality issue.

- Claims data is retained as an **exposure signal**
- County attribution will be handled in **Gold** using:
  - Disaster declarations
  - State-level aggregation
  - Temporal alignment

This constraint is explicitly addressed in Phase 4 design.

---

## Architecture

### Medallion Layers

#### Bronze (Raw)
- Stored exactly as received
- One S3 prefix per dataset
- Minimal transformation
- Partitioned where applicable (e.g., NOAA by year)

#### Silver (Curated)
- snake_case column names
- Explicit data types
- Standardized `county_fips`
- Deduplication on natural keys
- Lightweight validation checks
- Fully joinable where structurally possible

#### Gold (Analytics / ML)
- Aggregated, business-ready tables
- One row per county per year (or finer where required)
- No downstream joins required
- Designed for BI dashboards and ML feature stores

---

## Silver Layer (Implemented)

All Silver tables are registered in the Glue Catalog under:
- `database: silver_hazard_cleaned`


### Silver Tables

| Table | Description |
|-----|-----------|
| `noaa_events_clean` | Cleaned NOAA storm events (partitioned by year) |
| `fema_disaster_declarations_clean` | FEMA disaster declarations |
| `fema_claims_clean` | FEMA claims and assistance records |
| `nri_scores_clean` | FEMA National Risk Index scores |
| `census_clean` | County-level Census ACS features |

All tables:
- Have validated schemas
- Are mapped to correct S3 locations
- Are queryable via Athena

---

## Silver Validation Checks

Each Silver Glue job applies **production-relevant but lightweight validation**.

### 1. Row Count Checks
- Bronze row count vs Silver row count
- Reductions allowed only for:
  - Deduplication
  - Invalid geographic rows

### 2. Null Rate Checks
Computed for key columns such as:
- `county_fips`
- Primary identifiers
- Core metrics (population, income, etc.)

Logged at job runtime.

### 3. Join Readiness
- `county_fips` standardized to 5-character string
- Partition columns validated
- Data types enforced

---

## Phase 4 — Gold Layer (Next)

### Gold Table 1: `hazard_event_summary`

**Source**
- `noaa_events_clean`

**Grain**
- `county_fips + year + hazard_type`

**Metrics**
- `event_count`
- `total_fatalities`
- `total_injuries`
- `avg_property_damage`

Purpose: provide clean hazard frequency and severity signals.

---

### Gold Table 2: `risk_feature_mart`

**Design Principle**
> One row per county per year — no joins required downstream.

**Grain**
- `county_fips + year`


**Inputs**
- NOAA hazard aggregates
- FEMA disaster and claims signals
- NRI risk metrics
- Census socioeconomic features

This table will serve as the **primary ML feature table**.

Duplicate rows at this grain are considered a **hard failure**.

---

## Gold Validation Strategy (Planned)

Gold tables will enforce:

- Row uniqueness at target grain
- Join success rate (FIPS coverage %)
- Aggregate sanity checks (non-negative counts, plausible ranges)
- Cross-table row consistency

---

## AWS Stack

This project uses a fully serverless AWS stack:

- **Amazon S3** – Bronze, Silver, Gold storage
- **AWS Glue** – PySpark ETL + Data Catalog
- **Amazon Athena** – SQL analytics
- **Terraform** – Infrastructure as Code
- **CloudWatch** – Logging and monitoring
- **MWAA (Airflow)** – Orchestration (planned)

---

## What’s Next
- Build Gold aggregation jobs
- Implement Gold validation queries
- Add example Athena analytics
- Export ML-ready feature sets

---

## Design Philosophy
- Explicit schemas over implicit inference
- Geography and grain defined early
- Validation at every layer
- ML-readiness as a first-class concern

---

## License
This project uses only public datasets and is intended for educational, analytical, and research use.
