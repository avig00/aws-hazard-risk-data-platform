# AWS Hazard & Risk Data Platform

## Overview
This project implements a **production-style, serverless AWS data engineering platform** for multi-source **hazard, exposure, and risk analytics** using public U.S. datasets.

It follows a **Bronze → Silver → Gold medallion architecture**, transforming raw public data into **validated, analytics- and ML-ready tables** suitable for catastrophe risk modeling, climate analytics, and insurance use cases.

The design mirrors real-world data pipelines used by catastrophe modeling teams, insurers, and public-sector risk organizations.

This project’s scope is **data platform engineering only**.  
Modeling, ranking, and natural-language agents are intentionally deferred to a **separate downstream project** that consumes this platform’s Gold layer.

---

## Project Status

| Phase | Status |
|-----|------|
| Phase 1 – Infrastructure (Terraform) | ✅ Complete |
| Phase 2 – Bronze Ingestion | ✅ Complete |
| Phase 3 – Silver Curation | ✅ Complete |
| Phase 4 – Gold Analytics | ✅ **Complete & Validated** |
| Phase 5 – Orchestration | ⬜ Planned |

All Bronze, Silver, and Gold tables are fully built, validated, and queryable via Athena.

---

## Core Objectives
- Ingest heterogeneous public hazard and risk datasets
- Standardize them to a shared geographic and temporal grain
- Apply repeatable validation checks at each layer
- Produce Gold tables with **no downstream joins required**
- Ensure all analytics outputs are **agent- and ML-ready**

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

### 4. US Census ACS 5-Year Estimates
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
- County attribution is handled in **Gold** using:
  - FEMA disaster declarations
  - Disaster-to-county mappings
  - Year-level alignment

This constraint is explicitly addressed in the Gold layer design.

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
- Deterministic grains with enforced uniqueness
- Scaffold-first design (no silent row dropping)
- Designed for BI, ML feature extraction, and downstream agents

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

All Silver tables:
- Have validated schemas
- Are mapped to correct S3 locations
- Are queryable via Athena

---

## Phase 4 — Gold Layer (Completed)

Phase 4 builds the **analytics- and ML-ready Gold layer**, enforcing strict grains, explicit scaffolding, and hard validation rules.

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

Property damage values are parsed from NOAA string formats (`K / M / B`) into numeric USD.

---

### Gold Table 2: `risk_feature_mart`

**Design Principle**
> One row per county per year — no downstream joins required.

**Grain**
- `county_fips + year`

**Inputs**
- NOAA hazard aggregates
- FEMA disaster declarations
- FEMA claims and assistance signals
- NRI risk and resilience scores
- Census socioeconomic features (latest ACS per county)

This table is the **canonical analytics and ML feature mart**.

Duplicate rows at this grain are treated as a **hard failure**.

---

### Gold Supporting Tables

#### `county_dim`
- One row per county
- Human-readable county and state metadata
- Normalized county names for agent and UI use

#### `hazard_type_map`
- Stable mapping from NOAA `event_type` → hazard group
- Enables robust rollups (e.g., “flood events”) without brittle string matching
- Stored as a seeded external table

#### `county_year_universe`
- Complete scaffold of `county_fips × year`
- Ensures zero-event years exist explicitly
- Prevents silent row loss in downstream joins

#### `county_risk_scores`
- Placeholder Gold table for downstream model outputs
- Schema-only in this project
- Populated in the next project by ML pipelines

---

## Gold Views (Deterministic)

Gold views provide reusable, deterministic rollups:

- NOAA county-year aggregates
- FEMA claims and declaration rollups
- Static county-level Census and NRI features

All views are:
- Single-statement Athena definitions
- Idempotent
- Used exclusively by Gold CTAS jobs

---

## Gold Validation Framework (Implemented)

Phase 4 includes a **strict, automated validation framework**.

### Validation Principles
- Each validation query must return **0 rows** to pass
- Any non-zero result is treated as a **hard failure**
- Failures stop the pipeline immediately

### Validation Categories
- Grain uniqueness checks
- Non-negative metric checks
- Scaffold-to-mart rowcount equality
- Join coverage thresholds
- Parsing and type sanity checks

### Execution
- Validations are executed by a strict runner script
- Results are evaluated programmatically
- Phase 4 completion requires **all validations to pass**

---

## Operational Agents (Agentic DataOps)

This platform is **agent-ready**, without embedding LLMs into ETL logic.

Agentic behavior is confined to **operational decision-making**, not data transformation.

### Agent Roles
- **IngestionAgent** – source freshness and Bronze triggers
- **TransformAgent** – Silver job coordination and schema enforcement
- **GoldMartAgent** – Gold builds and validation gating
- **QualityAgent** – validation execution and anomaly detection
- **CatalogAgent** – Glue Catalog and partition integrity

All agents interact only with deterministic tools:
- Glue jobs
- Athena queries
- S3 metadata
- Validation outputs

---

## AWS Stack

- **Amazon S3** – Bronze, Silver, Gold storage
- **AWS Glue** – PySpark ETL + Data Catalog
- **Amazon Athena** – SQL analytics and CTAS
- **Terraform** – Infrastructure as Code
- **CloudWatch** – Logging and monitoring
- **MWAA (Airflow)** – Orchestration (Phase 5)

---

## What’s Next (Phase 5)
- Introduce Airflow-based orchestration
- Encode DAG-level dependencies and retries
- Integrate validation gating into orchestration
- Add run metadata and observability

Downstream ML, ranking, and NL agents are **explicitly out of scope** for this repository.

---

## Design Philosophy
- Explicit schemas over implicit inference
- Geography and grain defined early
- Validation as a first-class concern
- Idempotent, restart-safe pipelines
- Gold as a contract, not a convenience

---

## License
This project uses only public datasets and is intended for educational, analytical, and research use.
