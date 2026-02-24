# AWS Hazard & Risk Data Platform — Architecture Overview

This document describes the full architecture of the project, from ingestion to orchestration to analytical consumption.

---

## 1. High-Level Data Flow

Raw Data → Bronze (S3) → Silver (Curated) → Gold (Analytics & ML)  
Glue Jobs (PySpark) are orchestrated using MWAA/Airflow. Terraform provisions all infra.

---

## 2. Bronze Layer (Raw Zone)

Raw data from NOAA, FEMA, NRI, and Census is stored exactly as received.

- S3 Prefixes:
  - s3://hazard/bronze/noaa/{year}/
  - s3://hazard/bronze/fema/
  - s3://hazard/bronze/nri/
  - s3://hazard/bronze/census/

Schemas are registered in the Glue Data Catalog.

---

## 3. Silver Layer (Cleansed & Standardized)

Transforms applied:
- Column normalization (snake_case)
- Type corrections (dates, floats, ints)
- Standardize county codes (FIPS)
- Remove duplicates
- Build event_id and county_id keys

Silver Tables:
- silver.noaa_events_clean
- silver.fema_disaster_declarations_clean
- silver.fema_claims_clean
- silver.nri_scores_clean
- silver.census_clean

---

## 4. Gold Layer (Business-Ready Marts)

### 4.1 hazard_event_summary (Gold)
- county_fips
- year
- hazard_type
- event_count
- fatalities, injuries
- avg_property_damage

### 4.2 risk_feature_mart (Gold)
Aggregate and join:
- NOAA hazard stats
- FEMA claim stats
- NRI hazard scores
- Census socioeconomic features

This table is used by Project 2 ML pipeline.

---

## 5. Orchestration — Step Functions + Lambda

This platform uses **AWS Step Functions** as the orchestration layer, invoking **Lambda-based agents** for each stage of the pipeline. Each agent performs a focused responsibility (ingest, catalog, transform, validate, publish), and Step Functions enforces ordering, retries, and hard-fail behavior.

Primary workflow:
- **Agent Controller State Machine** (Step Functions)
  - **IngestionAgent (Lambda)**: triggers Bronze ingestion jobs and writes run manifests
  - **CatalogAgent (Lambda)**: runs Glue crawlers / catalog refresh
  - **TransformAgent (Lambda)**: executes Silver transformations
  - **GoldMartAgent (Lambda)**: builds Gold marts and `_current` views
  - **QualityAgent (Lambda)**: runs validation checks; blocks promotion on failure

Why this approach:
- Serverless orchestration with explicit state visibility (every transition is traceable)
- Deterministic build → validate → promote pattern for stable downstream contracts
- Clear separation of concerns via agent-style Lambdas

---

## 6. Infra — Terraform

Terraform manages:
- S3 bucket structure
- IAM roles/policies
- Glue jobs + triggers
- Glue Crawlers
- Athena Workgroup

---

## 7. Analytics Layer — Athena

Athena provides:
- Ad-hoc exploration
- BI-ready SQL queries
- ML training data extraction

---

## 8. RAG/LLM Support (Project 2 Interface)

Gold mart optionally feeds:
- RAG embeddings (OpenSearch Serverless)
- LLM applications (via Bedrock)

Project 1 prepares:
- Clean structured data
- Document storage locations
