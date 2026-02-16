# AWS Hazard & Risk Data Platform

## Overview

This project implements a **production-grade, serverless AWS data engineering platform** for multi-source **hazard, exposure, and risk analytics** using public U.S. datasets.

It follows a strict **Bronze → Silver → Gold medallion architecture**, transforming raw public data into validated, analytics- and ML-ready tables suitable for catastrophe risk modeling, climate analytics, and insurance applications.

The platform is fully deployed using **Terraform** and orchestrated via **AWS Step Functions + Lambda-based agents**.

---

## Project Status

| Phase | Status |
|-------|--------|
| Phase 1 – Infrastructure (Terraform) | ✅ Complete |
| Phase 2 – Bronze Ingestion | ✅ Complete |
| Phase 3 – Silver Curation | ✅ Complete |
| Phase 4 – Gold Analytics | ✅ Complete |
| Phase 5 – Orchestration (Step Functions + Agents) | ✅ Complete & Smoke-Test Verified |

All layers are:

- Infrastructure-as-Code managed
- Fully validated with blocking quality gates
- Orchestrated end-to-end
- Smoke-test verified via Step Functions execution

---

# Architecture Overview

## Core AWS Services

- Amazon S3 (Bronze / Silver / Gold storage)
- AWS Glue (Spark ETL jobs)
- Amazon Athena (CTAS builds + validation queries)
- AWS Lambda (Operational agents)
- AWS Step Functions (Orchestration)
- Terraform (Infrastructure provisioning)

---

# Medallion Architecture

## Bronze Layer (Raw)

**Purpose:** Preserve source data exactly as received.

### Characteristics

- Stored in S3 under dataset-specific prefixes
- Minimal transformation
- Glue crawlers register schemas
- Partitioned where applicable (e.g., NOAA by `year`)
- Basic validation suites (rowcount, schema presence)

Bronze is treated as immutable raw data.

---

## Silver Layer (Curated)

**Purpose:** Cleaned, standardized, analysis-ready datasets.

### Transformations

- snake_case column normalization
- Explicit type casting
- Timestamp parsing
- Standardized `county_fips`
- Deduplication on natural keys
- Partitioning (NOAA by `year`)
- Strict grain enforcement
- Blank-string → NULL normalization
- Required grain-key filtering

### Silver Database

- `silver_hazard_cleaned`


### Silver Tables

- `noaa_events_clean`
- `fema_disaster_declarations_clean`
- `fema_claims_clean`
- `nri_scores_clean`
- `census_clean`

### NOAA Silver Data Integrity Fix

A blocking validation failure revealed a single row with:

- `state IS NULL`
- `state_fips IS NULL`

Permanent fixes implemented:

- Explicit blank-string-to-null conversion
- Strict required-grain-key filtering
- Deterministic drop of null grain rows
- Partition overwrite validation
- Confirmed via Athena inspection and Step Functions smoke test

Silver now enforces deterministic grain integrity.

---

## Gold Layer (Analytics / ML-Ready)

**Purpose:** Business-ready, ML-ready data marts.

### Design Principles

- Explicit grain definitions
- Deterministic row counts
- No downstream joins required
- Blocking uniqueness validation
- Scaffold-based completeness
- CTAS swap-safe builds

---

### Gold Table 1: `hazard_event_summary`

**Grain:**

- `county_fips + year + hazard_type`


**Metrics:**

- event_count
- total_fatalities
- total_injuries
- avg_property_damage

---

### Gold Table 2: `risk_feature_mart`

**Grain:**

- `county_fips + year`


**Inputs:**

- NOAA hazard aggregates
- FEMA declarations
- FEMA claims
- NRI risk scores
- Census socioeconomic features

**Design Rule:**

> One row per county per year. No joins required downstream.

Duplicate grain rows trigger blocking failures.

---

# Orchestration — Step Functions + Agent Pattern

The pipeline is orchestrated using:

- `AWS Step Functions`


State Machine:

- `hazard-risk-agent-controller`

---

## Execution Flow

- `IngestionAgent`
↓
- `CatalogAgent`
↓
- `TransformAgent`
↓
- `GoldMartAgent`
↓
- `QualityAgent`
↓
- `SuccessState`


Any blocking failure routes execution to:

- `FailState`


Each state invokes a dedicated Lambda-based operational agent.

---

# Agent Responsibilities

## BronzeIngestionAgent (Bronze)

- Source readiness checks
- Builds ingestion plan artifact
- Triggers Bronze Glue jobs
- Runs Bronze validation suite
- Syncs Glue Catalog
- Records metadata

---

## TransformAgent (Silver)

- Builds transform plan
- Triggers Silver Glue jobs
- Enforces schema contracts
- Applies grain rules
- Executes Silver validation suite
- Records run metadata

---

## GoldMartAgent (Gold)

- Builds mart plan
- Executes Athena CTAS builds
- Enforces strict grain uniqueness
- Publishes Gold health artifacts

---

## QualityAgent (Cross-layer)

- Executes validation SQL suites
- Produces structured quality reports
- Determines:
  - pass / warn / fail
  - block_downstream decision
- Raises `RuntimeError` on blocking failures

All validation failures immediately halt the state machine.

Quality artifacts are stored in:

- `s3://<bucket>/hazard/ops/run_id=<run_id>/quality/`


---

## CatalogAgent (Cross-layer)

- Starts Glue crawlers when required
- Waits for completion
- Verifies Glue tables exist
- Confirms partitions are registered

---

# Validation Framework

Each validation query must return:

- `0 failure rows`

Validation categories include:

- Grain uniqueness
- Null grain keys
- Year partition completeness
- Non-negative metric checks
- Rowcount consistency
- Join coverage thresholds

Blocking failures set:

- `block_downstream = true`


And trigger pipeline termination.

---

# Smoke Test (Phase 5 Validation)

Script:

- `scripts/run_phase5_smoke.sh`

**Purpose:**

- Validate full Bronze → Silver → Gold pipeline
- Confirm Glue job wiring
- Confirm Lambda deployment
- Confirm Athena permissions
- Confirm validation gating
- Confirm Step Functions state transitions

**Latest execution result:**

- `Execution finished with status: SUCCEEDED`
- `Last state: SuccessState`

This confirms:

- Deterministic orchestration
- Cross-layer validation integrity
- Proper failure gating
- Correct agent wiring
- Stable Silver grain enforcement
- Gold mart uniqueness guarantees

---

# Design Principles

- Explicit grains over implicit joins
- Deterministic CTAS builds
- Hard failure over silent corruption
- Validation as a first-class citizen
- Idempotent ETL jobs
- Agent-based orchestration
- Fully reproducible infrastructure

---

# Future Extensions

This platform now serves as a foundation for:

- ML model training pipelines
- Feature store integration
- Risk scoring APIs
- Text-to-SQL analytical agents
- Interactive dashboards
- Agentic AI analytics systems

The Gold layer is fully ready for downstream ML and AI workloads.
