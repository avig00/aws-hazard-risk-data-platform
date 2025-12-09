# AWS Hazard & Risk Data Platform

This project builds a fully serverless, production-grade Data Engineering platform on AWS using:
- S3 (Bronze/Silver/Gold medallion architecture)
- AWS Glue (PySpark ETL)
- Athena (SQL query layer)
- MWAA / Airflow (orchestration)
- Terraform (infrastructure-as-code)

## Purpose
To ingest and standardize multi-source hazard and risk datasets (NOAA, FEMA, NRI, Census), transform them into curated analytical tables, and prepare ML-ready Gold data for downstream ML/RAG applications.

## Architecture
- **Bronze:** Raw ingested data (NOAA, FEMA, NRI, Census)
- **Silver:** Cleaned and conformed tables (type-fixing, dedupe, normalization)
- **Gold:** Business-ready data marts (hazard event summary, risk feature mart)

## Infra
All AWS resources are provisioned using Terraform:
- S3 buckets (bronze/silver/gold)
- Glue jobs + crawlers
- IAM roles/policies
- Athena workgroup
- MWAA environment (Airflow)

## Status
This repo contains:
- Architecture design
- Folder structures
- Pseudo-code stubs
- Terraform scaffolding
**Implementation begins after January 20.**
