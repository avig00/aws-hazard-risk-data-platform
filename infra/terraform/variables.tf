# variables.tf
#
# Purpose:
#   Central Terraform variables for the Hazard Risk data platform.
#   This file defines:
#     - S3 bucket + path prefixes (bronze/silver/assets/temp)
#     - Glue databases (bronze/silver)
#     - Glue job role + runtime sizing
#     - Glue workflow naming

variable "bucket_name" {
  type        = string
  description = "S3 bucket name for the project"
}

# -----------------------
# Glue Databases
# -----------------------
variable "glue_database_name" {
  type        = string
  description = "Glue database name for bronze layer"
  default     = "bronze_hazard_raw"
}

variable "silver_glue_database_name" {
  type        = string
  description = "Glue database name for silver layer"
  default     = "silver_hazard_cleaned"
}

# -----------------------
# S3 Prefixes (folder roots)
# -----------------------
variable "bronze_prefix" {
  type        = string
  description = "S3 prefix for bronze layer root"
  default     = "hazard/bronze"
}

variable "silver_prefix" {
  type        = string
  description = "S3 prefix for silver layer root"
  default     = "hazard/silver"
}

variable "assets_prefix" {
  type        = string
  description = "S3 prefix where Glue job scripts + libs are uploaded"
  default     = "hazard/glue-assets"
}

variable "glue_temp_prefix" {
  type        = string
  description = "S3 prefix for Glue TempDir (Spark spills, bookmarks, etc.)"
  default     = "hazard/glue-tmp"
}

# -----------------------
# IAM Role names
# -----------------------
variable "glue_job_role_name" {
  type        = string
  description = "IAM role name for Glue Spark jobs (Silver ETL)"
  default     = "glue-job-hazard-silver"
}

# -----------------------
# Glue Job Runtime Settings
# -----------------------
variable "glue_version" {
  type        = string
  description = "Glue version for Spark jobs"
  default     = "4.0"
}

variable "glue_worker_type" {
  type        = string
  description = "Glue worker type (e.g., G.1X, G.2X)"
  default     = "G.1X"
}

variable "glue_workers_default" {
  type        = number
  description = "Default number of workers for most Silver jobs"
  default     = 2
}

variable "glue_workers_noaa" {
  type        = number
  description = "Workers for NOAA details clean job (largest dataset)"
  default     = 5
}

variable "glue_timeout_minutes" {
  type        = number
  description = "Timeout for Glue jobs in minutes"
  default     = 60
}

# -----------------------
# Glue Workflow
# -----------------------
variable "glue_workflow_name" {
  type        = string
  description = "Glue workflow name for Phase 3 Silver"
  default     = "silver-workflow"
}
