# iam_stepfunctions.tf

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

resource "aws_iam_role" "lambda_agents_role" {
  name = "hazard-risk-agent-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_agents_policy" {
  name = "hazard-risk-agent-lambda-policy"
  role = aws_iam_role.lambda_agents_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # CloudWatch Logs
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },

      # Start + poll Glue Jobs
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns"
        ]
        Resource = [
          for j in concat(var.glue_bronze_jobs, var.glue_silver_jobs) :
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${j}"
        ]
      },

      # Glue Data Catalog reads (required by CatalogAgent.ensure_table_exists AND Athena partition discovery)
      {
        Effect = "Allow"
        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetPartition",
          "glue:GetPartitions"
        ]
        Resource = "*"
      },

      # Glue Data Catalog writes (needed for Athena CREATE/REPLACE VIEW, which uses Glue CreateTable/UpdateTable)
      {
        Effect = "Allow"
        Action = [
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable"
        ]
        Resource = [
          # Glue sometimes evaluates CreateTable against the catalog resource
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",

          # Scope writes to the Gold database + all tables (views are stored as tables in the catalog)
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.gold_glue_database_name}",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.gold_glue_database_name}/*"
        ]
      },

      # Start + poll Crawlers (CatalogAgent.start_crawler / wait_for_crawler)
      {
        Effect = "Allow"
        Action = [
          "glue:StartCrawler",
          "glue:GetCrawler"
        ]
        Resource = "*"
      },

      # Athena query execution
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetWorkGroup"
        ]
        Resource = "*"
      },

      # S3 platform artifacts (ops + common platform prefixes)
      # - ops: Lambda agent summaries/manifests/logs
      # - bronze/silver/gold: if any Lambda ever needs to read/write dataset outputs directly
      # - code/packages: reading job scripts or dependency zips if needed
      # - athena/results: common spill + query results prefix
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListBucketMultipartUploads"
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}/${local.ops_prefix}*",
          "arn:aws:s3:::${var.bucket_name}/hazard/bronze/*",
          "arn:aws:s3:::${var.bucket_name}/hazard/silver/*",
          "arn:aws:s3:::${var.bucket_name}/hazard/gold/*",
          "arn:aws:s3:::${var.bucket_name}/hazard/code/*",
          "arn:aws:s3:::${var.bucket_name}/hazard/athena/results/*",
          "arn:aws:s3:::${var.bucket_name}/athena-results/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketAcl"
        ]
        Resource = "arn:aws:s3:::${var.bucket_name}"
      }
    ]
  })
}

resource "aws_iam_role" "sfn_role" {
  name = "hazard-risk-agent-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_policy" {
  name = "hazard-risk-agent-sfn-policy"
  role = aws_iam_role.sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["lambda:InvokeFunction"]
        Resource = [
          aws_lambda_function.agent_ingestion.arn,
          aws_lambda_function.agent_catalog.arn,
          aws_lambda_function.agent_transform.arn,
          aws_lambda_function.agent_gold.arn,
          aws_lambda_function.agent_quality.arn
        ]
      }
    ]
  })
}
