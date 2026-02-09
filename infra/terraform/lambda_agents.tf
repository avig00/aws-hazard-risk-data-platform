locals {
  lambda_zip_path = "${path.module}/../../dist/agent_bundle.zip"
}

# Common env vars for all agent lambdas
locals {
  agent_env = {
    # Backward compat (ok to keep)
    BUCKET_NAME = var.bucket_name

    # Required by src/ops/config.py
    PLATFORM_S3_BUCKET = var.bucket_name
    OPS_PREFIX         = var.ops_prefix

    ATHENA_WORKGROUP  = var.athena_workgroup
    ATHENA_RESULTS_S3 = var.athena_results_s3

    ATHENA_DB_BRONZE = var.glue_database_name
    ATHENA_DB_SILVER = var.silver_glue_database_name
    ATHENA_DB_GOLD   = var.gold_glue_database_name
  }
}

resource "aws_lambda_function" "agent_ingestion" {
  function_name    = "hazard-agent-ingestion"
  role             = aws_iam_role.lambda_agents_role.arn
  runtime          = "python3.11"
  handler          = "lambda_handlers.bronze_ingestion_handler.handler"
  filename         = local.lambda_zip_path
  source_code_hash = filebase64sha256(local.lambda_zip_path)
  timeout          = 900

  environment {
    variables = local.agent_env
  }
}

resource "aws_lambda_function" "agent_catalog" {
  function_name    = "hazard-agent-catalog"
  role             = aws_iam_role.lambda_agents_role.arn
  runtime          = "python3.11"
  handler          = "lambda_handlers.catalog_handler.handler"
  filename         = local.lambda_zip_path
  source_code_hash = filebase64sha256(local.lambda_zip_path)
  timeout          = 900

  environment {
    variables = local.agent_env
  }
}

resource "aws_lambda_function" "agent_transform" {
  function_name    = "hazard-agent-transform"
  role             = aws_iam_role.lambda_agents_role.arn
  runtime          = "python3.11"
  handler          = "lambda_handlers.transform_handler.handler"
  filename         = local.lambda_zip_path
  source_code_hash = filebase64sha256(local.lambda_zip_path)
  timeout          = 900

  environment {
    variables = local.agent_env
  }
}

resource "aws_lambda_function" "agent_gold" {
  function_name    = "hazard-agent-gold"
  role             = aws_iam_role.lambda_agents_role.arn
  runtime          = "python3.11"
  handler          = "lambda_handlers.gold_handler.handler"
  filename         = local.lambda_zip_path
  source_code_hash = filebase64sha256(local.lambda_zip_path)
  timeout          = 900

  environment {
    variables = local.agent_env
  }
}

resource "aws_lambda_function" "agent_quality" {
  function_name    = "hazard-agent-quality"
  role             = aws_iam_role.lambda_agents_role.arn
  runtime          = "python3.11"
  handler          = "lambda_handlers.quality_handler.handler"
  filename         = local.lambda_zip_path
  source_code_hash = filebase64sha256(local.lambda_zip_path)
  timeout          = 900

  environment {
    variables = local.agent_env
  }
}
