# iam_glue.tf
#
# Purpose:
#   IAM role used by Glue Spark Jobs for Phase 3 (Silver).
#   Permissions:
#     - Glue service baseline
#     - Read bronze:      s3://<bucket>/<bronze_prefix>/*
#     - Write silver:     s3://<bucket>/<silver_prefix>/*
#     - Read glue-assets: s3://<bucket>/<assets_prefix>/*
#     - RW temp dir:      s3://<bucket>/<glue_temp_prefix>/*

data "aws_iam_policy_document" "glue_job_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "glue_job_role" {
  name               = var.glue_job_role_name
  assume_role_policy = data.aws_iam_policy_document.glue_job_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_job_service_role" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_job_s3" {
  statement {
    sid     = "ListBucket"
    effect  = "Allow"
    actions = ["s3:ListBucket"]
    resources = [
      "arn:aws:s3:::${var.bucket_name}"
    ]
  }

  statement {
    sid     = "ReadBronze"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "arn:aws:s3:::${var.bucket_name}/${var.bronze_prefix}/*"
    ]
  }

  statement {
    sid     = "WriteSilver"
    effect  = "Allow"
    actions = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [
      "arn:aws:s3:::${var.bucket_name}/${var.silver_prefix}/*"
    ]
  }

  statement {
    sid     = "ReadGlueAssets"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "arn:aws:s3:::${var.bucket_name}/${var.assets_prefix}/*"
    ]
  }

  statement {
    sid    = "TempDirRW"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload"
    ]
    resources = [
      "arn:aws:s3:::${var.bucket_name}/${var.glue_temp_prefix}/*"
    ]
  }
}

resource "aws_iam_policy" "glue_job_s3" {
  name   = "glue-job-hazard-silver-s3"
  policy = data.aws_iam_policy_document.glue_job_s3.json
}

resource "aws_iam_role_policy_attachment" "glue_job_s3_attach" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = aws_iam_policy.glue_job_s3.arn
}
