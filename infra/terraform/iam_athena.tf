# iam_athena.tf
#
# Purpose:
#   IAM role + policy for Athena Gold execution.
#   Allows reading Silver, writing Gold, and writing Athena results.

resource "aws_iam_role" "athena_gold_role" {
  name = "athena-gold-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "athena.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "athena_gold_policy" {
  name        = "athena-gold-policy"
  description = "Permissions for Athena Gold CTAS execution"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [

      # -----------------------
      # Athena permissions
      # -----------------------
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution"
        ]
        Resource = "*"
      },

      # -----------------------
      # Read Silver layer
      # -----------------------
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}",
          "arn:aws:s3:::${var.bucket_name}/${var.silver_prefix}/*"
        ]
      },

      # -----------------------
      # Write Gold layer
      # -----------------------
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}",
          "arn:aws:s3:::${var.bucket_name}/${var.gold_prefix}/*"
        ]
      },

      # -----------------------
      # Athena query results
      # -----------------------
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:AbortMultipartUpload",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}",
          "arn:aws:s3:::${var.bucket_name}/${var.athena_results_prefix}*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "athena_gold_attach" {
  role       = aws_iam_role.athena_gold_role.name
  policy_arn = aws_iam_policy.athena_gold_policy.arn
}
