# iam_crawlers.tf
#
# Purpose:
#   IAM roles used by Glue Crawlers.
#
# Option 1 rollout:
#   - Keep existing crawler role (glue-crawler-hazard-bronze) temporarily.
#   - Create a new crawler role (glue-crawler-hazard) as *_v2.
#   - Switch crawlers to *_v2 in crawlers.tf.
#   - After validation, remove the legacy role resources from this file.

# -----------------------------
# Legacy crawler role (KEEP for now)
# -----------------------------
# These resource names match your current Terraform state (safe refactor).

resource "aws_iam_role" "glue_crawler_role" {
  name = "glue-crawler-hazard-bronze"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "s3_read" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

# -----------------------------
# New crawler role (v2) — clearer name, used for BOTH bronze + silver crawlers
# -----------------------------

resource "aws_iam_role" "glue_crawler_role_v2" {
  name = "glue-crawler-hazard"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_v2" {
  role       = aws_iam_role.glue_crawler_role_v2.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "s3_read_v2" {
  role       = aws_iam_role.glue_crawler_role_v2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}
