# s3_lifecycle.tf
#
# Purpose:
#   S3 lifecycle rules to keep the bucket clean:
#     - expire Athena query results after N days (athena-results/)
#     - expire legacy/alternate Athena results prefix (hazard/athena/results/)
#     - expire Glue temp spills after N days
#     - abort incomplete multipart uploads

resource "aws_s3_bucket_lifecycle_configuration" "cleanup" {
  bucket = var.bucket_name

  # ---------------------------
  # Rule 1: Athena query results (current)
  # ---------------------------
  rule {
    id     = "expire-athena-query-results"
    status = "Enabled"

    filter {
      prefix = var.athena_results_prefix
    }

    expiration {
      days = var.athena_results_expire_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_multipart_upload_days
    }
  }

  # ---------------------------
  # Rule 2: Athena query results (alternate/legacy prefix)
  # ---------------------------
  rule {
    id     = "expire-athena-query-results-hazard-prefix"
    status = "Enabled"

    filter {
      prefix = var.athena_results_prefix_hazard
    }

    expiration {
      days = var.athena_results_expire_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_multipart_upload_days
    }
  }

  # ---------------------------
  # Rule 3: Glue temp directory
  # ---------------------------
  rule {
    id     = "expire-glue-temp"
    status = "Enabled"

    filter {
      prefix = var.glue_temp_prefix
    }

    expiration {
      days = var.glue_temp_expire_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_multipart_upload_days
    }
  }
}
