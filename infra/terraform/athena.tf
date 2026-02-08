# athena.tf
#
# Purpose:
#   Define a dedicated Athena workgroup for Gold layer queries.
#   Enforces query result location and isolates Gold CTAS execution.

resource "aws_athena_workgroup" "gold" {
  name        = "athena-gold"
  description = "Athena workgroup for Gold layer CTAS and analytics"

  configuration {
    enforce_workgroup_configuration = false

    result_configuration {
      output_location = "s3://${var.bucket_name}/${var.athena_results_prefix}"
    }
  }
}
