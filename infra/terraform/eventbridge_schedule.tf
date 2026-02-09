resource "aws_cloudwatch_event_rule" "monthly_run" {
  name                = "hazard-risk-agent-monthly"
  description         = "Monthly hazard risk pipeline run"
  schedule_expression = "cron(0 2 1 * ? *)"
}

resource "aws_iam_role" "eventbridge_to_sfn_role" {
  name = "hazard-risk-agent-eventbridge-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_to_sfn_policy" {
  name = "hazard-risk-agent-eventbridge-sfn-policy"
  role = aws_iam_role.eventbridge_to_sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = [aws_sfn_state_machine.hazard_agent_controller.arn]
    }]
  })
}

resource "aws_cloudwatch_event_target" "monthly_target" {
  rule     = aws_cloudwatch_event_rule.monthly_run.name
  arn      = aws_sfn_state_machine.hazard_agent_controller.arn
  role_arn = aws_iam_role.eventbridge_to_sfn_role.arn

  input = jsonencode({
    # a default input object; handlers can override/extend
    bucket_name      = var.bucket_name
    athena_workgroup = var.athena_workgroup
    # run_id will be added by Step Functions execution context; you can also leave blank
  })
}
