resource "aws_sfn_state_machine" "hazard_agent_controller" {
  name     = "hazard-risk-agent-controller"
  role_arn = aws_iam_role.sfn_role.arn

  definition = jsonencode({
    Comment = "Hazard Risk Agent Orchestration: Bronze -> Catalog -> Silver -> Gold -> QA"
    StartAt = "IngestionAgent"
    States = {
      IngestionAgent = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.agent_ingestion.arn
          "Payload.$"  = "$"
        }

        # Keep only what you care about from the invoke response (optional but recommended)
        ResultSelector = {
          "payload.$"    = "$.Payload"
          "statusCode.$" = "$.StatusCode"
          "requestId.$"  = "$.SdkResponseMetadata.RequestId"
        }
        ResultPath = "$.ingestion_result"

        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 5
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "FailState"
        }]
        Next = "CatalogAgent"
      }

      CatalogAgent = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.agent_catalog.arn
          "Payload.$"  = "$"
        }

        ResultSelector = {
          "payload.$"    = "$.Payload"
          "statusCode.$" = "$.StatusCode"
          "requestId.$"  = "$.SdkResponseMetadata.RequestId"
        }
        ResultPath = "$.catalog_result"

        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 5
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "FailState"
        }]
        Next = "TransformAgent"
      }

      TransformAgent = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.agent_transform.arn
          "Payload.$"  = "$"
        }

        ResultSelector = {
          "payload.$"    = "$.Payload"
          "statusCode.$" = "$.StatusCode"
          "requestId.$"  = "$.SdkResponseMetadata.RequestId"
        }
        ResultPath = "$.transform_result"

        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 10
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "FailState"
        }]
        Next = "GoldMartAgent"
      }

      GoldMartAgent = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.agent_gold.arn
          "Payload.$"  = "$"
        }

        ResultSelector = {
          "payload.$"    = "$.Payload"
          "statusCode.$" = "$.StatusCode"
          "requestId.$"  = "$.SdkResponseMetadata.RequestId"
        }
        ResultPath = "$.gold_result"

        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 10
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "FailState"
        }]
        Next = "QualityAgent"
      }

      QualityAgent = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.agent_quality.arn
          "Payload.$"  = "$"
        }

        ResultSelector = {
          "payload.$"    = "$.Payload"
          "statusCode.$" = "$.StatusCode"
          "requestId.$"  = "$.SdkResponseMetadata.RequestId"
        }
        ResultPath = "$.quality_result"

        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 10
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "FailState"
        }]
        Next = "SuccessState"
      }

      SuccessState = { Type = "Succeed" }
      FailState    = { Type = "Fail" }
    }
  })
}
