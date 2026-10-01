# alert-handler: a Go binary named "bootstrap" on the provided.al2023 runtime.

resource "aws_cloudwatch_log_group" "alert_handler" {
  #checkov:skip=CKV_AWS_158:Logs use the default CloudWatch encryption rather than a customer-managed KMS key.
  #checkov:skip=CKV_AWS_338:Two weeks of Lambda logs is plenty for this project.
  name              = "/aws/lambda/${local.name}-alert-handler"
  retention_in_days = 14
}

resource "aws_lambda_function" "alert_handler" {
  #checkov:skip=CKV_AWS_117:No VPC needed: the function only talks to DynamoDB and a public webhook.
  #checkov:skip=CKV_AWS_272:Code signing needs AWS Signer; out of scope for a sandbox.
  #checkov:skip=CKV_AWS_173:Environment variables hold no secrets except the optional webhook URL.
  function_name    = "${local.name}-alert-handler"
  role             = aws_iam_role.alert_handler.arn
  filename         = var.lambda_zip
  source_code_hash = filebase64sha256(var.lambda_zip)
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = [var.lambda_architecture]
  memory_size      = 128
  timeout          = 10

  # Caps how many copies run at once (protects DynamoDB and the webhook).
  reserved_concurrent_executions = 5

  environment {
    variables = {
      DDB_TABLE_ALERTS  = aws_dynamodb_table.alerts.name
      ALERT_WEBHOOK_URL = var.alert_webhook_url
    }
  }

  tracing_config {
    mode = "Active"
  }

  # Failures of asynchronous invocations; SQS-triggered failures go to the queue's DLQ.
  dead_letter_config {
    target_arn = aws_sqs_queue.alerts_dlq.arn
  }

  depends_on = [aws_cloudwatch_log_group.alert_handler]
}

resource "aws_lambda_event_source_mapping" "alerts" {
  event_source_arn        = aws_sqs_queue.alerts.arn
  function_name           = aws_lambda_function.alert_handler.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"] # retry only the messages that failed
}
