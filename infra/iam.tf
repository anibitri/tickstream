# One role per component, each allowed only the exact actions and resources it
# uses. The Go services run locally, so their roles are what they would assume
# on ECS; the Lambda role is used for real.

locals {
  bucket_arn = aws_s3_bucket.archive.arn

  service_policies = {
    archiver = [
      { Action = ["s3:PutObject"], Resource = ["${local.bucket_arn}/trades/*"] },
    ]
    metrics-sink = [
      { Action = ["dynamodb:PutItem"], Resource = [aws_dynamodb_table.latest_metrics.arn, aws_dynamodb_table.feed_health.arn] },
    ]
    alert-bridge = [
      { Action = ["sns:Publish"], Resource = [aws_sns_topic.alerts.arn] },
      # SNS encrypts with the AWS-managed key; publishers need to use it.
      { Action = ["kms:GenerateDataKey", "kms:Decrypt"], Resource = ["arn:aws:kms:${var.region}:${data.aws_caller_identity.current.account_id}:alias/aws/sns"] },
    ]
    api-gateway = [
      { Action = ["dynamodb:GetItem"], Resource = [aws_dynamodb_table.latest_metrics.arn] },
      { Action = ["dynamodb:Query"], Resource = [aws_dynamodb_table.alerts.arn] },
      { Action = ["dynamodb:Scan"], Resource = [aws_dynamodb_table.feed_health.arn] },
      { Action = ["s3:GetObject"], Resource = ["${local.bucket_arn}/backtests/*"] },
      { Action = ["s3:ListBucket"], Resource = [local.bucket_arn], Condition = { StringLike = { "s3:prefix" = ["backtests/*"] } } },
    ]
    replayer = [
      { Action = ["s3:GetObject"], Resource = ["${local.bucket_arn}/trades/*"] },
      { Action = ["s3:ListBucket"], Resource = [local.bucket_arn], Condition = { StringLike = { "s3:prefix" = ["trades/*"] } } },
    ]
    backtester = [
      { Action = ["s3:GetObject"], Resource = ["${local.bucket_arn}/trades/*"] },
      { Action = ["s3:PutObject"], Resource = ["${local.bucket_arn}/backtests/*"] },
      { Action = ["s3:ListBucket"], Resource = [local.bucket_arn], Condition = { StringLike = { "s3:prefix" = ["trades/*"] } } },
    ]
  }
}

resource "aws_iam_role" "service" {
  for_each = local.service_policies
  name     = "${local.name}-${each.key}"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "ecs-tasks.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "service" {
  for_each = local.service_policies
  name     = "least-privilege"
  role     = aws_iam_role.service[each.key].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [for s in each.value : merge({ Effect = "Allow" }, s)]
  })
}

resource "aws_iam_role" "alert_handler" {
  name = "${local.name}-alert-handler"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "alert_handler" {
  name = "least-privilege"
  role = aws_iam_role.alert_handler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["dynamodb:PutItem"], Resource = [aws_dynamodb_table.alerts.arn] },
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = [aws_sqs_queue.alerts.arn]
      },
      { Effect = "Allow", Action = ["sqs:SendMessage"], Resource = [aws_sqs_queue.alerts_dlq.arn] },
      { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = ["${aws_cloudwatch_log_group.alert_handler.arn}:*"] },
      { Effect = "Allow", Action = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"], Resource = ["*"] },
    ]
  })
}
