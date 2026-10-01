output "archive_bucket" {
  description = "S3 bucket holding the trade archive and backtest reports."
  value       = aws_s3_bucket.archive.bucket
}

output "alerts_topic_arn" {
  description = "SNS topic alert-bridge publishes to (ALERTS_SNS_TOPIC_ARN)."
  value       = aws_sns_topic.alerts.arn
}

output "alerts_queue_url" {
  description = "SQS queue that triggers the alert-handler Lambda."
  value       = aws_sqs_queue.alerts.id
}

output "alerts_dlq_url" {
  description = "Dead-letter queue for alerts that failed 3 times."
  value       = aws_sqs_queue.alerts_dlq.id
}

output "tables" {
  description = "DynamoDB table names."
  value = {
    latest_metrics = aws_dynamodb_table.latest_metrics.name
    alerts         = aws_dynamodb_table.alerts.name
    feed_health    = aws_dynamodb_table.feed_health.name
  }
}

output "service_role_arns" {
  description = "Least-privilege IAM role per service."
  value       = merge({ for k, r in aws_iam_role.service : k => r.arn }, { alert-handler = aws_iam_role.alert_handler.arn })
}
