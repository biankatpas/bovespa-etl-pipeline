# Obtain the existing IAM role etl-role
data "aws_iam_role" "etl_role" {
  name = "LabRole"
}

# Obtain the existing state machine
# data "aws_sfn_state_machine" "bovespa_etl_state_machine" {
#   name = "bovespa-etl-state-machine"
# }

variable "state_machine_arn" {
  description = "ARN of the Step Functions state machine"
  type        = string
}

# EventBridge rule to trigger when an object is created in your S3 bucket
resource "aws_cloudwatch_event_rule" "s3_etl" {
  name        = "etl-s3-rule"
  description = "EventBridge rule to trigger the state machine when a file is uploaded to the raw folder in the bucket"
  event_pattern = jsonencode({
    "source": [
      "aws.s3"
    ],
    "detail-type": [
      "Object Created"
    ],
    "detail": {
      "bucket": {
        "name": [
          "bovespa-etl-360494"
        ]
      },
      "object": {
        "key": [
          { "prefix": "raw/" }
        ]
      }
    }
  })
}

# Permission for EventBridge to invoke the Step Functions state machine
resource "aws_cloudwatch_event_target" "target_step_functions" {
  rule     = aws_cloudwatch_event_rule.s3_etl.name
  arn      = var.state_machine_arn
  role_arn = data.aws_iam_role.etl_role.arn
  input_path = "$"
}

