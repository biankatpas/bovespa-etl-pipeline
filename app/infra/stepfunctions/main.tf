# Obtain the existing IAM role etl-role
data "aws_iam_role" "etl_role" {
  name = "LabRole"
}

# Definition of the Step Functions state machine
resource "aws_sfn_state_machine" "bovespa_etl_state_machine" {
  name     = "bovespa-etl-state-machine"
  role_arn = data.aws_iam_role.etl_role.arn

  definition = jsonencode({
    "Comment" : "Orchestration of ETL jobs using Step Functions",
    "StartAt" : "ExtractJob",
    "States" : {
      "ExtractJob" : {
        "Type" : "Task",
        "Resource" : "arn:aws:states:::glue:startJobRun.sync",
        "Parameters" : {
          "JobName" : "extract"
        },
        "Catch" : [{
          "ErrorEquals" : ["States.ALL"],
          "Next" : "FailState"
        }],
        "Next" : "TransformJob"
      },
      "TransformJob" : {
        "Type" : "Task",
        "Resource" : "arn:aws:states:::glue:startJobRun.sync",
        "Parameters" : {
          "JobName" : "transform"
        },
        "Catch" : [{
          "ErrorEquals" : ["States.ALL"],
          "Next" : "FailState"
        }],
        "Next" : "LoadJob"
      },
      "LoadJob" : {
        "Type" : "Task",
        "Resource" : "arn:aws:states:::glue:startJobRun.sync",
        "Parameters" : {
          "JobName" : "load"
        },
        "Catch" : [{
          "ErrorEquals" : ["States.ALL"],
          "Next" : "FailState"
        }],
        "End" : true
      },
      "FailState" : {
        "Type" : "Fail",
        "Error" : "JobFailed",
        "Cause" : "The job execution failed."
      }
    }
  })
}
