terraform {
  required_version = ">= 1.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

module "s3" {
  source = "./app/infra/s3"
}

module "glue" {
  source = "./app/infra/glue"
}

module "stepfunctions" {
  source = "./app/infra/stepfunctions"
}

module "eventbridge" {
  source = "./app/infra/eventbridge"
  state_machine_arn = module.stepfunctions.state_machine_arn
}
