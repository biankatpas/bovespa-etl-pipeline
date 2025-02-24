variable "role_name" {
  default = "LabRole"
}

variable "script_location" {
  default = "s3://bovespa-etl-360494/scripts/"
}

variable "default_arguments" {
  type = map(string)
  default = {
    "--job-bookmark-option"       = "job-bookmark-disable"
    "--enable-glue-datacatalog"   = "true"
    "--additional-python-modules" = "ipeadatapy"
  }
}