resource "aws_s3_bucket" "bovespa_etl_bucket" {
  bucket = "bovespa-etl-360494"
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket      = aws_s3_bucket.bovespa_etl_bucket.id
  eventbridge = true
}

resource "aws_s3_object" "raw_path" {
  bucket = aws_s3_bucket.bovespa_etl_bucket.bucket
  key    = "raw/"
}

resource "aws_s3_object" "interim_path" {
  bucket = aws_s3_bucket.bovespa_etl_bucket.bucket
  key    = "interim/"
}

resource "aws_s3_object" "final_path" {
  bucket = aws_s3_bucket.bovespa_etl_bucket.bucket
  key    = "final/"
}

resource "aws_s3_object" "query_results_path" {
  bucket = aws_s3_bucket.bovespa_etl_bucket.bucket
  key    = "query-results/"
}

resource "aws_s3_object" "scripts_path" {
  bucket = aws_s3_bucket.bovespa_etl_bucket.bucket
  key    = "scripts/"
}
