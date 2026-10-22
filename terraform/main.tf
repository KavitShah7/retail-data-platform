terraform {
  required_version = ">= 1.16.0"
}

resource "terraform_data" "project" {
  input = var.project_name
}

output "project_name" {
  value = terraform_data.project.output
}

data "aws_s3_bucket" "existing_retail_bucket" {
  bucket = var.bucket_name
}

output "existing_bucket_arn" {
  value = data.aws_s3_bucket.existing_retail_bucket.arn
}