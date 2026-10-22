variable "project_name" {
  description = "Name of our Retail Data Platform project"
  type        = string
  default     = "retail-data-platform-v2"
}

variable "bucket_name" {
  description = "Existing S3 bucket used by the pipeline (set in an untracked .tfvars file or TF_VAR_bucket_name)"
  type        = string
}
