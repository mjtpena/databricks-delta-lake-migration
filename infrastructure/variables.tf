variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
}

variable "databricks_token" {
  description = "Databricks API token"
  type        = string
  sensitive   = true
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-west-2"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "etl_node_type" {
  description = "Node type for ETL cluster"
  type        = string
  default     = "i3.xlarge"
}

variable "batch_node_type" {
  description = "Node type for batch processing cluster"
  type        = string
  default     = "i3.2xlarge"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for Delta Lake storage"
  type        = string
}

variable "s3_access_key" {
  description = "AWS access key for S3"
  type        = string
  sensitive   = true
}

variable "s3_secret_key" {
  description = "AWS secret key for S3"
  type        = string
  sensitive   = true
}

variable "legacy_db_password" {
  description = "Password for legacy database access"
  type        = string
  sensitive   = true
}

variable "notification_email" {
  description = "Email for job notifications"
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account ID"
  type        = string
}
