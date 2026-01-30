terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5.0"

  backend "s3" {
    bucket = "databricks-terraform-state"
    key    = "delta-lake-migration/terraform.tfstate"
    region = "us-west-2"
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

provider "aws" {
  region = var.aws_region
}

# Databricks Workspace Configuration
resource "databricks_workspace_conf" "this" {
  custom_config = {
    "enableDbfsFileBrowser" : true
    "enableNotebookTableClipboard" : true
  }
}

# High-Concurrency Cluster for ETL Workloads
resource "databricks_cluster" "etl_cluster" {
  cluster_name            = "delta-lake-etl-cluster"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = var.etl_node_type
  autotermination_minutes = 30
  autoscale {
    min_workers = 2
    max_workers = 20
  }

  spark_conf = {
    "spark.databricks.delta.preview.enabled" : "true"
    "spark.databricks.delta.optimizeWrite.enabled" : "true"
    "spark.databricks.delta.autoCompact.enabled" : "true"
    "spark.sql.adaptive.enabled" : "true"
    "spark.sql.adaptive.coalescePartitions.enabled" : "true"
    "spark.databricks.io.cache.enabled" : "true"
    "spark.databricks.delta.merge.enableLowShuffle" : "true"
  }

  custom_tags = {
    Environment = var.environment
    Project     = "delta-lake-migration"
    CostCenter  = "data-engineering"
  }

  init_scripts {
    dbfs {
      destination = "dbfs:/databricks/scripts/install-dependencies.sh"
    }
  }
}

# Job Cluster for Batch Processing
resource "databricks_cluster" "batch_cluster" {
  cluster_name            = "delta-lake-batch-cluster"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = var.batch_node_type
  autotermination_minutes = 15
  autoscale {
    min_workers = 5
    max_workers = 50
  }

  spark_conf = {
    "spark.databricks.delta.preview.enabled" : "true"
    "spark.databricks.delta.optimizeWrite.enabled" : "true"
    "spark.databricks.delta.autoCompact.enabled" : "true"
    "spark.sql.adaptive.enabled" : "true"
    "spark.sql.adaptive.coalescePartitions.enabled" : "true"
    "spark.databricks.io.cache.enabled" : "true"
    "spark.sql.files.maxPartitionBytes" : "134217728" # 128MB
    "spark.sql.shuffle.partitions" : "auto"
  }

  custom_tags = {
    Environment = var.environment
    Project     = "delta-lake-migration"
    CostCenter  = "data-engineering"
    WorkloadType = "batch"
  }
}

# SQL Analytics Endpoint
resource "databricks_sql_endpoint" "analytics" {
  name             = "delta-lake-analytics"
  cluster_size     = "2X-Small"
  max_num_clusters = 3

  tags {
    custom_tags = {
      Environment = var.environment
      Project     = "delta-lake-migration"
    }
  }

  enable_photon              = true
  enable_serverless_compute  = true
  spot_instance_policy       = "COST_OPTIMIZED"
}

# Secret Scope for Credentials
resource "databricks_secret_scope" "migration" {
  name                     = "delta-lake-migration"
  initial_manage_principal = "users"
}

resource "databricks_secret" "s3_access_key" {
  key          = "s3-access-key"
  string_value = var.s3_access_key
  scope        = databricks_secret_scope.migration.name
}

resource "databricks_secret" "s3_secret_key" {
  key          = "s3-secret-key"
  string_value = var.s3_secret_key
  scope        = databricks_secret_scope.migration.name
}

resource "databricks_secret" "legacy_db_password" {
  key          = "legacy-db-password"
  string_value = var.legacy_db_password
  scope        = databricks_secret_scope.migration.name
}

# DBFS Mount for S3 Data Lake
resource "databricks_mount" "data_lake" {
  name = "data-lake"

  s3 {
    bucket_name = var.s3_bucket_name
    instance_profile = aws_iam_instance_profile.databricks.arn
  }
}

# External Location for Unity Catalog
resource "databricks_external_location" "delta_tables" {
  name            = "delta-tables"
  url             = "s3://${var.s3_bucket_name}/delta-tables"
  credential_name = databricks_storage_credential.external.id
  comment         = "External location for Delta Lake tables"
}

# Storage Credential for Unity Catalog
resource "databricks_storage_credential" "external" {
  name = "delta-lake-storage-cred"
  aws_iam_role {
    role_arn = aws_iam_role.unity_catalog.arn
  }
  comment = "Managed by Terraform"
}

# Catalog and Schemas
resource "databricks_catalog" "main" {
  name         = "delta_migration"
  comment      = "Main catalog for Delta Lake migration"
  storage_root = "s3://${var.s3_bucket_name}/delta-tables"

  properties = {
    purpose = "migration"
  }
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.main.name
  name         = "bronze"
  comment      = "Raw ingestion layer"

  properties = {
    layer = "bronze"
  }
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.main.name
  name         = "silver"
  comment      = "Cleaned and conformed layer"

  properties = {
    layer = "silver"
  }
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.main.name
  name         = "gold"
  comment      = "Business-level aggregates"

  properties = {
    layer = "gold"
  }
}

# Delta Tables
resource "databricks_sql_table" "events_bronze" {
  name               = "${databricks_schema.bronze.name}.events"
  catalog_name       = databricks_catalog.main.name
  schema_name        = databricks_schema.bronze.name
  table_type         = "MANAGED"
  data_source_format = "DELTA"
  storage_location   = "${databricks_external_location.delta_tables.url}/bronze/events"

  column {
    name = "event_id"
    type = "STRING"
  }
  column {
    name = "event_timestamp"
    type = "TIMESTAMP"
  }
  column {
    name = "user_id"
    type = "STRING"
  }
  column {
    name = "event_type"
    type = "STRING"
  }
  column {
    name = "payload"
    type = "STRING"
  }
  column {
    name = "ingestion_timestamp"
    type = "TIMESTAMP"
  }

  comment = "Raw events from source systems"
}

# Job for ETL Pipeline
resource "databricks_job" "etl_pipeline" {
  name = "delta-lake-etl-pipeline"

  job_cluster {
    job_cluster_key = "etl_cluster"
    new_cluster {
      num_workers   = 10
      spark_version = data.databricks_spark_version.latest_lts.id
      node_type_id  = var.etl_node_type

      spark_conf = {
        "spark.databricks.delta.optimizeWrite.enabled" : "true"
        "spark.databricks.delta.autoCompact.enabled" : "true"
      }
    }
  }

  task {
    task_key = "bronze_ingestion"

    notebook_task {
      notebook_path = "/Workspace/notebooks/01_bronze_ingestion"
      base_parameters = {
        environment = var.environment
      }
    }

    job_cluster_key = "etl_cluster"
  }

  task {
    task_key = "silver_transformation"
    depends_on {
      task_key = "bronze_ingestion"
    }

    notebook_task {
      notebook_path = "/Workspace/notebooks/02_silver_transformation"
      base_parameters = {
        environment = var.environment
      }
    }

    job_cluster_key = "etl_cluster"
  }

  task {
    task_key = "gold_aggregation"
    depends_on {
      task_key = "silver_transformation"
    }

    notebook_task {
      notebook_path = "/Workspace/notebooks/03_gold_aggregation"
      base_parameters = {
        environment = var.environment
      }
    }

    job_cluster_key = "etl_cluster"
  }

  schedule {
    quartz_cron_expression = "0 0 2 * * ?"
    timezone_id            = "America/Los_Angeles"
    pause_status           = "UNPAUSED"
  }

  email_notifications {
    on_failure = [var.notification_email]
    on_success = [var.notification_email]
  }

  max_concurrent_runs = 1
  timeout_seconds     = 86400
}

# Data source for latest LTS Spark version
data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

# Outputs
output "etl_cluster_id" {
  value       = databricks_cluster.etl_cluster.id
  description = "ID of the ETL cluster"
}

output "batch_cluster_id" {
  value       = databricks_cluster.batch_cluster.id
  description = "ID of the batch processing cluster"
}

output "sql_endpoint_id" {
  value       = databricks_sql_endpoint.analytics.id
  description = "ID of the SQL Analytics endpoint"
}

output "catalog_name" {
  value       = databricks_catalog.main.name
  description = "Name of the Delta Lake catalog"
}
