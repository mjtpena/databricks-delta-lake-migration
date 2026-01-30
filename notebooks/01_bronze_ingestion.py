# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Data Ingestion
# MAGIC
# MAGIC This notebook ingests raw data from various sources into the Bronze layer (Delta Lake).
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Batch and streaming ingestion
# MAGIC - Schema enforcement
# MAGIC - Data quality checks
# MAGIC - Incremental loads with checkpointing
# MAGIC - Support for 10TB+ daily volumes

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("source_path", "s3://source-bucket/data", "Source Path")
dbutils.widgets.text("checkpoint_path", "s3://checkpoint-bucket/bronze", "Checkpoint Path")

environment = dbutils.widgets.get("environment")
source_path = dbutils.widgets.get("source_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

# Configuration
BRONZE_CATALOG = "delta_migration"
BRONZE_SCHEMA = "bronze"
BATCH_SIZE = 1000000
MAX_FILES_PER_TRIGGER = 100

print(f"Environment: {environment}")
print(f"Source Path: {source_path}")
print(f"Checkpoint Path: {checkpoint_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def add_ingestion_metadata(df):
    """Add metadata columns for tracking"""
    return df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("ingestion_date", current_date()) \
        .withColumn("source_file", input_file_name()) \
        .withColumn("data_hash", sha2(to_json(struct(*[col(c) for c in df.columns])), 256))

def create_bronze_table(table_name, schema, partition_cols=None):
    """Create Bronze Delta table if not exists"""
    full_table_name = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}"

    df = spark.createDataFrame([], schema)
    df = add_ingestion_metadata(df)

    writer = df.write.format("delta") \
        .mode("ignore") \
        .option("delta.enableChangeDataFeed", "true") \
        .option("delta.autoOptimize.optimizeWrite", "true") \
        .option("delta.autoOptimize.autoCompact", "true")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.saveAsTable(full_table_name)
    print(f"Created table: {full_table_name}")

    return full_table_name

def optimize_table(table_name):
    """Optimize Delta table with Z-Ordering"""
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY (ingestion_date)")
    spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")  # 7 days
    print(f"Optimized table: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schemas

# COMMAND ----------

# Events schema
events_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), False),
    StructField("event_name", StringType(), True),
    StructField("properties", MapType(StringType(), StringType()), True),
    StructField("device_type", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("app_version", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True)
])

# Transactions schema
transactions_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("transaction_timestamp", TimestampType(), False),
    StructField("user_id", StringType(), False),
    StructField("amount", DecimalType(18, 2), False),
    StructField("currency", StringType(), False),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), False),
    StructField("merchant_id", StringType(), True),
    StructField("category", StringType(), True)
])

# Users schema
users_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("subscription_tier", StringType(), True),
    StructField("country", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Ingestion - Historical Load

# COMMAND ----------

def ingest_batch_data(source_path, table_name, schema, partition_cols=None):
    """
    Ingest batch data from S3 into Bronze Delta table
    Optimized for large-scale historical loads (10TB+)
    """
    print(f"Starting batch ingestion for {table_name}...")

    # Create table if not exists
    full_table_name = create_bronze_table(table_name, schema, partition_cols)

    # Read data with optimizations
    df = spark.read \
        .format("parquet") \
        .schema(schema) \
        .option("mergeSchema", "false") \
        .option("pathGlobFilter", "*.parquet") \
        .option("recursiveFileLookup", "true") \
        .load(source_path)

    # Add metadata
    df = add_ingestion_metadata(df)

    # Repartition for optimal write performance
    if partition_cols:
        df = df.repartition(*partition_cols)
    else:
        df = df.repartition(200)

    # Write to Delta
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "false") \
        .option("optimizeWrite", "true") \
        .option("autoCompact", "true") \
        .saveAsTable(full_table_name)

    row_count = df.count()
    print(f"Ingested {row_count:,} rows into {full_table_name}")

    # Optimize after ingestion
    optimize_table(full_table_name)

    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Ingestion - Real-time Load

# COMMAND ----------

def ingest_streaming_data(source_path, table_name, schema, partition_cols=None):
    """
    Ingest streaming data from S3/Kinesis into Bronze Delta table
    Auto-scaling for high throughput
    """
    print(f"Starting streaming ingestion for {table_name}...")

    # Create table if not exists
    full_table_name = create_bronze_table(table_name, schema, partition_cols)

    # Read streaming data
    stream_df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/{table_name}/schema") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER) \
        .schema(schema) \
        .load(source_path)

    # Add metadata
    stream_df = add_ingestion_metadata(stream_df)

    # Write stream to Delta with checkpointing
    query = stream_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_path}/{table_name}/checkpoint") \
        .option("mergeSchema", "false") \
        .trigger(processingTime="30 seconds") \
        .toTable(full_table_name)

    print(f"Streaming query started for {full_table_name}")
    print(f"Query ID: {query.id}")

    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Ingestion

# COMMAND ----------

# Batch Ingestion Examples
if environment == "dev":
    # For development, use sample data
    events_count = ingest_batch_data(
        f"{source_path}/events",
        "events",
        events_schema,
        partition_cols=["ingestion_date"]
    )

    transactions_count = ingest_batch_data(
        f"{source_path}/transactions",
        "transactions",
        transactions_schema,
        partition_cols=["ingestion_date"]
    )

    users_count = ingest_batch_data(
        f"{source_path}/users",
        "users",
        users_schema
    )

    print("\n=== Ingestion Summary ===")
    print(f"Events ingested: {events_count:,}")
    print(f"Transactions ingested: {transactions_count:,}")
    print(f"Users ingested: {users_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Ingestion (Uncomment to enable)

# COMMAND ----------

# Streaming ingestion - uncomment to enable
# events_query = ingest_streaming_data(
#     f"{source_path}/streaming/events",
#     "events_stream",
#     events_schema,
#     partition_cols=["ingestion_date"]
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

def run_data_quality_checks(table_name):
    """Run data quality checks on ingested data"""
    full_table_name = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}"

    print(f"\n=== Data Quality Report for {table_name} ===")

    # Row count
    total_rows = spark.table(full_table_name).count()
    print(f"Total rows: {total_rows:,}")

    # Check for duplicates
    df = spark.table(full_table_name)
    id_column = df.columns[0]  # Assume first column is ID
    duplicate_count = df.groupBy(id_column).count().filter("count > 1").count()
    print(f"Duplicate {id_column}s: {duplicate_count:,}")

    # Null checks
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).first()
    print(f"\nNull counts:")
    for column, null_count in null_counts.asDict().items():
        if null_count > 0:
            print(f"  {column}: {null_count:,}")

    # Latest ingestion
    latest_ingestion = df.agg(max("ingestion_timestamp")).first()[0]
    print(f"\nLatest ingestion: {latest_ingestion}")

    # Partition distribution
    if "ingestion_date" in df.columns:
        partition_dist = df.groupBy("ingestion_date").count().orderBy("ingestion_date", ascending=False)
        print(f"\nPartition distribution:")
        partition_dist.show(10, truncate=False)

# Run quality checks
run_data_quality_checks("events")
run_data_quality_checks("transactions")
run_data_quality_checks("users")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Statistics

# COMMAND ----------

# Analyze tables for optimal query performance
spark.sql(f"ANALYZE TABLE {BRONZE_CATALOG}.{BRONZE_SCHEMA}.events COMPUTE STATISTICS")
spark.sql(f"ANALYZE TABLE {BRONZE_CATALOG}.{BRONZE_SCHEMA}.transactions COMPUTE STATISTICS")
spark.sql(f"ANALYZE TABLE {BRONZE_CATALOG}.{BRONZE_SCHEMA}.users COMPUTE STATISTICS")

print("Table statistics computed successfully")
