# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Time Travel - Advanced Use Cases
# MAGIC
# MAGIC This notebook demonstrates advanced Time Travel scenarios for Delta Lake.
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Audit and compliance tracking
# MAGIC - Reproducing ML model training data
# MAGIC - Rollback accidental changes
# MAGIC - Debugging data pipelines
# MAGIC - Point-in-time analysis

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "delta_migration"
SCHEMA = "silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Audit Trail Analysis

# COMMAND ----------

def analyze_audit_trail(table_name):
    """
    Analyze complete audit trail of table changes
    Track who changed what and when
    """
    print(f"=== Audit Trail Analysis for {table_name} ===\n")

    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    # Get complete history
    history_df = spark.sql(f"DESCRIBE HISTORY {full_table_name}")

    print("Table modification history:")
    history_df.select(
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "userIdentity",
        "operationMetrics"
    ).show(20, truncate=False)

    # Analyze operations by type
    print("\nOperations summary:")
    ops_summary = history_df.groupBy("operation").agg(
        count("*").alias("count"),
        min("timestamp").alias("first_occurrence"),
        max("timestamp").alias("last_occurrence")
    ).orderBy("count", ascending=False)
    ops_summary.show(truncate=False)

    # Analyze data growth over time
    print("\nData growth analysis:")
    growth_df = history_df.select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics.numOutputRows",
        "operationMetrics.numTargetRowsInserted",
        "operationMetrics.numTargetRowsUpdated",
        "operationMetrics.numTargetRowsDeleted"
    ).orderBy("version")
    growth_df.show(20, truncate=False)

    return history_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Version Comparison

# COMMAND ----------

def compare_versions(table_name, version1, version2):
    """
    Compare data between two versions to identify changes
    """
    print(f"=== Comparing versions {version1} and {version2} ===\n")

    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    # Read both versions
    df_v1 = spark.read.format("delta") \
        .option("versionAsOf", version1) \
        .table(full_table_name)

    df_v2 = spark.read.format("delta") \
        .option("versionAsOf", version2) \
        .table(full_table_name)

    # Compare row counts
    count_v1 = df_v1.count()
    count_v2 = df_v2.count()

    print(f"Row count in version {version1}: {count_v1:,}")
    print(f"Row count in version {version2}: {count_v2:,}")
    print(f"Difference: {count_v2 - count_v1:,}")

    # Identify new records in v2
    id_column = df_v1.columns[0]  # Assume first column is ID

    new_records = df_v2.join(df_v1, id_column, "left_anti")
    print(f"\nNew records in version {version2}: {new_records.count():,}")

    # Identify deleted records
    deleted_records = df_v1.join(df_v2, id_column, "left_anti")
    print(f"Deleted records from version {version1}: {deleted_records.count():,}")

    # Show sample of changes
    if new_records.count() > 0:
        print(f"\nSample new records:")
        new_records.show(5)

    return {
        "version1_count": count_v1,
        "version2_count": count_v2,
        "new_records": new_records.count(),
        "deleted_records": deleted_records.count()
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Change Data Feed Analysis

# COMMAND ----------

def analyze_change_data_feed(table_name, start_version, end_version):
    """
    Use Change Data Feed to track all row-level changes
    """
    print(f"=== Change Data Feed Analysis ===\n")

    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    # Enable Change Data Feed if not already enabled
    spark.sql(f"""
        ALTER TABLE {full_table_name}
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

    # Read change data feed
    changes_df = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", start_version) \
        .option("endingVersion", end_version) \
        .table(full_table_name)

    print(f"Changes between version {start_version} and {end_version}:")

    # Analyze change types
    change_summary = changes_df.groupBy("_change_type").agg(
        count("*").alias("count")
    )
    change_summary.show()

    # Show detailed changes by version
    print("\nChanges by version:")
    version_changes = changes_df.groupBy("_commit_version", "_change_type").agg(
        count("*").alias("count")
    ).orderBy("_commit_version", "_change_type")
    version_changes.show(50)

    # Sample of actual changes
    print("\nSample changes:")
    changes_df.select(
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
        *changes_df.columns[:5]  # Show first 5 data columns
    ).show(10, truncate=False)

    return changes_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Point-in-Time Recovery

# COMMAND ----------

def point_in_time_recovery(table_name, target_timestamp):
    """
    Recover data as it existed at a specific point in time
    """
    print(f"=== Point-in-Time Recovery ===\n")

    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    # Query historical data
    historical_df = spark.read.format("delta") \
        .option("timestampAsOf", target_timestamp) \
        .table(full_table_name)

    row_count = historical_df.count()
    print(f"Data as of {target_timestamp}:")
    print(f"Total rows: {row_count:,}")

    # Show schema at that point in time
    print("\nSchema at that time:")
    historical_df.printSchema()

    # Sample data
    print("\nSample data:")
    historical_df.show(10)

    # Option to restore to this version
    print(f"\nTo restore to this version, run:")
    print(f"RESTORE TABLE {full_table_name} TO TIMESTAMP AS OF '{target_timestamp}'")

    return historical_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Reproduce ML Training Data

# COMMAND ----------

def reproduce_ml_training_data(table_name, training_date):
    """
    Reproduce exact dataset used for ML model training
    Critical for model debugging and compliance
    """
    print(f"=== Reproducing ML Training Data ===\n")

    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    # Get data as it existed on training date
    training_data = spark.read.format("delta") \
        .option("timestampAsOf", training_date) \
        .table(full_table_name)

    print(f"Training data as of {training_date}:")
    print(f"Total samples: {training_data.count():,}")

    # Show data statistics
    print("\nData statistics:")
    training_data.describe().show()

    # Save for reproducibility
    output_path = f"/tmp/ml_training_data_{training_date.replace(' ', '_')}"
    training_data.write.format("parquet").mode("overwrite").save(output_path)
    print(f"\nTraining data saved to: {output_path}")

    return training_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Time-Based Analytics

# COMMAND ----------

def time_based_analytics(table_name, analysis_dates):
    """
    Perform time-series analysis across multiple historical versions
    """
    print(f"=== Time-Based Analytics ===\n")

    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    results = []

    for analysis_date in analysis_dates:
        print(f"Analyzing data as of {analysis_date}...")

        try:
            df = spark.read.format("delta") \
                .option("timestampAsOf", analysis_date) \
                .table(full_table_name)

            row_count = df.count()

            # Calculate metrics
            metrics = {
                "date": analysis_date,
                "row_count": row_count
            }

            # Add table-specific metrics
            if "amount_usd" in df.columns:
                metrics["total_revenue"] = df.agg(sum("amount_usd")).first()[0]
                metrics["avg_transaction"] = df.agg(avg("amount_usd")).first()[0]

            if "user_id" in df.columns:
                metrics["unique_users"] = df.select("user_id").distinct().count()

            results.append(metrics)

        except Exception as e:
            print(f"  Error: {e}")

    # Convert to pandas for visualization
    results_df = pd.DataFrame(results)
    print("\nTime-series metrics:")
    print(results_df)

    return results_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Incremental Processing with Time Travel

# COMMAND ----------

def incremental_processing_with_time_travel(table_name, last_processed_version):
    """
    Process only new data since last run using version tracking
    """
    print(f"=== Incremental Processing ===\n")

    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    # Get current version
    current_version = spark.sql(f"DESCRIBE HISTORY {full_table_name}").first()["version"]

    print(f"Last processed version: {last_processed_version}")
    print(f"Current version: {current_version}")

    if current_version <= last_processed_version:
        print("No new data to process")
        return None

    # Read only changes since last processing
    incremental_df = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", last_processed_version + 1) \
        .option("endingVersion", current_version) \
        .table(full_table_name)

    # Filter for inserts and updates only (ignore deletes for this example)
    new_data = incremental_df.filter(col("_change_type").isin(["insert", "update_postimage"]))

    row_count = new_data.count()
    print(f"\nNew/updated rows to process: {row_count:,}")

    if row_count > 0:
        print("\nSample of new data:")
        new_data.drop("_change_type", "_commit_version", "_commit_timestamp").show(10)

    # Save checkpoint for next run
    checkpoint = {
        "last_processed_version": current_version,
        "last_processed_timestamp": datetime.now().isoformat(),
        "rows_processed": row_count
    }

    print(f"\nCheckpoint for next run: {checkpoint}")

    return new_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Data Quality Validation with Time Travel

# COMMAND ----------

def validate_data_quality_over_time(table_name, versions_to_check):
    """
    Track data quality metrics across versions
    Identify when quality degradation occurred
    """
    print(f"=== Data Quality Validation Over Time ===\n")

    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    quality_results = []

    for version in versions_to_check:
        print(f"Checking version {version}...")

        df = spark.read.format("delta") \
            .option("versionAsOf", version) \
            .table(full_table_name)

        # Calculate quality metrics
        total_rows = df.count()

        quality_metrics = {
            "version": version,
            "total_rows": total_rows
        }

        # Null checks
        for col_name in df.columns[:5]:  # Check first 5 columns
            null_count = df.filter(col(col_name).isNull()).count()
            quality_metrics[f"{col_name}_null_pct"] = (null_count / total_rows * 100) if total_rows > 0 else 0

        # Duplicate checks (if first column is ID)
        id_column = df.columns[0]
        duplicate_count = df.groupBy(id_column).count().filter(col("count") > 1).count()
        quality_metrics["duplicate_ids"] = duplicate_count

        quality_results.append(quality_metrics)

    # Convert to pandas for analysis
    quality_df = pd.DataFrame(quality_results)
    print("\nData quality metrics across versions:")
    print(quality_df)

    return quality_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Time Travel Demonstrations

# COMMAND ----------

# 1. Audit Trail Analysis
print("1. AUDIT TRAIL ANALYSIS")
print("="*80)
history = analyze_audit_trail("transactions")
print("\n")

# 2. Version Comparison
print("2. VERSION COMPARISON")
print("="*80)
latest_version = history.first()["version"]
if latest_version >= 2:
    comparison = compare_versions("transactions", latest_version - 1, latest_version)
print("\n")

# 3. Change Data Feed Analysis
print("3. CHANGE DATA FEED ANALYSIS")
print("="*80)
if latest_version >= 2:
    changes = analyze_change_data_feed("transactions", 0, latest_version)
print("\n")

# 4. Point-in-Time Recovery
print("4. POINT-IN-TIME RECOVERY")
print("="*80)
one_hour_ago = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
try:
    historical_data = point_in_time_recovery("transactions", one_hour_ago)
except Exception as e:
    print(f"Historical data not available: {e}")
print("\n")

# 5. Time-Based Analytics
print("5. TIME-BASED ANALYTICS")
print("="*80)
analysis_dates = [
    (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S")
    for i in range(3)
]
try:
    time_series = time_based_analytics("transactions", analysis_dates)
except Exception as e:
    print(f"Time-series analysis error: {e}")
print("\n")

# 6. Incremental Processing
print("6. INCREMENTAL PROCESSING")
print("="*80)
if latest_version >= 1:
    incremental_data = incremental_processing_with_time_travel("transactions", latest_version - 1)
print("\n")

# 7. Data Quality Validation
print("7. DATA QUALITY VALIDATION")
print("="*80)
versions_to_check = list(range(max(0, latest_version - 3), latest_version + 1))
quality_report = validate_data_quality_over_time("transactions", versions_to_check)

print("\n" + "="*80)
print("Time Travel demonstrations completed!")
print("="*80)
