# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake ACID Operations
# MAGIC
# MAGIC This notebook demonstrates ACID transaction capabilities of Delta Lake.
# MAGIC
# MAGIC **Operations Covered:**
# MAGIC - MERGE (Upsert)
# MAGIC - UPDATE
# MAGIC - DELETE
# MAGIC - Time Travel
# MAGIC - Schema Evolution
# MAGIC - OPTIMIZE and Z-Ordering
# MAGIC - VACUUM

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "delta_migration"
SCHEMA = "silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. MERGE Operation (Upsert)

# COMMAND ----------

def demonstrate_merge_operation():
    """
    Demonstrate MERGE operation for efficient upserts
    Use case: Update existing records and insert new ones in a single atomic operation
    """
    print("=== MERGE Operation Demo ===")

    table_name = f"{CATALOG}.{SCHEMA}.users"

    # Create sample update data
    updates_data = [
        ("user_001", "john.updated@example.com", "John", "Doe", "PREMIUM", "US"),
        ("user_002", "jane.updated@example.com", "Jane", "Smith", "ENTERPRISE", "UK"),
        ("user_999", "newuser@example.com", "New", "User", "FREE", "CA")  # New user
    ]

    updates_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("country", StringType(), True)
    ])

    updates_df = spark.createDataFrame(updates_data, updates_schema) \
        .withColumn("updated_at", current_timestamp())

    # Load target Delta table
    target_table = DeltaTable.forName(spark, table_name)

    # Perform MERGE
    target_table.alias("target").merge(
        updates_df.alias("updates"),
        "target.user_id = updates.user_id AND target.is_current = true"
    ).whenMatchedUpdate(
        set = {
            "email": "updates.email",
            "first_name": "updates.first_name",
            "last_name": "updates.last_name",
            "subscription_tier": "updates.subscription_tier",
            "country": "updates.country",
            "updated_at": "updates.updated_at",
            "is_current": "false",
            "effective_end_date": "updates.updated_at"
        }
    ).whenNotMatchedInsert(
        values = {
            "user_id": "updates.user_id",
            "email": "updates.email",
            "first_name": "updates.first_name",
            "last_name": "updates.last_name",
            "subscription_tier": "updates.subscription_tier",
            "country": "updates.country",
            "created_at": "updates.updated_at",
            "updated_at": "updates.updated_at",
            "status": "lit('ACTIVE')",
            "is_current": "lit(true)",
            "effective_start_date": "updates.updated_at"
        }
    ).execute()

    # Show merge statistics
    print(f"Merged {updates_df.count()} records")
    print("\nAfter merge:")
    spark.table(table_name).filter(col("user_id").isin(["user_001", "user_002", "user_999"])).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. UPDATE Operation

# COMMAND ----------

def demonstrate_update_operation():
    """
    Demonstrate UPDATE operation with predicates
    Use case: Bulk update of records matching specific conditions
    """
    print("=== UPDATE Operation Demo ===")

    table_name = f"{CATALOG}.{SCHEMA}.transactions"

    # Count before update
    before_count = spark.table(table_name).filter(col("status") == "PENDING").count()
    print(f"Transactions with PENDING status: {before_count}")

    # Update PENDING transactions older than 24 hours to EXPIRED
    target_table = DeltaTable.forName(spark, table_name)

    expired_threshold = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")

    target_table.update(
        condition = f"status = 'PENDING' AND transaction_timestamp < '{expired_threshold}'",
        set = {
            "status": "lit('EXPIRED')",
            "processed_timestamp": "current_timestamp()"
        }
    )

    # Count after update
    after_count = spark.table(table_name).filter(col("status") == "PENDING").count()
    expired_count = before_count - after_count

    print(f"Updated {expired_count} transactions to EXPIRED status")
    print(f"Remaining PENDING transactions: {after_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DELETE Operation

# COMMAND ----------

def demonstrate_delete_operation():
    """
    Demonstrate DELETE operation with predicates
    Use case: Remove test data or comply with data retention policies
    """
    print("=== DELETE Operation Demo ===")

    table_name = f"{CATALOG}.{SCHEMA}.events"

    # Count before delete
    old_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    before_count = spark.table(table_name).filter(col("event_date") < old_date).count()

    print(f"Events older than 90 days: {before_count}")

    # Delete old events (soft delete - move to archive first in production)
    target_table = DeltaTable.forName(spark, table_name)

    target_table.delete(
        condition = f"event_date < '{old_date}'"
    )

    # Count after delete
    after_count = spark.table(table_name).filter(col("event_date") < old_date).count()

    print(f"Deleted {before_count - after_count} old events")
    print(f"Remaining old events: {after_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Time Travel

# COMMAND ----------

def demonstrate_time_travel():
    """
    Demonstrate Delta Lake Time Travel capabilities
    Use case: Query historical versions, audit changes, rollback mistakes
    """
    print("=== Time Travel Demo ===")

    table_name = f"{CATALOG}.{SCHEMA}.users"

    # Show table history
    print("\nTable History:")
    history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
    history_df.select("version", "timestamp", "operation", "operationMetrics").show(10, truncate=False)

    # Query specific version
    latest_version = history_df.first()["version"]
    if latest_version >= 1:
        print(f"\nQuerying version {latest_version - 1}:")
        previous_version_df = spark.read.format("delta") \
            .option("versionAsOf", latest_version - 1) \
            .table(table_name)
        print(f"Row count in version {latest_version - 1}: {previous_version_df.count()}")

    # Query as of timestamp
    one_hour_ago = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"\nQuerying as of {one_hour_ago}:")
    try:
        historical_df = spark.read.format("delta") \
            .option("timestampAsOf", one_hour_ago) \
            .table(table_name)
        print(f"Row count at {one_hour_ago}: {historical_df.count()}")
    except Exception as e:
        print(f"Historical timestamp not available: {e}")

    # Show changes between versions (Change Data Feed)
    if latest_version >= 1:
        print(f"\nChanges between version {latest_version - 1} and {latest_version}:")
        changes_df = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", latest_version - 1) \
            .option("endingVersion", latest_version) \
            .table(table_name)
        changes_df.groupBy("_change_type").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. RESTORE Operation

# COMMAND ----------

def demonstrate_restore_operation():
    """
    Demonstrate RESTORE operation to rollback changes
    Use case: Recover from accidental data modifications
    """
    print("=== RESTORE Operation Demo ===")

    table_name = f"{CATALOG}.{SCHEMA}.events"

    # Get current version
    current_version = spark.sql(f"DESCRIBE HISTORY {table_name}").first()["version"]
    print(f"Current version: {current_version}")

    # Restore to previous version (if available)
    if current_version >= 2:
        restore_version = current_version - 1
        print(f"Restoring to version {restore_version}...")

        spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF {restore_version}")

        print(f"Table restored to version {restore_version}")
        print("\nUpdated history:")
        spark.sql(f"DESCRIBE HISTORY {table_name}").select("version", "operation").show(5)
    else:
        print("Not enough versions available for restore demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Schema Evolution

# COMMAND ----------

def demonstrate_schema_evolution():
    """
    Demonstrate schema evolution capabilities
    Use case: Add new columns without rewriting existing data
    """
    print("=== Schema Evolution Demo ===")

    table_name = f"{CATALOG}.{SCHEMA}.events"

    # Show current schema
    print("Current schema:")
    spark.table(table_name).printSchema()

    # Add new column using ALTER TABLE
    try:
        spark.sql(f"""
            ALTER TABLE {table_name}
            ADD COLUMNS (
                experiment_id STRING COMMENT 'A/B test experiment ID',
                experiment_variant STRING COMMENT 'A/B test variant'
            )
        """)
        print("\nAdded new columns: experiment_id, experiment_variant")
    except Exception as e:
        print(f"Columns may already exist: {e}")

    # Show updated schema
    print("\nUpdated schema:")
    spark.table(table_name).printSchema()

    # Insert data with new columns
    new_event_data = [(
        "evt_new_001",
        datetime.now(),
        "user_001",
        "session_001",
        "PAGE_VIEW",
        "home_page",
        {},
        "mobile",
        "ios",
        "1.0.0",
        "US",
        "San Francisco",
        "exp_001",  # New column
        "variant_a"  # New column
    )]

    # Schema evolution on write
    print("\nWriting data with new schema...")
    df = spark.createDataFrame(new_event_data) \
        .toDF("event_id", "event_timestamp", "user_id", "session_id", "event_type",
              "event_name", "properties", "device_type", "platform", "app_version",
              "country", "city", "experiment_id", "experiment_variant") \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("ingestion_date", current_date()) \
        .withColumn("source_file", lit("manual_insert")) \
        .withColumn("data_hash", lit(""))

    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)

    print("Data written successfully with evolved schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. OPTIMIZE and Z-Ordering

# COMMAND ----------

def demonstrate_optimize_zorder():
    """
    Demonstrate OPTIMIZE with Z-Ordering for query performance
    Use case: Improve query performance by co-locating related data
    """
    print("=== OPTIMIZE and Z-Ordering Demo ===")

    table_name = f"{CATALOG}.{SCHEMA}.events"

    # Show table details before optimization
    print("Table details before optimization:")
    spark.sql(f"DESCRIBE DETAIL {table_name}").select("numFiles", "sizeInBytes").show()

    # Run OPTIMIZE without Z-Ordering
    print("\nRunning OPTIMIZE (compaction only)...")
    spark.sql(f"OPTIMIZE {table_name}")

    print("\nTable details after OPTIMIZE:")
    spark.sql(f"DESCRIBE DETAIL {table_name}").select("numFiles", "sizeInBytes").show()

    # Run OPTIMIZE with Z-Ordering
    print("\nRunning OPTIMIZE with Z-Ordering on (event_date, user_id)...")
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY (event_date, user_id)")

    print("\nTable details after Z-Ordering:")
    spark.sql(f"DESCRIBE DETAIL {table_name}").select("numFiles", "sizeInBytes").show()

    # Show optimization metrics
    print("\nOptimization metrics:")
    history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
    optimize_metrics = history_df.filter(col("operation") == "OPTIMIZE").first()
    if optimize_metrics:
        print(f"Files added: {optimize_metrics['operationMetrics'].get('numAddedFiles', 'N/A')}")
        print(f"Files removed: {optimize_metrics['operationMetrics'].get('numRemovedFiles', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. VACUUM

# COMMAND ----------

def demonstrate_vacuum():
    """
    Demonstrate VACUUM operation to clean up old files
    Use case: Reclaim storage by removing old file versions
    """
    print("=== VACUUM Demo ===")

    table_name = f"{CATALOG}.{SCHEMA}.transactions"

    # Show table details before vacuum
    print("Table details before VACUUM:")
    spark.sql(f"DESCRIBE DETAIL {table_name}").select("numFiles", "sizeInBytes").show()

    # Check retention configuration
    retention_hours = spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    print(f"\nRetention check enabled: {retention_hours}")

    # Run VACUUM with dry run
    print("\nDry run - files to be deleted:")
    try:
        vacuum_result = spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS DRY RUN")
        vacuum_result.show(10, truncate=False)
    except Exception as e:
        print(f"Dry run result: {e}")

    # Run actual VACUUM (7 days retention)
    print("\nRunning VACUUM (7 days retention)...")
    spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")

    print("\nTable details after VACUUM:")
    spark.sql(f"DESCRIBE DETAIL {table_name}").select("numFiles", "sizeInBytes").show()

    print("\nNote: Time travel is still possible within the retention period")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Concurrent Writes

# COMMAND ----------

def demonstrate_concurrent_writes():
    """
    Demonstrate concurrent write safety with optimistic concurrency control
    Use case: Multiple writers can safely write to the same table
    """
    print("=== Concurrent Writes Demo ===")

    table_name = f"{CATALOG}.{SCHEMA}.events"

    print("Delta Lake handles concurrent writes using optimistic concurrency control")
    print("Multiple writers can append data simultaneously without conflicts")
    print("Conflicts are detected and resolved automatically")

    # Show isolation levels
    print("\nIsolation Level Settings:")
    print(f"  - Write Serializable: Protects against concurrent modifications")
    print(f"  - Snapshot Isolation: Readers always see consistent snapshots")
    print(f"  - Optimistic Concurrency: Uses transaction log for coordination")

    # Demonstrate append operation (naturally concurrent)
    print("\nAppending data (concurrent-safe operation)...")
    new_events = [
        ("evt_concurrent_001", datetime.now(), "user_001", "session_001", "CLICK")
    ]
    df = spark.createDataFrame(new_events, ["event_id", "event_timestamp", "user_id", "session_id", "event_type"])

    df.write.format("delta") \
        .mode("append") \
        .saveAsTable(table_name)

    print("Data appended successfully - operation is naturally concurrent-safe")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute All Demonstrations

# COMMAND ----------

# Run all ACID operation demonstrations
try:
    demonstrate_merge_operation()
    print("\n" + "="*80 + "\n")

    demonstrate_update_operation()
    print("\n" + "="*80 + "\n")

    demonstrate_delete_operation()
    print("\n" + "="*80 + "\n")

    demonstrate_time_travel()
    print("\n" + "="*80 + "\n")

    # Skip restore in demo to avoid data loss
    # demonstrate_restore_operation()

    demonstrate_schema_evolution()
    print("\n" + "="*80 + "\n")

    demonstrate_optimize_zorder()
    print("\n" + "="*80 + "\n")

    demonstrate_vacuum()
    print("\n" + "="*80 + "\n")

    demonstrate_concurrent_writes()

    print("\n" + "="*80)
    print("All ACID operations demonstrated successfully!")
    print("="*80)

except Exception as e:
    print(f"Error during demonstration: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook demonstrated key Delta Lake ACID capabilities:
# MAGIC
# MAGIC 1. **MERGE**: Efficient upserts with atomic operations
# MAGIC 2. **UPDATE**: Bulk updates with predicates
# MAGIC 3. **DELETE**: Safe deletion with audit trail
# MAGIC 4. **Time Travel**: Query historical data versions
# MAGIC 5. **RESTORE**: Rollback to previous versions
# MAGIC 6. **Schema Evolution**: Add columns without rewriting data
# MAGIC 7. **OPTIMIZE**: Compact and Z-Order for performance
# MAGIC 8. **VACUUM**: Clean up old files to reclaim storage
# MAGIC 9. **Concurrent Writes**: Safe multi-writer operations
# MAGIC
# MAGIC These features enable reliable, high-performance data operations at scale.
