# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Transformation
# MAGIC
# MAGIC This notebook transforms Bronze data into cleaned, validated Silver layer.
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Data cleansing and validation
# MAGIC - Schema evolution
# MAGIC - SCD Type 2 implementation
# MAGIC - Data deduplication
# MAGIC - Business rule enforcement

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("process_date", "", "Process Date (YYYY-MM-DD)")

environment = dbutils.widgets.get("environment")
process_date = dbutils.widgets.get("process_date")

if not process_date:
    process_date = datetime.now().strftime("%Y-%m-%d")

BRONZE_CATALOG = "delta_migration"
BRONZE_SCHEMA = "bronze"
SILVER_CATALOG = "delta_migration"
SILVER_SCHEMA = "silver"

print(f"Environment: {environment}")
print(f"Process Date: {process_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def create_silver_table(table_name, df_sample):
    """Create Silver Delta table with SCD Type 2 columns"""
    full_table_name = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{table_name}"

    # Add SCD Type 2 columns
    df_with_scd = df_sample \
        .withColumn("effective_start_date", current_timestamp()) \
        .withColumn("effective_end_date", lit(None).cast(TimestampType())) \
        .withColumn("is_current", lit(True)) \
        .withColumn("record_hash", lit("")) \
        .withColumn("processed_timestamp", current_timestamp())

    df_with_scd.write.format("delta") \
        .mode("ignore") \
        .option("delta.enableChangeDataFeed", "true") \
        .option("delta.autoOptimize.optimizeWrite", "true") \
        .option("delta.autoOptimize.autoCompact", "true") \
        .saveAsTable(full_table_name)

    print(f"Created Silver table: {full_table_name}")
    return full_table_name

def calculate_record_hash(df, key_columns):
    """Calculate hash for change detection"""
    non_key_cols = [c for c in df.columns if c not in key_columns]
    return df.withColumn(
        "record_hash",
        sha2(to_json(struct(*[col(c) for c in non_key_cols])), 256)
    )

def upsert_scd_type2(source_df, target_table, key_columns):
    """
    Perform SCD Type 2 upsert
    - Insert new records
    - Update changed records (close old, insert new)
    - Skip unchanged records
    """
    print(f"Starting SCD Type 2 upsert for {target_table}...")

    # Calculate hash for source
    source_df = calculate_record_hash(source_df, key_columns)
    source_df = source_df \
        .withColumn("effective_start_date", current_timestamp()) \
        .withColumn("effective_end_date", lit(None).cast(TimestampType())) \
        .withColumn("is_current", lit(True)) \
        .withColumn("processed_timestamp", current_timestamp())

    # Create temp view
    source_df.createOrReplaceTempView("source_data")

    # Load target Delta table
    target_delta = DeltaTable.forName(spark, target_table)

    # Build merge condition
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])
    merge_condition += " AND target.is_current = true"

    # Perform merge
    target_delta.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        condition = "target.record_hash <> source.record_hash",
        set = {
            "is_current": "false",
            "effective_end_date": "source.processed_timestamp"
        }
    ).whenNotMatchedInsertAll().execute()

    # Insert new versions of updated records
    updated_records = spark.sql(f"""
        SELECT source.*
        FROM source_data source
        INNER JOIN {target_table} target
        ON {" AND ".join([f"target.{col} = source.{col}" for col in key_columns])}
        WHERE target.is_current = false
        AND target.effective_end_date = source.processed_timestamp
    """)

    if updated_records.count() > 0:
        updated_records.write.format("delta").mode("append").saveAsTable(target_table)

    print(f"SCD Type 2 upsert completed for {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Events

# COMMAND ----------

def transform_events():
    """Transform events from Bronze to Silver"""
    print("Transforming events...")

    # Read from Bronze
    events_bronze = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.events") \
        .filter(col("ingestion_date") == process_date)

    # Data cleansing and transformation
    events_silver = events_bronze \
        .filter(col("event_id").isNotNull()) \
        .filter(col("event_timestamp").isNotNull()) \
        .dropDuplicates(["event_id"]) \
        .withColumn("event_date", to_date(col("event_timestamp"))) \
        .withColumn("event_hour", hour(col("event_timestamp"))) \
        .withColumn("user_id", trim(col("user_id"))) \
        .withColumn("event_type", upper(trim(col("event_type")))) \
        .withColumn("device_type", coalesce(col("device_type"), lit("UNKNOWN"))) \
        .withColumn("platform", coalesce(col("platform"), lit("UNKNOWN"))) \
        .withColumn("country", upper(trim(coalesce(col("country"), lit("UNKNOWN"))))) \
        .withColumn(
            "is_valid_event",
            when(
                (col("event_id").isNotNull()) &
                (col("event_timestamp").isNotNull()) &
                (col("event_type").isin("PAGE_VIEW", "CLICK", "PURCHASE", "SIGNUP")),
                True
            ).otherwise(False)
        ) \
        .filter(col("is_valid_event") == True) \
        .drop("is_valid_event")

    # Add derived columns
    events_silver = events_silver \
        .withColumn(
            "event_category",
            when(col("event_type").isin("PURCHASE", "ADD_TO_CART"), "CONVERSION")
            .when(col("event_type").isin("PAGE_VIEW", "CLICK"), "ENGAGEMENT")
            .when(col("event_type").isin("SIGNUP", "LOGIN"), "AUTHENTICATION")
            .otherwise("OTHER")
        )

    # Create or update Silver table
    target_table = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.events"

    if not spark.catalog.tableExists(target_table):
        create_silver_table("events", events_silver.limit(0))

    # Append to Silver (events are immutable, no SCD needed)
    events_silver \
        .withColumn("processed_timestamp", current_timestamp()) \
        .write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "false") \
        .saveAsTable(target_table)

    row_count = events_silver.count()
    print(f"Transformed {row_count:,} events")

    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Transactions

# COMMAND ----------

def transform_transactions():
    """Transform transactions from Bronze to Silver with validation"""
    print("Transforming transactions...")

    # Read from Bronze
    transactions_bronze = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.transactions") \
        .filter(col("ingestion_date") == process_date)

    # Data cleansing and transformation
    transactions_silver = transactions_bronze \
        .filter(col("transaction_id").isNotNull()) \
        .filter(col("transaction_timestamp").isNotNull()) \
        .filter(col("user_id").isNotNull()) \
        .dropDuplicates(["transaction_id"]) \
        .withColumn("transaction_date", to_date(col("transaction_timestamp"))) \
        .withColumn("transaction_hour", hour(col("transaction_timestamp"))) \
        .withColumn("user_id", trim(col("user_id"))) \
        .withColumn("currency", upper(trim(coalesce(col("currency"), lit("USD"))))) \
        .withColumn("status", upper(trim(col("status")))) \
        .withColumn(
            "amount_usd",
            when(col("currency") == "USD", col("amount"))
            .when(col("currency") == "EUR", col("amount") * 1.1)
            .when(col("currency") == "GBP", col("amount") * 1.25)
            .otherwise(col("amount"))
        ) \
        .withColumn(
            "is_valid_transaction",
            when(
                (col("amount") > 0) &
                (col("status").isin("COMPLETED", "PENDING", "FAILED", "REFUNDED")),
                True
            ).otherwise(False)
        ) \
        .filter(col("is_valid_transaction") == True) \
        .drop("is_valid_transaction")

    # Add derived columns
    transactions_silver = transactions_silver \
        .withColumn(
            "transaction_category",
            when(col("amount_usd") < 10, "SMALL")
            .when(col("amount_usd") < 100, "MEDIUM")
            .when(col("amount_usd") < 1000, "LARGE")
            .otherwise("ENTERPRISE")
        ) \
        .withColumn(
            "is_successful",
            when(col("status") == "COMPLETED", True).otherwise(False)
        )

    # Create or update Silver table
    target_table = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.transactions"

    if not spark.catalog.tableExists(target_table):
        create_silver_table("transactions", transactions_silver.limit(0))

    # Append to Silver
    transactions_silver \
        .withColumn("processed_timestamp", current_timestamp()) \
        .write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "false") \
        .saveAsTable(target_table)

    row_count = transactions_silver.count()
    print(f"Transformed {row_count:,} transactions")

    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Users (SCD Type 2)

# COMMAND ----------

def transform_users():
    """Transform users from Bronze to Silver with SCD Type 2"""
    print("Transforming users...")

    # Read from Bronze (latest snapshot)
    users_bronze = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.users") \
        .filter(col("ingestion_date") == process_date)

    # Data cleansing and transformation
    users_silver = users_bronze \
        .filter(col("user_id").isNotNull()) \
        .dropDuplicates(["user_id"]) \
        .withColumn("email", lower(trim(col("email")))) \
        .withColumn("first_name", initcap(trim(col("first_name")))) \
        .withColumn("last_name", initcap(trim(col("last_name")))) \
        .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) \
        .withColumn("status", upper(trim(coalesce(col("status"), lit("ACTIVE"))))) \
        .withColumn("subscription_tier", upper(trim(coalesce(col("subscription_tier"), lit("FREE"))))) \
        .withColumn("country", upper(trim(coalesce(col("country"), lit("UNKNOWN"))))) \
        .withColumn(
            "is_premium",
            when(col("subscription_tier").isin("PREMIUM", "ENTERPRISE"), True).otherwise(False)
        ) \
        .withColumn(
            "account_age_days",
            datediff(current_date(), to_date(col("created_at")))
        )

    # Create or update Silver table with SCD Type 2
    target_table = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.users"

    if not spark.catalog.tableExists(target_table):
        create_silver_table("users", users_silver.limit(0))

    # Perform SCD Type 2 upsert
    upsert_scd_type2(users_silver, target_table, ["user_id"])

    row_count = users_silver.count()
    print(f"Transformed {row_count:,} users")

    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Transformations

# COMMAND ----------

# Run all transformations
events_count = transform_events()
transactions_count = transform_transactions()
users_count = transform_users()

print("\n=== Transformation Summary ===")
print(f"Events transformed: {events_count:,}")
print(f"Transactions transformed: {transactions_count:,}")
print(f"Users transformed: {users_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Silver Tables

# COMMAND ----------

# Optimize tables
spark.sql(f"OPTIMIZE {SILVER_CATALOG}.{SILVER_SCHEMA}.events ZORDER BY (event_date, user_id)")
spark.sql(f"OPTIMIZE {SILVER_CATALOG}.{SILVER_SCHEMA}.transactions ZORDER BY (transaction_date, user_id)")
spark.sql(f"OPTIMIZE {SILVER_CATALOG}.{SILVER_SCHEMA}.users ZORDER BY (user_id)")

# Vacuum old files
spark.sql(f"VACUUM {SILVER_CATALOG}.{SILVER_SCHEMA}.events RETAIN 168 HOURS")
spark.sql(f"VACUUM {SILVER_CATALOG}.{SILVER_SCHEMA}.transactions RETAIN 168 HOURS")
spark.sql(f"VACUUM {SILVER_CATALOG}.{SILVER_SCHEMA}.users RETAIN 168 HOURS")

print("Silver tables optimized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation

# COMMAND ----------

def validate_silver_data():
    """Validate data quality in Silver layer"""
    print("\n=== Silver Layer Data Quality Report ===")

    # Events validation
    events = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.events")
    events_total = events.count()
    events_valid = events.filter(
        col("event_id").isNotNull() &
        col("event_timestamp").isNotNull() &
        col("user_id").isNotNull()
    ).count()
    print(f"\nEvents: {events_total:,} total, {events_valid:,} valid ({events_valid/events_total*100:.2f}%)")

    # Transactions validation
    transactions = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.transactions")
    trans_total = transactions.count()
    trans_valid = transactions.filter(
        col("transaction_id").isNotNull() &
        col("amount_usd") > 0 &
        col("status").isNotNull()
    ).count()
    print(f"Transactions: {trans_total:,} total, {trans_valid:,} valid ({trans_valid/trans_total*100:.2f}%)")

    # Users validation
    users = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.users")
    users_current = users.filter(col("is_current") == True).count()
    users_total = users.count()
    print(f"Users: {users_total:,} total records, {users_current:,} current ({users_current/users_total*100:.2f}%)")

validate_silver_data()
