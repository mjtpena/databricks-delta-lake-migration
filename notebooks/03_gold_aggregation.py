# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business Aggregates
# MAGIC
# MAGIC This notebook creates business-level aggregates and analytical views in the Gold layer.
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Pre-computed aggregates for analytics
# MAGIC - Dimensional modeling
# MAGIC - Business KPIs
# MAGIC - Optimized for query performance

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("process_date", "", "Process Date")

environment = dbutils.widgets.get("environment")
process_date = dbutils.widgets.get("process_date")

if not process_date:
    process_date = datetime.now().strftime("%Y-%m-%d")

SILVER_CATALOG = "delta_migration"
SILVER_SCHEMA = "silver"
GOLD_CATALOG = "delta_migration"
GOLD_SCHEMA = "gold"

print(f"Environment: {environment}")
print(f"Process Date: {process_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily User Activity Aggregates

# COMMAND ----------

def create_daily_user_activity():
    """Create daily user activity aggregates"""
    print("Creating daily user activity aggregates...")

    events = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.events")

    daily_activity = events \
        .filter(col("event_date") == process_date) \
        .groupBy("event_date", "user_id") \
        .agg(
            count("*").alias("total_events"),
            countDistinct("session_id").alias("total_sessions"),
            countDistinct("event_type").alias("unique_event_types"),
            sum(when(col("event_category") == "CONVERSION", 1).otherwise(0)).alias("conversion_events"),
            sum(when(col("event_category") == "ENGAGEMENT", 1).otherwise(0)).alias("engagement_events"),
            min("event_timestamp").alias("first_event_timestamp"),
            max("event_timestamp").alias("last_event_timestamp"),
            collect_set("device_type").alias("devices_used"),
            collect_set("platform").alias("platforms_used")
        ) \
        .withColumn(
            "session_duration_minutes",
            (unix_timestamp("last_event_timestamp") - unix_timestamp("first_event_timestamp")) / 60
        ) \
        .withColumn("is_power_user", when(col("total_events") > 50, True).otherwise(False)) \
        .withColumn("processed_timestamp", current_timestamp())

    # Write to Gold
    target_table = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.daily_user_activity"

    daily_activity.write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"event_date = '{process_date}'") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)

    row_count = daily_activity.count()
    print(f"Created {row_count:,} daily user activity records")

    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction Analytics

# COMMAND ----------

def create_transaction_analytics():
    """Create transaction analytics aggregates"""
    print("Creating transaction analytics...")

    transactions = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.transactions")
    users = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.users") \
        .filter(col("is_current") == True)

    # Daily transaction metrics by user
    daily_transactions = transactions \
        .filter(col("transaction_date") == process_date) \
        .join(users, "user_id", "left") \
        .groupBy("transaction_date", "user_id", "subscription_tier", "country") \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount_usd").alias("total_amount_usd"),
            avg("amount_usd").alias("avg_amount_usd"),
            max("amount_usd").alias("max_amount_usd"),
            min("amount_usd").alias("min_amount_usd"),
            sum(when(col("is_successful"), 1).otherwise(0)).alias("successful_transactions"),
            sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failed_transactions"),
            sum(when(col("status") == "REFUNDED", 1).otherwise(0)).alias("refunded_transactions"),
            countDistinct("merchant_id").alias("unique_merchants"),
            collect_set("payment_method").alias("payment_methods_used")
        ) \
        .withColumn(
            "success_rate",
            (col("successful_transactions") / col("transaction_count")) * 100
        ) \
        .withColumn(
            "user_segment",
            when(col("total_amount_usd") > 1000, "HIGH_VALUE")
            .when(col("total_amount_usd") > 100, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        ) \
        .withColumn("processed_timestamp", current_timestamp())

    # Write to Gold
    target_table = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.daily_transaction_analytics"

    daily_transactions.write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"transaction_date = '{process_date}'") \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)

    row_count = daily_transactions.count()
    print(f"Created {row_count:,} transaction analytics records")

    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Lifetime Value (LTV)

# COMMAND ----------

def create_user_ltv():
    """Calculate User Lifetime Value"""
    print("Calculating User LTV...")

    transactions = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.transactions") \
        .filter(col("is_successful") == True)

    users = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.users") \
        .filter(col("is_current") == True)

    events = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.events")

    # Calculate transaction metrics
    transaction_metrics = transactions.groupBy("user_id").agg(
        sum("amount_usd").alias("total_revenue"),
        count("*").alias("total_transactions"),
        avg("amount_usd").alias("avg_transaction_value"),
        min("transaction_timestamp").alias("first_transaction_date"),
        max("transaction_timestamp").alias("last_transaction_date"),
        countDistinct("transaction_date").alias("active_transaction_days")
    )

    # Calculate engagement metrics
    engagement_metrics = events.groupBy("user_id").agg(
        count("*").alias("total_events"),
        countDistinct("event_date").alias("active_days"),
        countDistinct("session_id").alias("total_sessions")
    )

    # Combine all metrics
    user_ltv = users \
        .join(transaction_metrics, "user_id", "left") \
        .join(engagement_metrics, "user_id", "left") \
        .withColumn("total_revenue", coalesce(col("total_revenue"), lit(0))) \
        .withColumn("total_transactions", coalesce(col("total_transactions"), lit(0))) \
        .withColumn("total_events", coalesce(col("total_events"), lit(0))) \
        .withColumn("active_days", coalesce(col("active_days"), lit(0))) \
        .withColumn(
            "customer_lifetime_days",
            datediff(current_date(), col("created_at"))
        ) \
        .withColumn(
            "ltv_score",
            col("total_revenue") *
            (col("active_days") / greatest(col("customer_lifetime_days"), lit(1)))
        ) \
        .withColumn(
            "engagement_score",
            (col("total_events") / greatest(col("active_days"), lit(1)))
        ) \
        .withColumn(
            "ltv_segment",
            when(col("ltv_score") > 1000, "PLATINUM")
            .when(col("ltv_score") > 500, "GOLD")
            .when(col("ltv_score") > 100, "SILVER")
            .otherwise("BRONZE")
        ) \
        .withColumn("calculation_date", current_date()) \
        .withColumn("processed_timestamp", current_timestamp())

    # Write to Gold
    target_table = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.user_ltv"

    user_ltv.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)

    row_count = user_ltv.count()
    print(f"Calculated LTV for {row_count:,} users")

    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cohort Analysis

# COMMAND ----------

def create_cohort_analysis():
    """Create cohort analysis for retention tracking"""
    print("Creating cohort analysis...")

    users = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.users") \
        .filter(col("is_current") == True) \
        .select("user_id", "created_at") \
        .withColumn("cohort_month", date_trunc("month", col("created_at")))

    events = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.events") \
        .select("user_id", "event_date") \
        .withColumn("activity_month", date_trunc("month", col("event_date")))

    # Join users with their activity
    cohort_activity = users.join(events, "user_id") \
        .select("cohort_month", "activity_month") \
        .distinct()

    # Calculate months since cohort
    cohort_analysis = cohort_activity \
        .withColumn(
            "months_since_cohort",
            months_between(col("activity_month"), col("cohort_month")).cast(IntegerType())
        ) \
        .groupBy("cohort_month", "months_since_cohort") \
        .agg(
            count("*").alias("active_users")
        )

    # Calculate cohort size
    cohort_sizes = users.groupBy("cohort_month").agg(
        count("*").alias("cohort_size")
    )

    # Calculate retention rate
    cohort_retention = cohort_analysis.join(cohort_sizes, "cohort_month") \
        .withColumn(
            "retention_rate",
            (col("active_users") / col("cohort_size")) * 100
        ) \
        .withColumn("processed_timestamp", current_timestamp())

    # Write to Gold
    target_table = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.cohort_retention"

    cohort_retention.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)

    row_count = cohort_retention.count()
    print(f"Created {row_count:,} cohort retention records")

    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Business KPIs

# COMMAND ----------

def create_daily_kpis():
    """Create daily business KPIs"""
    print("Creating daily KPIs...")

    transactions = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.transactions") \
        .filter(col("transaction_date") == process_date)

    events = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.events") \
        .filter(col("event_date") == process_date)

    users = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.users") \
        .filter(col("is_current") == True)

    # Revenue KPIs
    revenue_kpis = transactions.filter(col("is_successful") == True).agg(
        sum("amount_usd").alias("total_revenue"),
        count("*").alias("total_transactions"),
        avg("amount_usd").alias("avg_transaction_value"),
        countDistinct("user_id").alias("paying_users")
    )

    # User KPIs
    user_kpis = users.agg(
        count("*").alias("total_users"),
        sum(when(col("is_premium"), 1).otherwise(0)).alias("premium_users"),
        sum(when(col("status") == "ACTIVE", 1).otherwise(0)).alias("active_users")
    )

    # Engagement KPIs
    engagement_kpis = events.agg(
        count("*").alias("total_events"),
        countDistinct("user_id").alias("active_users_today"),
        countDistinct("session_id").alias("total_sessions"),
        sum(when(col("event_category") == "CONVERSION", 1).otherwise(0)).alias("conversion_events")
    )

    # Combine all KPIs
    daily_kpis = revenue_kpis.crossJoin(user_kpis).crossJoin(engagement_kpis) \
        .withColumn("kpi_date", lit(process_date).cast(DateType())) \
        .withColumn("conversion_rate", (col("conversion_events") / col("total_events")) * 100) \
        .withColumn("revenue_per_user", col("total_revenue") / col("paying_users")) \
        .withColumn("processed_timestamp", current_timestamp())

    # Write to Gold
    target_table = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.daily_kpis"

    daily_kpis.write.format("delta") \
        .mode("append") \
        .saveAsTable(target_table)

    print(f"Created daily KPIs for {process_date}")

    return 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Gold Layer Creation

# COMMAND ----------

# Run all gold layer aggregations
activity_count = create_daily_user_activity()
transaction_count = create_transaction_analytics()
ltv_count = create_user_ltv()
cohort_count = create_cohort_analysis()
kpi_count = create_daily_kpis()

print("\n=== Gold Layer Summary ===")
print(f"Daily user activity records: {activity_count:,}")
print(f"Transaction analytics records: {transaction_count:,}")
print(f"User LTV records: {ltv_count:,}")
print(f"Cohort retention records: {cohort_count:,}")
print(f"Daily KPIs created: {kpi_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Gold Tables

# COMMAND ----------

# Optimize all Gold tables
spark.sql(f"OPTIMIZE {GOLD_CATALOG}.{GOLD_SCHEMA}.daily_user_activity ZORDER BY (event_date, user_id)")
spark.sql(f"OPTIMIZE {GOLD_CATALOG}.{GOLD_SCHEMA}.daily_transaction_analytics ZORDER BY (transaction_date, user_id)")
spark.sql(f"OPTIMIZE {GOLD_CATALOG}.{GOLD_SCHEMA}.user_ltv ZORDER BY (ltv_score)")
spark.sql(f"OPTIMIZE {GOLD_CATALOG}.{GOLD_SCHEMA}.cohort_retention ZORDER BY (cohort_month)")
spark.sql(f"OPTIMIZE {GOLD_CATALOG}.{GOLD_SCHEMA}.daily_kpis ZORDER BY (kpi_date)")

print("Gold tables optimized successfully")
