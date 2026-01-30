"""
Migration from HDFS/Hive to Delta Lake

Handles migration from Hadoop ecosystems including:
- HDFS files (Parquet, ORC, Avro, CSV)
- Hive tables (managed and external)
- Large-scale historical data (10TB+)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from typing import Dict, List, Optional
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HDFSMigrator:
    """Migrate data from HDFS/Hive to Delta Lake"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def migrate_hive_table(
        self,
        source_database: str,
        source_table: str,
        target_table: str,
        partition_columns: Optional[List[str]] = None,
        optimize_write: bool = True
    ) -> Dict:
        """
        Migrate Hive table to Delta Lake

        Args:
            source_database: Source Hive database
            source_table: Source Hive table name
            target_table: Target Delta table name
            partition_columns: Columns to partition by in Delta
            optimize_write: Enable optimized writes

        Returns:
            Migration metrics
        """
        logger.info(f"Migrating Hive table: {source_database}.{source_table} -> {target_table}")
        start_time = datetime.now()

        # Read from Hive
        source_full_name = f"{source_database}.{source_table}"
        df = self.spark.table(source_full_name)

        # Add migration metadata
        df = df.withColumn("migration_timestamp", current_timestamp()) \
               .withColumn("source_system", lit("hive")) \
               .withColumn("source_table", lit(source_full_name))

        # Write to Delta Lake
        writer = df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true")

        if optimize_write:
            writer = writer.option("optimizeWrite", "true") \
                          .option("autoCompact", "true")

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        writer.saveAsTable(target_table)

        # Optimize after migration
        self.spark.sql(f"OPTIMIZE {target_table}")

        # Calculate metrics
        row_count = df.count()
        duration = (datetime.now() - start_time).total_seconds()

        metrics = {
            "source_table": source_full_name,
            "target_table": target_table,
            "rows_migrated": row_count,
            "duration_seconds": duration,
            "rows_per_second": row_count / duration if duration > 0 else 0,
            "migration_timestamp": datetime.now().isoformat()
        }

        logger.info(f"Hive migration completed: {metrics}")
        return metrics

    def migrate_hdfs_files(
        self,
        source_path: str,
        target_table: str,
        file_format: str = "parquet",
        schema: Optional[StructType] = None,
        partition_columns: Optional[List[str]] = None,
        merge_schema: bool = False
    ) -> Dict:
        """
        Migrate HDFS files to Delta Lake

        Args:
            source_path: HDFS path to files
            target_table: Target Delta table name
            file_format: Source file format (parquet, orc, avro, csv)
            schema: Optional schema (required for CSV)
            partition_columns: Columns to partition by
            merge_schema: Whether to merge schemas if different

        Returns:
            Migration metrics
        """
        logger.info(f"Migrating HDFS files: {source_path} -> {target_table}")
        start_time = datetime.now()

        # Read from HDFS
        reader = self.spark.read.format(file_format)

        if schema:
            reader = reader.schema(schema)

        if merge_schema:
            reader = reader.option("mergeSchema", "true")

        # File format specific options
        if file_format == "csv":
            reader = reader.option("header", "true") \
                          .option("inferSchema", "true")

        df = reader.load(source_path)

        # Add migration metadata
        df = df.withColumn("migration_timestamp", current_timestamp()) \
               .withColumn("source_system", lit("hdfs")) \
               .withColumn("source_path", lit(source_path))

        # Write to Delta Lake
        writer = df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("optimizeWrite", "true")

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        writer.saveAsTable(target_table)

        # Calculate metrics
        row_count = df.count()
        duration = (datetime.now() - start_time).total_seconds()

        # Get file metrics
        file_count = len(df.inputFiles())

        metrics = {
            "source_path": source_path,
            "target_table": target_table,
            "file_format": file_format,
            "source_files": file_count,
            "rows_migrated": row_count,
            "duration_seconds": duration,
            "rows_per_second": row_count / duration if duration > 0 else 0,
            "migration_timestamp": datetime.now().isoformat()
        }

        logger.info(f"HDFS migration completed: {metrics}")
        return metrics

    def incremental_hdfs_migration(
        self,
        source_path: str,
        target_table: str,
        file_format: str = "parquet",
        checkpoint_path: str = None,
        partition_date: Optional[str] = None
    ) -> Dict:
        """
        Perform incremental migration using specific date partitions

        Args:
            source_path: HDFS path with date partitions
            target_table: Target Delta table
            file_format: Source file format
            checkpoint_path: Path to store checkpoint info
            partition_date: Specific date partition to migrate (YYYY-MM-DD)

        Returns:
            Migration metrics
        """
        logger.info(f"Starting incremental HDFS migration for date: {partition_date}")
        start_time = datetime.now()

        # Build partition-specific path
        if partition_date:
            full_path = f"{source_path}/date={partition_date}"
        else:
            full_path = source_path

        # Read partition data
        df = self.spark.read.format(file_format).load(full_path)

        row_count = df.count()

        if row_count == 0:
            logger.info("No data to migrate for this partition")
            return {"rows_migrated": 0, "partition_date": partition_date}

        # Add migration metadata
        df = df.withColumn("migration_timestamp", current_timestamp()) \
               .withColumn("source_system", lit("hdfs")) \
               .withColumn("partition_date", lit(partition_date))

        # Merge or append to Delta table
        if partition_date:
            # Overwrite specific partition
            df.write.format("delta") \
                .mode("overwrite") \
                .option("replaceWhere", f"partition_date = '{partition_date}'") \
                .saveAsTable(target_table)
        else:
            # Append all data
            df.write.format("delta") \
                .mode("append") \
                .saveAsTable(target_table)

        duration = (datetime.now() - start_time).total_seconds()

        metrics = {
            "source_path": full_path,
            "target_table": target_table,
            "partition_date": partition_date,
            "rows_migrated": row_count,
            "duration_seconds": duration,
            "rows_per_second": row_count / duration if duration > 0 else 0,
            "migration_timestamp": datetime.now().isoformat()
        }

        logger.info(f"Incremental migration completed: {metrics}")
        return metrics

    def migrate_with_transformation(
        self,
        source_path: str,
        target_table: str,
        file_format: str,
        transformation_func: callable,
        partition_columns: Optional[List[str]] = None
    ) -> Dict:
        """
        Migrate with custom transformation logic

        Args:
            source_path: Source HDFS path
            target_table: Target Delta table
            file_format: Source file format
            transformation_func: Function to transform DataFrame
            partition_columns: Partition columns

        Returns:
            Migration metrics
        """
        logger.info(f"Migrating with transformation: {source_path} -> {target_table}")
        start_time = datetime.now()

        # Read source data
        df = self.spark.read.format(file_format).load(source_path)

        # Apply transformation
        transformed_df = transformation_func(df)

        # Add migration metadata
        transformed_df = transformed_df \
            .withColumn("migration_timestamp", current_timestamp()) \
            .withColumn("source_system", lit("hdfs"))

        # Write to Delta
        writer = transformed_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("optimizeWrite", "true")

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        writer.saveAsTable(target_table)

        row_count = transformed_df.count()
        duration = (datetime.now() - start_time).total_seconds()

        metrics = {
            "source_path": source_path,
            "target_table": target_table,
            "rows_migrated": row_count,
            "duration_seconds": duration,
            "migration_timestamp": datetime.now().isoformat()
        }

        logger.info(f"Transformation migration completed: {metrics}")
        return metrics

    def validate_hdfs_migration(
        self,
        source_path: str,
        target_table: str,
        file_format: str = "parquet"
    ) -> Dict:
        """
        Validate HDFS to Delta migration

        Args:
            source_path: Source HDFS path
            target_table: Target Delta table
            file_format: Source file format

        Returns:
            Validation results
        """
        logger.info(f"Validating migration: {source_path} -> {target_table}")

        # Count source rows
        source_df = self.spark.read.format(file_format).load(source_path)
        source_count = source_df.count()
        source_files = len(source_df.inputFiles())

        # Count target rows
        target_df = self.spark.table(target_table)
        target_count = target_df.count()

        # Compare schemas
        source_cols = set(source_df.columns)
        target_cols = set([c for c in target_df.columns if c not in ["migration_timestamp", "source_system", "source_path"]])

        validation = {
            "source_row_count": source_count,
            "source_files": source_files,
            "target_row_count": target_count,
            "row_count_match": source_count == target_count,
            "row_count_difference": abs(source_count - target_count),
            "schema_match": source_cols == target_cols,
            "missing_columns": list(source_cols - target_cols),
            "extra_columns": list(target_cols - source_cols),
            "validation_timestamp": datetime.now().isoformat()
        }

        if validation["row_count_match"] and validation["schema_match"]:
            logger.info("✓ HDFS migration validation PASSED")
        else:
            logger.warning(f"✗ HDFS migration validation FAILED: {validation}")

        return validation


# Example transformation function
def example_transformation(df: DataFrame) -> DataFrame:
    """Example transformation function"""
    return df \
        .withColumn("processed_date", current_date()) \
        .withColumn("is_valid", col("amount") > 0) \
        .filter(col("is_valid") == True) \
        .drop("is_valid")


# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("HDFS Migration") \
        .enableHiveSupport() \
        .getOrCreate()

    migrator = HDFSMigrator(spark)

    # Migrate Hive table
    hive_metrics = migrator.migrate_hive_table(
        source_database="legacy_db",
        source_table="transactions",
        target_table="delta_migration.bronze.transactions",
        partition_columns=["transaction_date"]
    )

    # Migrate HDFS files
    hdfs_metrics = migrator.migrate_hdfs_files(
        source_path="/data/raw/events",
        target_table="delta_migration.bronze.events",
        file_format="parquet",
        partition_columns=["event_date"]
    )

    print(f"Hive migration: {hive_metrics['rows_migrated']:,} rows")
    print(f"HDFS migration: {hdfs_metrics['rows_migrated']:,} rows")
