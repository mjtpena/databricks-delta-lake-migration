"""
Migration from AWS S3 to Delta Lake

Handles migration from S3 data lakes including:
- Large-scale data ingestion (10TB+ daily)
- Multiple file formats
- Incremental processing
- Auto Loader for continuous ingestion
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, List, Optional
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Migrator:
    """Migrate data from S3 to Delta Lake"""

    def __init__(self, spark: SparkSession, s3_config: Optional[Dict] = None):
        """
        Initialize S3 migrator

        Args:
            spark: SparkSession
            s3_config: Optional S3 configuration (access keys, region, etc.)
        """
        self.spark = spark
        self.s3_config = s3_config or {}
        self._configure_s3_access()

    def _configure_s3_access(self):
        """Configure S3 access credentials"""
        if "access_key" in self.s3_config:
            self.spark.conf.set("spark.hadoop.fs.s3a.access.key", self.s3_config["access_key"])
            self.spark.conf.set("spark.hadoop.fs.s3a.secret.key", self.s3_config["secret_key"])

        if "aws_region" in self.s3_config:
            self.spark.conf.set("spark.hadoop.fs.s3a.endpoint.region", self.s3_config["aws_region"])

        # S3A optimizations
        self.spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
        self.spark.conf.set("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB

    def batch_migrate_from_s3(
        self,
        s3_path: str,
        target_table: str,
        file_format: str = "parquet",
        partition_columns: Optional[List[str]] = None,
        schema: Optional[StructType] = None,
        num_repartitions: int = 200
    ) -> Dict:
        """
        Batch migrate data from S3 to Delta Lake

        Args:
            s3_path: S3 path (s3://bucket/path)
            target_table: Target Delta table name
            file_format: Source file format
            partition_columns: Columns to partition by
            schema: Optional schema
            num_repartitions: Number of partitions for writing

        Returns:
            Migration metrics
        """
        logger.info(f"Starting batch migration from S3: {s3_path}")
        start_time = datetime.now()

        # Read from S3
        reader = self.spark.read.format(file_format)

        if schema:
            reader = reader.schema(schema)

        # File format specific options
        if file_format == "json":
            reader = reader.option("multiLine", "false")
        elif file_format == "csv":
            reader = reader.option("header", "true") \
                          .option("inferSchema", "true")

        df = reader.load(s3_path)

        # Add metadata
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("source_path", input_file_name()) \
               .withColumn("source_system", lit("s3"))

        # Optimize partitioning for large datasets
        if partition_columns:
            df = df.repartition(num_repartitions, *partition_columns)
        else:
            df = df.repartition(num_repartitions)

        # Write to Delta Lake
        writer = df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("optimizeWrite", "true") \
            .option("autoCompact", "true")

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        writer.saveAsTable(target_table)

        # Optimize after initial load
        self.spark.sql(f"OPTIMIZE {target_table}")

        # Calculate metrics
        row_count = df.count()
        duration = (datetime.now() - start_time).total_seconds()

        metrics = {
            "s3_path": s3_path,
            "target_table": target_table,
            "file_format": file_format,
            "rows_migrated": row_count,
            "duration_seconds": duration,
            "rows_per_second": row_count / duration if duration > 0 else 0,
            "throughput_gb_per_hour": self._estimate_throughput(row_count, duration),
            "migration_timestamp": datetime.now().isoformat()
        }

        logger.info(f"Batch migration completed: {metrics}")
        return metrics

    def stream_migrate_with_autoloader(
        self,
        s3_path: str,
        target_table: str,
        checkpoint_path: str,
        file_format: str = "json",
        schema: Optional[StructType] = None,
        trigger_interval: str = "30 seconds",
        max_files_per_trigger: int = 100
    ):
        """
        Set up streaming migration using Auto Loader

        Args:
            s3_path: S3 path to monitor
            target_table: Target Delta table
            checkpoint_path: Checkpoint location
            file_format: Source file format
            schema: Optional schema
            trigger_interval: Processing trigger interval
            max_files_per_trigger: Max files per trigger

        Returns:
            Streaming query
        """
        logger.info(f"Setting up Auto Loader stream from: {s3_path}")

        # Read stream with Auto Loader
        stream_reader = self.spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", file_format) \
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema") \
            .option("cloudFiles.maxFilesPerTrigger", max_files_per_trigger) \
            .option("cloudFiles.useNotifications", "true")  # Use S3 notifications

        if schema:
            stream_reader = stream_reader.schema(schema)
        else:
            stream_reader = stream_reader.option("cloudFiles.inferColumnTypes", "true")

        stream_df = stream_reader.load(s3_path)

        # Add metadata
        stream_df = stream_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_path", input_file_name()) \
            .withColumn("source_system", lit("s3"))

        # Write stream to Delta
        query = stream_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{checkpoint_path}/checkpoint") \
            .option("mergeSchema", "false") \
            .trigger(processingTime=trigger_interval) \
            .toTable(target_table)

        logger.info(f"Auto Loader stream started with query ID: {query.id}")
        return query

    def incremental_s3_migration(
        self,
        s3_path: str,
        target_table: str,
        file_format: str,
        partition_date: str,
        partition_columns: Optional[List[str]] = None
    ) -> Dict:
        """
        Incremental migration of specific S3 partition

        Args:
            s3_path: Base S3 path
            target_table: Target Delta table
            file_format: File format
            partition_date: Date partition to migrate (YYYY-MM-DD)
            partition_columns: Partition columns

        Returns:
            Migration metrics
        """
        logger.info(f"Incremental S3 migration for partition: {partition_date}")
        start_time = datetime.now()

        # Build partition path
        partition_path = f"{s3_path}/date={partition_date}"

        # Read partition data
        df = self.spark.read.format(file_format).load(partition_path)

        row_count = df.count()

        if row_count == 0:
            logger.info(f"No data for partition {partition_date}")
            return {"rows_migrated": 0, "partition_date": partition_date}

        # Add metadata
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("partition_date", lit(partition_date)) \
               .withColumn("source_system", lit("s3"))

        # Write with partition overwrite
        writer = df.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"partition_date = '{partition_date}'") \
            .option("optimizeWrite", "true")

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        writer.saveAsTable(target_table)

        duration = (datetime.now() - start_time).total_seconds()

        metrics = {
            "s3_path": partition_path,
            "target_table": target_table,
            "partition_date": partition_date,
            "rows_migrated": row_count,
            "duration_seconds": duration,
            "rows_per_second": row_count / duration if duration > 0 else 0,
            "migration_timestamp": datetime.now().isoformat()
        }

        logger.info(f"Incremental migration completed: {metrics}")
        return metrics

    def migrate_multiple_s3_paths(
        self,
        s3_paths: List[str],
        target_table: str,
        file_format: str,
        partition_columns: Optional[List[str]] = None
    ) -> Dict:
        """
        Migrate multiple S3 paths into single Delta table

        Args:
            s3_paths: List of S3 paths
            target_table: Target Delta table
            file_format: File format
            partition_columns: Partition columns

        Returns:
            Combined migration metrics
        """
        logger.info(f"Migrating {len(s3_paths)} S3 paths to {target_table}")
        start_time = datetime.now()

        # Read all paths
        dfs = []
        for path in s3_paths:
            df = self.spark.read.format(file_format).load(path)
            df = df.withColumn("source_path", lit(path))
            dfs.append(df)

        # Union all dataframes
        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.union(df)

        # Add metadata
        combined_df = combined_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_system", lit("s3"))

        # Write to Delta
        writer = combined_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("optimizeWrite", "true")

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        writer.saveAsTable(target_table)

        row_count = combined_df.count()
        duration = (datetime.now() - start_time).total_seconds()

        metrics = {
            "s3_paths_count": len(s3_paths),
            "target_table": target_table,
            "rows_migrated": row_count,
            "duration_seconds": duration,
            "rows_per_second": row_count / duration if duration > 0 else 0,
            "migration_timestamp": datetime.now().isoformat()
        }

        logger.info(f"Multi-path migration completed: {metrics}")
        return metrics

    def _estimate_throughput(self, rows: int, seconds: float) -> float:
        """Estimate throughput in GB/hour"""
        avg_row_size_kb = 1  # Assume 1KB per row
        total_gb = (rows * avg_row_size_kb) / (1024 * 1024)
        hours = seconds / 3600
        return total_gb / hours if hours > 0 else 0


# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("S3 Migration") \
        .getOrCreate()

    # Initialize migrator
    s3_config = {
        "access_key": "your-access-key",
        "secret_key": "your-secret-key",
        "aws_region": "us-west-2"
    }

    migrator = S3Migrator(spark, s3_config)

    # Batch migration
    batch_metrics = migrator.batch_migrate_from_s3(
        s3_path="s3://my-data-lake/raw/events/",
        target_table="delta_migration.bronze.events",
        file_format="parquet",
        partition_columns=["event_date"],
        num_repartitions=500
    )

    print(f"Migrated {batch_metrics['rows_migrated']:,} rows in {batch_metrics['duration_seconds']:.2f}s")
    print(f"Throughput: {batch_metrics['throughput_gb_per_hour']:.2f} GB/hour")
