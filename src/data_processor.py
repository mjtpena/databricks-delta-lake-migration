"""
High-performance data processor optimized for 10TB+ daily volumes

This module provides optimized data processing capabilities for large-scale
Delta Lake operations with focus on:
- Memory efficiency
- Partitioning strategies
- Adaptive query execution
- Resource optimization
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LargeScaleDataProcessor:
    """
    Processor optimized for handling 10TB+ daily data volumes
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict] = None):
        """
        Initialize processor with Spark session and configuration

        Args:
            spark: SparkSession instance
            config: Optional configuration dictionary
        """
        self.spark = spark
        self.config = config or self._default_config()
        self._optimize_spark_settings()

    def _default_config(self) -> Dict:
        """Default configuration for large-scale processing"""
        return {
            "batch_size": 5000000,  # 5M rows per batch
            "max_partition_bytes": 134217728,  # 128MB
            "shuffle_partitions": 2000,
            "compression": "snappy",
            "optimize_write": True,
            "auto_compact": True,
            "enable_adaptive_execution": True,
            "broadcast_threshold": 104857600,  # 100MB
        }

    def _optimize_spark_settings(self):
        """Configure Spark for optimal large-scale processing"""
        logger.info("Optimizing Spark settings for large-scale processing...")

        # Adaptive Query Execution
        if self.config["enable_adaptive_execution"]:
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

        # Partition settings
        self.spark.conf.set("spark.sql.files.maxPartitionBytes",
                           self.config["max_partition_bytes"])
        self.spark.conf.set("spark.sql.shuffle.partitions",
                           self.config["shuffle_partitions"])

        # Delta optimizations
        self.spark.conf.set("spark.databricks.delta.optimizeWrite.enabled",
                           str(self.config["optimize_write"]).lower())
        self.spark.conf.set("spark.databricks.delta.autoCompact.enabled",
                           str(self.config["auto_compact"]).lower())

        # Memory and caching
        self.spark.conf.set("spark.databricks.io.cache.enabled", "true")
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold",
                           self.config["broadcast_threshold"])

        # Compression
        self.spark.conf.set("spark.sql.parquet.compression.codec",
                           self.config["compression"])

        logger.info("Spark optimization completed")

    def process_large_dataset(
        self,
        source_path: str,
        target_table: str,
        schema: StructType,
        partition_columns: Optional[List[str]] = None,
        processing_func: Optional[callable] = None
    ) -> Tuple[int, Dict]:
        """
        Process large dataset with optimized memory usage

        Args:
            source_path: Path to source data
            target_table: Target Delta table name
            schema: Data schema
            partition_columns: Columns to partition by
            processing_func: Optional transformation function

        Returns:
            Tuple of (row_count, metrics)
        """
        logger.info(f"Processing large dataset from {source_path}")
        start_time = datetime.now()

        # Read with partitioning
        df = self.spark.read \
            .format("parquet") \
            .schema(schema) \
            .option("mergeSchema", "false") \
            .option("pathGlobFilter", "*.parquet") \
            .load(source_path)

        # Apply transformation if provided
        if processing_func:
            df = processing_func(df)

        # Optimize partitioning
        if partition_columns:
            # Calculate optimal partition count
            estimated_size = self._estimate_data_size(df)
            optimal_partitions = self._calculate_optimal_partitions(estimated_size)

            logger.info(f"Repartitioning to {optimal_partitions} partitions")
            df = df.repartition(optimal_partitions, *partition_columns)

        # Write with optimizations
        writer = df.write.format("delta") \
            .mode("append") \
            .option("optimizeWrite", "true") \
            .option("autoCompact", "true")

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        writer.saveAsTable(target_table)

        # Calculate metrics
        row_count = df.count()
        processing_time = (datetime.now() - start_time).total_seconds()

        metrics = {
            "rows_processed": row_count,
            "processing_time_seconds": processing_time,
            "rows_per_second": row_count / processing_time if processing_time > 0 else 0,
            "throughput_mb_per_second": self._calculate_throughput(row_count, processing_time)
        }

        logger.info(f"Processed {row_count:,} rows in {processing_time:.2f} seconds")
        logger.info(f"Throughput: {metrics['rows_per_second']:,.0f} rows/sec")

        return row_count, metrics

    def streaming_ingest(
        self,
        source_path: str,
        target_table: str,
        checkpoint_path: str,
        schema: StructType,
        trigger_interval: str = "30 seconds",
        max_files_per_trigger: int = 100
    ):
        """
        Set up optimized streaming ingestion for continuous data loads

        Args:
            source_path: Source data path
            target_table: Target Delta table
            checkpoint_path: Checkpoint location
            schema: Data schema
            trigger_interval: Trigger processing interval
            max_files_per_trigger: Max files to process per trigger
        """
        logger.info(f"Setting up streaming ingestion to {target_table}")

        # Read streaming data with auto-loader
        stream_df = self.spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", "json") \
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema") \
            .option("cloudFiles.maxFilesPerTrigger", max_files_per_trigger) \
            .schema(schema) \
            .load(source_path)

        # Write stream with optimizations
        query = stream_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{checkpoint_path}/checkpoint") \
            .option("mergeSchema", "false") \
            .trigger(processingTime=trigger_interval) \
            .toTable(target_table)

        logger.info(f"Streaming query started with ID: {query.id}")
        return query

    def optimize_table(
        self,
        table_name: str,
        zorder_columns: Optional[List[str]] = None,
        vacuum_hours: int = 168
    ):
        """
        Optimize Delta table for query performance

        Args:
            table_name: Name of Delta table
            zorder_columns: Columns to Z-Order by
            vacuum_hours: Retention period in hours
        """
        logger.info(f"Optimizing table {table_name}")

        # Run OPTIMIZE
        if zorder_columns:
            zorder_cols = ", ".join(zorder_columns)
            self.spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})")
        else:
            self.spark.sql(f"OPTIMIZE {table_name}")

        # Run VACUUM
        self.spark.sql(f"VACUUM {table_name} RETAIN {vacuum_hours} HOURS")

        # Analyze table statistics
        self.spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")

        logger.info(f"Optimization completed for {table_name}")

    def batch_upsert(
        self,
        source_df: DataFrame,
        target_table: str,
        key_columns: List[str],
        batch_size: Optional[int] = None
    ) -> Dict:
        """
        Perform batch upsert with memory optimization

        Args:
            source_df: Source DataFrame
            target_table: Target Delta table
            key_columns: Columns to use as merge key
            batch_size: Optional batch size override

        Returns:
            Metrics dictionary
        """
        logger.info(f"Starting batch upsert to {target_table}")

        batch_size = batch_size or self.config["batch_size"]
        target_delta = DeltaTable.forName(self.spark, target_table)

        # Build merge condition
        merge_condition = " AND ".join(
            [f"target.{col} = source.{col}" for col in key_columns]
        )

        # Perform merge
        merge_builder = target_delta.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        )

        # Execute merge
        merge_builder.whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        metrics = {
            "merge_completed": True,
            "key_columns": key_columns,
            "source_rows": source_df.count()
        }

        logger.info(f"Batch upsert completed: {metrics}")
        return metrics

    def _estimate_data_size(self, df: DataFrame) -> int:
        """Estimate DataFrame size in bytes"""
        # Sample and estimate
        sample_size = min(10000, df.count())
        sample_df = df.limit(sample_size)

        # Rough estimation based on sample
        avg_row_size = 1024  # Assume 1KB per row as baseline
        total_rows = df.count()

        return total_rows * avg_row_size

    def _calculate_optimal_partitions(self, estimated_size: int) -> int:
        """Calculate optimal number of partitions based on data size"""
        target_partition_size = self.config["max_partition_bytes"]
        optimal_partitions = max(200, estimated_size // target_partition_size)

        # Cap at reasonable limits
        return min(optimal_partitions, 10000)

    def _calculate_throughput(self, rows: int, seconds: float) -> float:
        """Calculate throughput in MB/s (rough estimate)"""
        avg_row_size_bytes = 1024  # Assume 1KB per row
        total_mb = (rows * avg_row_size_bytes) / (1024 * 1024)
        return total_mb / seconds if seconds > 0 else 0

    def get_table_metrics(self, table_name: str) -> Dict:
        """
        Get comprehensive metrics for a Delta table

        Args:
            table_name: Name of Delta table

        Returns:
            Dictionary of metrics
        """
        # Get table details
        details = self.spark.sql(f"DESCRIBE DETAIL {table_name}").first()

        # Get history
        history = self.spark.sql(f"DESCRIBE HISTORY {table_name}").first()

        metrics = {
            "table_name": table_name,
            "format": details["format"],
            "location": details["location"],
            "num_files": details["numFiles"],
            "size_in_bytes": details["sizeInBytes"],
            "size_in_gb": details["sizeInBytes"] / (1024**3),
            "partition_columns": details["partitionColumns"],
            "created_at": details["createdAt"],
            "last_modified": history["timestamp"] if history else None,
            "last_operation": history["operation"] if history else None
        }

        return metrics


class DataQualityValidator:
    """Validate data quality for large datasets"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def validate_completeness(
        self,
        df: DataFrame,
        required_columns: List[str]
    ) -> Dict:
        """Check for null values in required columns"""
        results = {}

        total_rows = df.count()

        for col_name in required_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0

            results[col_name] = {
                "null_count": null_count,
                "null_percentage": null_percentage,
                "is_valid": null_count == 0
            }

        return results

    def validate_uniqueness(
        self,
        df: DataFrame,
        key_columns: List[str]
    ) -> Dict:
        """Check for duplicate records"""
        total_rows = df.count()
        unique_rows = df.select(*key_columns).distinct().count()
        duplicate_count = total_rows - unique_rows

        return {
            "total_rows": total_rows,
            "unique_rows": unique_rows,
            "duplicate_count": duplicate_count,
            "is_valid": duplicate_count == 0
        }

    def validate_ranges(
        self,
        df: DataFrame,
        column_ranges: Dict[str, Tuple[float, float]]
    ) -> Dict:
        """Validate numeric columns are within expected ranges"""
        results = {}

        for col_name, (min_val, max_val) in column_ranges.items():
            out_of_range = df.filter(
                (col(col_name) < min_val) | (col(col_name) > max_val)
            ).count()

            results[col_name] = {
                "min_allowed": min_val,
                "max_allowed": max_val,
                "out_of_range_count": out_of_range,
                "is_valid": out_of_range == 0
            }

        return results
