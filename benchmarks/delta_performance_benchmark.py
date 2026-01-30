"""
Delta Lake Performance Benchmarking Suite

Comprehensive performance tests for:
- Read performance
- Write performance
- MERGE operations
- Time Travel queries
- Z-Ordering impact
- Partition pruning
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from typing import Dict, List
import time
import json
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd


class DeltaPerformanceBenchmark:
    """Performance benchmarking for Delta Lake operations"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results = []

    def benchmark_write_performance(
        self,
        num_rows: int = 10000000,
        num_partitions: int = 200,
        target_table: str = "benchmark.write_test"
    ) -> Dict:
        """
        Benchmark Delta Lake write performance

        Args:
            num_rows: Number of rows to write
            num_partitions: Number of partitions
            target_table: Target table name

        Returns:
            Performance metrics
        """
        print(f"\n=== Benchmarking Write Performance ({num_rows:,} rows) ===")

        # Generate test data
        print("Generating test data...")
        df = self.spark.range(num_rows) \
            .withColumn("timestamp", current_timestamp()) \
            .withColumn("date", current_date()) \
            .withColumn("user_id", (col("id") % 100000).cast("string")) \
            .withColumn("amount", (rand() * 1000).cast("decimal(18,2)")) \
            .withColumn("category", (col("id") % 10).cast("string")) \
            .withColumn("data", concat(lit("data_"), col("id").cast("string"))) \
            .repartition(num_partitions)

        # Benchmark write
        start_time = time.time()

        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("optimizeWrite", "true") \
            .partitionBy("date") \
            .saveAsTable(target_table)

        write_duration = time.time() - start_time

        # Calculate metrics
        metrics = {
            "operation": "write",
            "rows": num_rows,
            "partitions": num_partitions,
            "duration_seconds": write_duration,
            "rows_per_second": num_rows / write_duration,
            "mb_per_second": self._estimate_throughput(num_rows, write_duration)
        }

        print(f"Write completed in {write_duration:.2f}s")
        print(f"Throughput: {metrics['rows_per_second']:,.0f} rows/sec")
        print(f"Throughput: {metrics['mb_per_second']:.2f} MB/sec")

        self.results.append(metrics)
        return metrics

    def benchmark_read_performance(
        self,
        source_table: str,
        use_cache: bool = False
    ) -> Dict:
        """
        Benchmark Delta Lake read performance

        Args:
            source_table: Source table to read
            use_cache: Whether to use caching

        Returns:
            Performance metrics
        """
        print(f"\n=== Benchmarking Read Performance (Cache: {use_cache}) ===")

        # Full table scan
        start_time = time.time()

        df = self.spark.table(source_table)

        if use_cache:
            df.cache()

        row_count = df.count()
        read_duration = time.time() - start_time

        metrics = {
            "operation": "read",
            "rows": row_count,
            "cached": use_cache,
            "duration_seconds": read_duration,
            "rows_per_second": row_count / read_duration,
            "mb_per_second": self._estimate_throughput(row_count, read_duration)
        }

        print(f"Read {row_count:,} rows in {read_duration:.2f}s")
        print(f"Throughput: {metrics['rows_per_second']:,.0f} rows/sec")

        self.results.append(metrics)
        return metrics

    def benchmark_merge_performance(
        self,
        target_table: str,
        num_updates: int = 1000000
    ) -> Dict:
        """
        Benchmark MERGE operation performance

        Args:
            target_table: Target table for merge
            num_updates: Number of rows to update

        Returns:
            Performance metrics
        """
        print(f"\n=== Benchmarking MERGE Performance ({num_updates:,} updates) ===")

        # Create update data
        updates_df = self.spark.range(num_updates) \
            .withColumn("timestamp", current_timestamp()) \
            .withColumn("date", current_date()) \
            .withColumn("user_id", (col("id") % 100000).cast("string")) \
            .withColumn("amount", (rand() * 2000).cast("decimal(18,2)")) \
            .withColumn("category", (col("id") % 10).cast("string")) \
            .withColumn("data", concat(lit("updated_"), col("id").cast("string")))

        # Benchmark merge
        start_time = time.time()

        target_delta = DeltaTable.forName(self.spark, target_table)

        target_delta.alias("target").merge(
            updates_df.alias("updates"),
            "target.id = updates.id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        merge_duration = time.time() - start_time

        metrics = {
            "operation": "merge",
            "rows": num_updates,
            "duration_seconds": merge_duration,
            "rows_per_second": num_updates / merge_duration
        }

        print(f"MERGE completed in {merge_duration:.2f}s")
        print(f"Throughput: {metrics['rows_per_second']:,.0f} rows/sec")

        self.results.append(metrics)
        return metrics

    def benchmark_time_travel(
        self,
        table_name: str,
        versions_to_test: int = 5
    ) -> Dict:
        """
        Benchmark Time Travel query performance

        Args:
            table_name: Table name
            versions_to_test: Number of versions to query

        Returns:
            Performance metrics
        """
        print(f"\n=== Benchmarking Time Travel (versions: {versions_to_test}) ===")

        # Get available versions
        history = self.spark.sql(f"DESCRIBE HISTORY {table_name}")
        versions = [row.version for row in history.limit(versions_to_test).collect()]

        time_travel_times = []

        for version in versions:
            start_time = time.time()

            df = self.spark.read.format("delta") \
                .option("versionAsOf", version) \
                .table(table_name)

            count = df.count()
            duration = time.time() - start_time

            time_travel_times.append(duration)
            print(f"  Version {version}: {count:,} rows in {duration:.2f}s")

        avg_duration = sum(time_travel_times) / len(time_travel_times)

        metrics = {
            "operation": "time_travel",
            "versions_tested": len(versions),
            "avg_duration_seconds": avg_duration,
            "min_duration": min(time_travel_times),
            "max_duration": max(time_travel_times)
        }

        print(f"Average Time Travel query: {avg_duration:.2f}s")

        self.results.append(metrics)
        return metrics

    def benchmark_zorder_impact(
        self,
        table_name: str,
        zorder_columns: List[str]
    ) -> Dict:
        """
        Benchmark Z-Ordering performance impact

        Args:
            table_name: Table name
            zorder_columns: Columns to Z-Order by

        Returns:
            Performance metrics
        """
        print(f"\n=== Benchmarking Z-Order Impact ===")

        # Query performance before Z-Ordering
        query = f"SELECT * FROM {table_name} WHERE user_id = '12345' AND date = current_date()"

        print("Testing query performance BEFORE Z-Ordering...")
        start_time = time.time()
        before_count = self.spark.sql(query).count()
        before_duration = time.time() - start_time
        print(f"  Before: {before_duration:.2f}s")

        # Apply Z-Ordering
        print(f"Applying Z-Order on {zorder_columns}...")
        zorder_start = time.time()
        zorder_cols = ", ".join(zorder_columns)
        self.spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})")
        zorder_duration = time.time() - zorder_start
        print(f"  Z-Ordering completed in {zorder_duration:.2f}s")

        # Query performance after Z-Ordering
        print("Testing query performance AFTER Z-Ordering...")
        start_time = time.time()
        after_count = self.spark.sql(query).count()
        after_duration = time.time() - start_time
        print(f"  After: {after_duration:.2f}s")

        improvement = ((before_duration - after_duration) / before_duration) * 100

        metrics = {
            "operation": "zorder",
            "zorder_columns": zorder_columns,
            "zorder_duration": zorder_duration,
            "query_before_seconds": before_duration,
            "query_after_seconds": after_duration,
            "improvement_percent": improvement
        }

        print(f"Query performance improvement: {improvement:.1f}%")

        self.results.append(metrics)
        return metrics

    def benchmark_partition_pruning(
        self,
        table_name: str,
        partition_column: str
    ) -> Dict:
        """
        Benchmark partition pruning effectiveness

        Args:
            table_name: Table name
            partition_column: Partition column

        Returns:
            Performance metrics
        """
        print(f"\n=== Benchmarking Partition Pruning ===")

        # Full scan without partition filter
        print("Full table scan (no partition filter)...")
        start_time = time.time()
        full_count = self.spark.table(table_name).count()
        full_scan_duration = time.time() - start_time
        print(f"  Full scan: {full_scan_duration:.2f}s ({full_count:,} rows)")

        # Scan with partition filter
        print("Scan with partition filter...")
        start_time = time.time()
        filtered_count = self.spark.table(table_name) \
            .filter(col(partition_column) == current_date()) \
            .count()
        pruned_duration = time.time() - start_time
        print(f"  Pruned scan: {pruned_duration:.2f}s ({filtered_count:,} rows)")

        improvement = ((full_scan_duration - pruned_duration) / full_scan_duration) * 100

        metrics = {
            "operation": "partition_pruning",
            "partition_column": partition_column,
            "full_scan_seconds": full_scan_duration,
            "pruned_scan_seconds": pruned_duration,
            "improvement_percent": improvement,
            "data_scanned_percent": (filtered_count / full_count) * 100
        }

        print(f"Partition pruning speedup: {improvement:.1f}%")
        print(f"Data scanned: {metrics['data_scanned_percent']:.1f}% of total")

        self.results.append(metrics)
        return metrics

    def benchmark_optimize_impact(
        self,
        table_name: str
    ) -> Dict:
        """
        Benchmark OPTIMIZE operation impact

        Args:
            table_name: Table name

        Returns:
            Performance metrics
        """
        print(f"\n=== Benchmarking OPTIMIZE Impact ===")

        # Get table details before
        details_before = self.spark.sql(f"DESCRIBE DETAIL {table_name}").first()
        files_before = details_before["numFiles"]
        size_before = details_before["sizeInBytes"]

        print(f"Before OPTIMIZE: {files_before} files, {size_before / (1024**3):.2f} GB")

        # Run OPTIMIZE
        start_time = time.time()
        self.spark.sql(f"OPTIMIZE {table_name}")
        optimize_duration = time.time() - start_time

        # Get table details after
        details_after = self.spark.sql(f"DESCRIBE DETAIL {table_name}").first()
        files_after = details_after["numFiles"]
        size_after = details_after["sizeInBytes"]

        print(f"After OPTIMIZE: {files_after} files, {size_after / (1024**3):.2f} GB")
        print(f"OPTIMIZE completed in {optimize_duration:.2f}s")

        metrics = {
            "operation": "optimize",
            "files_before": files_before,
            "files_after": files_after,
            "files_reduced": files_before - files_after,
            "size_before_gb": size_before / (1024**3),
            "size_after_gb": size_after / (1024**3),
            "optimize_duration": optimize_duration,
            "file_reduction_percent": ((files_before - files_after) / files_before) * 100
        }

        print(f"File reduction: {metrics['file_reduction_percent']:.1f}%")

        self.results.append(metrics)
        return metrics

    def run_comprehensive_benchmark(
        self,
        num_rows: int = 10000000
    ) -> Dict:
        """
        Run comprehensive benchmark suite

        Args:
            num_rows: Number of rows for tests

        Returns:
            All benchmark results
        """
        print("="*80)
        print("DELTA LAKE PERFORMANCE BENCHMARK SUITE")
        print("="*80)

        table_name = "benchmark.comprehensive_test"

        # 1. Write Performance
        write_metrics = self.benchmark_write_performance(
            num_rows=num_rows,
            target_table=table_name
        )

        # 2. Read Performance
        read_metrics = self.benchmark_read_performance(table_name)
        cached_read_metrics = self.benchmark_read_performance(table_name, use_cache=True)

        # 3. MERGE Performance
        merge_metrics = self.benchmark_merge_performance(
            target_table=table_name,
            num_updates=num_rows // 10
        )

        # 4. Time Travel Performance
        time_travel_metrics = self.benchmark_time_travel(table_name)

        # 5. Z-Ordering Impact
        zorder_metrics = self.benchmark_zorder_impact(
            table_name=table_name,
            zorder_columns=["user_id", "date"]
        )

        # 6. Partition Pruning
        pruning_metrics = self.benchmark_partition_pruning(
            table_name=table_name,
            partition_column="date"
        )

        # 7. OPTIMIZE Impact
        optimize_metrics = self.benchmark_optimize_impact(table_name)

        # Generate report
        self.generate_report()

        return {
            "write": write_metrics,
            "read": read_metrics,
            "cached_read": cached_read_metrics,
            "merge": merge_metrics,
            "time_travel": time_travel_metrics,
            "zorder": zorder_metrics,
            "partition_pruning": pruning_metrics,
            "optimize": optimize_metrics
        }

    def generate_report(self):
        """Generate benchmark report"""
        print("\n" + "="*80)
        print("BENCHMARK REPORT")
        print("="*80)

        results_df = pd.DataFrame(self.results)

        print("\nOperation Summary:")
        print(results_df.to_string(index=False))

        # Save to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"benchmark_report_{timestamp}.json"

        with open(report_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)

        print(f"\nDetailed report saved to: {report_file}")

    def _estimate_throughput(self, rows: int, seconds: float) -> float:
        """Estimate throughput in MB/s"""
        avg_row_size_kb = 1
        total_mb = (rows * avg_row_size_kb) / 1024
        return total_mb / seconds if seconds > 0 else 0


if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Delta Lake Performance Benchmark") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Run benchmark
    benchmark = DeltaPerformanceBenchmark(spark)

    # Run comprehensive benchmark with 10M rows
    results = benchmark.run_comprehensive_benchmark(num_rows=10000000)

    print("\n" + "="*80)
    print("BENCHMARK COMPLETED SUCCESSFULLY")
    print("="*80)
