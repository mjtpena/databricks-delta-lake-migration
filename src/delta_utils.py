"""
Delta Lake utility functions for common operations

Provides helper functions for:
- Table management
- Schema operations
- Metadata queries
- Performance optimization
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class DeltaTableManager:
    """Manager for Delta table operations"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def create_table(
        self,
        table_name: str,
        schema: StructType,
        partition_columns: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Create a new Delta table

        Args:
            table_name: Full table name (catalog.schema.table)
            schema: Table schema
            partition_columns: Columns to partition by
            properties: Table properties

        Returns:
            Table name
        """
        logger.info(f"Creating Delta table: {table_name}")

        # Create empty DataFrame with schema
        empty_df = self.spark.createDataFrame([], schema)

        # Default properties
        default_props = {
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "delta.deletedFileRetentionDuration": "interval 7 days",
            "delta.logRetentionDuration": "interval 30 days"
        }

        if properties:
            default_props.update(properties)

        # Build writer
        writer = empty_df.write.format("delta").mode("ignore")

        # Add partitioning
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        # Add properties
        for key, value in default_props.items():
            writer = writer.option(key, value)

        writer.saveAsTable(table_name)

        logger.info(f"Created table: {table_name}")
        return table_name

    def clone_table(
        self,
        source_table: str,
        target_table: str,
        deep_clone: bool = False
    ) -> str:
        """
        Clone a Delta table (shallow or deep)

        Args:
            source_table: Source table name
            target_table: Target table name
            deep_clone: If True, creates deep clone; otherwise shallow

        Returns:
            Target table name
        """
        clone_type = "DEEP" if deep_clone else "SHALLOW"
        logger.info(f"Creating {clone_type} clone: {source_table} -> {target_table}")

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {target_table}
            {clone_type} CLONE {source_table}
        """)

        logger.info(f"Clone created: {target_table}")
        return target_table

    def merge_schema(
        self,
        table_name: str,
        new_schema_df: DataFrame
    ) -> Dict:
        """
        Merge new schema into existing table

        Args:
            table_name: Table name
            new_schema_df: DataFrame with new schema

        Returns:
            Schema merge results
        """
        logger.info(f"Merging schema for table: {table_name}")

        # Read current table
        current_df = self.spark.table(table_name)
        current_columns = set(current_df.columns)
        new_columns = set(new_schema_df.columns)

        # Find new columns
        added_columns = new_columns - current_columns
        removed_columns = current_columns - new_columns

        if added_columns:
            logger.info(f"Adding columns: {added_columns}")
            # Delta Lake handles schema evolution on write
            new_schema_df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(table_name)

        results = {
            "added_columns": list(added_columns),
            "removed_columns": list(removed_columns),
            "schema_changed": len(added_columns) > 0
        }

        return results

    def get_table_properties(self, table_name: str) -> Dict:
        """Get all table properties"""
        properties_df = self.spark.sql(f"SHOW TBLPROPERTIES {table_name}")
        properties = {row["key"]: row["value"] for row in properties_df.collect()}
        return properties

    def set_table_property(
        self,
        table_name: str,
        property_name: str,
        property_value: str
    ):
        """Set a table property"""
        self.spark.sql(f"""
            ALTER TABLE {table_name}
            SET TBLPROPERTIES ('{property_name}' = '{property_value}')
        """)
        logger.info(f"Set property {property_name} = {property_value} on {table_name}")

    def get_table_history(
        self,
        table_name: str,
        limit: int = 20
    ) -> DataFrame:
        """Get table history"""
        return self.spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT {limit}")

    def restore_table(
        self,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None
    ):
        """
        Restore table to a previous version

        Args:
            table_name: Table name
            version: Version number to restore to
            timestamp: Timestamp to restore to
        """
        if version is not None:
            self.spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF {version}")
            logger.info(f"Restored {table_name} to version {version}")
        elif timestamp is not None:
            self.spark.sql(f"RESTORE TABLE {table_name} TO TIMESTAMP AS OF '{timestamp}'")
            logger.info(f"Restored {table_name} to timestamp {timestamp}")
        else:
            raise ValueError("Must provide either version or timestamp")


class DeltaSchemaUtils:
    """Utilities for Delta Lake schema management"""

    @staticmethod
    def compare_schemas(schema1: StructType, schema2: StructType) -> Dict:
        """
        Compare two schemas and return differences

        Returns:
            Dictionary with added, removed, and changed fields
        """
        fields1 = {f.name: f for f in schema1.fields}
        fields2 = {f.name: f for f in schema2.fields}

        added = [name for name in fields2.keys() if name not in fields1]
        removed = [name for name in fields1.keys() if name not in fields2]

        changed = []
        for name in set(fields1.keys()) & set(fields2.keys()):
            if fields1[name].dataType != fields2[name].dataType:
                changed.append({
                    "field": name,
                    "old_type": str(fields1[name].dataType),
                    "new_type": str(fields2[name].dataType)
                })

        return {
            "added_fields": added,
            "removed_fields": removed,
            "changed_fields": changed,
            "has_changes": len(added) + len(removed) + len(changed) > 0
        }

    @staticmethod
    def generate_schema_from_json(json_sample: str) -> StructType:
        """Generate schema from JSON sample"""
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        df = spark.read.json(spark.sparkContext.parallelize([json_sample]))
        return df.schema

    @staticmethod
    def schema_to_ddl(schema: StructType, table_name: str) -> str:
        """Convert schema to CREATE TABLE DDL"""
        fields_ddl = []

        for field in schema.fields:
            nullable = "NULL" if field.nullable else "NOT NULL"
            comment = f" COMMENT '{field.metadata.get('comment')}'" if field.metadata.get('comment') else ""
            fields_ddl.append(f"  {field.name} {field.dataType.simpleString().upper()} {nullable}{comment}")

        ddl = f"CREATE TABLE {table_name} (\n"
        ddl += ",\n".join(fields_ddl)
        ddl += "\n) USING DELTA"

        return ddl


class DeltaPerformanceOptimizer:
    """Optimize Delta Lake performance"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def analyze_table_stats(self, table_name: str) -> Dict:
        """Analyze table and return performance statistics"""
        details = self.spark.sql(f"DESCRIBE DETAIL {table_name}").first()
        history = self.spark.sql(f"DESCRIBE HISTORY {table_name}").first()

        stats = {
            "num_files": details["numFiles"],
            "size_in_bytes": details["sizeInBytes"],
            "size_in_gb": details["sizeInBytes"] / (1024**3),
            "avg_file_size_mb": (details["sizeInBytes"] / details["numFiles"]) / (1024**2) if details["numFiles"] > 0 else 0,
            "partition_columns": details["partitionColumns"],
            "last_operation": history["operation"] if history else None
        }

        # Recommendations
        recommendations = []

        if stats["avg_file_size_mb"] < 64:
            recommendations.append("Files are small - consider running OPTIMIZE")

        if stats["num_files"] > 1000:
            recommendations.append("Many small files detected - run OPTIMIZE to compact")

        if not stats["partition_columns"]:
            recommendations.append("Consider partitioning large tables for better performance")

        stats["recommendations"] = recommendations

        return stats

    def optimize_with_zorder(
        self,
        table_name: str,
        zorder_columns: List[str],
        dry_run: bool = False
    ) -> Dict:
        """
        Optimize table with Z-Ordering

        Args:
            table_name: Table name
            zorder_columns: Columns to Z-Order by
            dry_run: If True, only show what would be done

        Returns:
            Optimization results
        """
        if dry_run:
            logger.info(f"DRY RUN: Would optimize {table_name} with Z-Order on {zorder_columns}")
            return {"dry_run": True}

        logger.info(f"Optimizing {table_name} with Z-Order on {zorder_columns}")

        zorder_cols = ", ".join(zorder_columns)
        self.spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})")

        # Get optimization metrics
        history = self.spark.sql(f"DESCRIBE HISTORY {table_name}").first()
        metrics = history["operationMetrics"]

        results = {
            "table": table_name,
            "zorder_columns": zorder_columns,
            "files_added": metrics.get("numAddedFiles", 0),
            "files_removed": metrics.get("numRemovedFiles", 0),
            "bytes_added": metrics.get("numAddedBytes", 0),
            "bytes_removed": metrics.get("numRemovedBytes", 0)
        }

        logger.info(f"Optimization complete: {results}")
        return results

    def vacuum_table(
        self,
        table_name: str,
        retention_hours: int = 168,
        dry_run: bool = False
    ) -> Dict:
        """
        Vacuum table to remove old files

        Args:
            table_name: Table name
            retention_hours: Retention period in hours
            dry_run: If True, show files that would be deleted

        Returns:
            Vacuum results
        """
        if dry_run:
            logger.info(f"DRY RUN: Vacuum {table_name} (retention: {retention_hours}h)")
            result_df = self.spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS DRY RUN")
            files_to_delete = result_df.count()
            return {
                "dry_run": True,
                "files_to_delete": files_to_delete
            }

        logger.info(f"Vacuuming {table_name} (retention: {retention_hours}h)")
        self.spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")

        return {
            "table": table_name,
            "retention_hours": retention_hours,
            "completed": True
        }


class DeltaChangeDataCapture:
    """Utilities for Change Data Capture with Delta Lake"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def enable_cdc(self, table_name: str):
        """Enable Change Data Feed for a table"""
        self.spark.sql(f"""
            ALTER TABLE {table_name}
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
        logger.info(f"Enabled Change Data Feed for {table_name}")

    def get_changes(
        self,
        table_name: str,
        start_version: Optional[int] = None,
        end_version: Optional[int] = None,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None
    ) -> DataFrame:
        """
        Get changes between versions or timestamps

        Returns DataFrame with _change_type column:
        - insert: New row inserted
        - update_preimage: Row before update
        - update_postimage: Row after update
        - delete: Row deleted
        """
        reader = self.spark.read.format("delta") \
            .option("readChangeFeed", "true")

        if start_version is not None:
            reader = reader.option("startingVersion", start_version)
        if end_version is not None:
            reader = reader.option("endingVersion", end_version)
        if start_timestamp is not None:
            reader = reader.option("startingTimestamp", start_timestamp)
        if end_timestamp is not None:
            reader = reader.option("endingTimestamp", end_timestamp)

        return reader.table(table_name)

    def track_row_changes(
        self,
        table_name: str,
        key_column: str,
        start_version: int,
        end_version: int
    ) -> DataFrame:
        """
        Track changes for specific rows

        Returns summary of changes per key
        """
        changes_df = self.get_changes(table_name, start_version, end_version)

        change_summary = changes_df.groupBy(key_column, "_change_type").agg(
            count("*").alias("change_count"),
            min("_commit_timestamp").alias("first_change"),
            max("_commit_timestamp").alias("last_change")
        ).orderBy(key_column, "_change_type")

        return change_summary
