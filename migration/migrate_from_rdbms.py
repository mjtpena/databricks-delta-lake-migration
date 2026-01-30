"""
Migration from RDBMS (Oracle, SQL Server, PostgreSQL, MySQL) to Delta Lake

This script handles full and incremental migration from relational databases
to Databricks Delta Lake with optimizations for 10TB+ datasets.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from typing import Dict, List, Optional
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RDBMSMigrator:
    """Migrate data from RDBMS to Delta Lake"""

    def __init__(self, spark: SparkSession, source_config: Dict):
        """
        Initialize migrator

        Args:
            spark: SparkSession
            source_config: RDBMS connection configuration
        """
        self.spark = spark
        self.source_config = source_config
        self.jdbc_url = self._build_jdbc_url()

    def _build_jdbc_url(self) -> str:
        """Build JDBC connection URL"""
        db_type = self.source_config["type"]
        host = self.source_config["host"]
        port = self.source_config["port"]
        database = self.source_config["database"]

        if db_type == "oracle":
            return f"jdbc:oracle:thin:@{host}:{port}:{database}"
        elif db_type == "sqlserver":
            return f"jdbc:sqlserver://{host}:{port};databaseName={database}"
        elif db_type == "postgresql":
            return f"jdbc:postgresql://{host}:{port}/{database}"
        elif db_type == "mysql":
            return f"jdbc:mysql://{host}:{port}/{database}"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    def full_table_migration(
        self,
        source_table: str,
        target_table: str,
        partition_column: Optional[str] = None,
        num_partitions: int = 200,
        fetch_size: int = 10000
    ) -> Dict:
        """
        Perform full table migration from RDBMS to Delta Lake

        Args:
            source_table: Source table name in RDBMS
            target_table: Target Delta table name
            partition_column: Column to use for parallel reads
            num_partitions: Number of parallel read partitions
            fetch_size: JDBC fetch size

        Returns:
            Migration metrics
        """
        logger.info(f"Starting full migration: {source_table} -> {target_table}")
        start_time = datetime.now()

        # Read from RDBMS with optimizations
        read_options = {
            "url": self.jdbc_url,
            "dbtable": source_table,
            "user": self.source_config["username"],
            "password": self.source_config["password"],
            "driver": self._get_jdbc_driver(),
            "fetchSize": str(fetch_size),
        }

        # Add partitioning for parallel reads
        if partition_column:
            read_options.update({
                "partitionColumn": partition_column,
                "numPartitions": str(num_partitions),
                "lowerBound": "1",
                "upperBound": str(num_partitions * 1000000)
            })

        df = self.spark.read.format("jdbc").options(**read_options).load()

        # Add migration metadata
        df = df.withColumn("migration_timestamp", current_timestamp()) \
               .withColumn("source_system", lit(self.source_config["type"])) \
               .withColumn("source_table", lit(source_table))

        # Write to Delta Lake
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("optimizeWrite", "true") \
            .saveAsTable(target_table)

        # Calculate metrics
        row_count = df.count()
        duration = (datetime.now() - start_time).total_seconds()

        metrics = {
            "source_table": source_table,
            "target_table": target_table,
            "rows_migrated": row_count,
            "duration_seconds": duration,
            "rows_per_second": row_count / duration if duration > 0 else 0,
            "migration_timestamp": datetime.now().isoformat()
        }

        logger.info(f"Migration completed: {metrics}")
        return metrics

    def incremental_migration(
        self,
        source_table: str,
        target_table: str,
        key_columns: List[str],
        incremental_column: str,
        last_value: Optional[str] = None,
        fetch_size: int = 10000
    ) -> Dict:
        """
        Perform incremental migration based on timestamp or sequence column

        Args:
            source_table: Source table name
            target_table: Target Delta table name
            key_columns: Primary key columns for merge
            incremental_column: Column to use for incremental load (timestamp/sequence)
            last_value: Last processed value (None for initial load)
            fetch_size: JDBC fetch size

        Returns:
            Migration metrics
        """
        logger.info(f"Starting incremental migration: {source_table}")
        start_time = datetime.now()

        # Build incremental query
        if last_value:
            query = f"(SELECT * FROM {source_table} WHERE {incremental_column} > '{last_value}') AS incremental_data"
        else:
            query = f"(SELECT * FROM {source_table}) AS initial_data"

        # Read incremental data
        df = self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", query) \
            .option("user", self.source_config["username"]) \
            .option("password", self.source_config["password"]) \
            .option("driver", self._get_jdbc_driver()) \
            .option("fetchSize", str(fetch_size)) \
            .load()

        row_count = df.count()

        if row_count == 0:
            logger.info("No new data to migrate")
            return {"rows_migrated": 0, "max_incremental_value": last_value}

        # Add migration metadata
        df = df.withColumn("migration_timestamp", current_timestamp()) \
               .withColumn("source_system", lit(self.source_config["type"]))

        # Get max value for next run
        max_value = df.agg(max(incremental_column)).first()[0]

        # Merge into Delta Lake
        target_delta = DeltaTable.forName(self.spark, target_table)

        merge_condition = " AND ".join(
            [f"target.{col} = source.{col}" for col in key_columns]
        )

        target_delta.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        duration = (datetime.now() - start_time).total_seconds()

        metrics = {
            "source_table": source_table,
            "target_table": target_table,
            "rows_migrated": row_count,
            "duration_seconds": duration,
            "rows_per_second": row_count / duration if duration > 0 else 0,
            "max_incremental_value": str(max_value),
            "migration_timestamp": datetime.now().isoformat()
        }

        logger.info(f"Incremental migration completed: {metrics}")
        return metrics

    def batch_table_migration(
        self,
        tables: List[Dict],
        max_parallel: int = 5
    ) -> List[Dict]:
        """
        Migrate multiple tables in parallel batches

        Args:
            tables: List of table configurations
            max_parallel: Maximum parallel migrations

        Returns:
            List of migration results
        """
        logger.info(f"Starting batch migration of {len(tables)} tables")

        results = []
        for i in range(0, len(tables), max_parallel):
            batch = tables[i:i + max_parallel]
            logger.info(f"Processing batch {i // max_parallel + 1}")

            for table_config in batch:
                try:
                    if table_config.get("incremental"):
                        result = self.incremental_migration(
                            source_table=table_config["source_table"],
                            target_table=table_config["target_table"],
                            key_columns=table_config["key_columns"],
                            incremental_column=table_config["incremental_column"],
                            last_value=table_config.get("last_value")
                        )
                    else:
                        result = self.full_table_migration(
                            source_table=table_config["source_table"],
                            target_table=table_config["target_table"],
                            partition_column=table_config.get("partition_column"),
                            num_partitions=table_config.get("num_partitions", 200)
                        )

                    results.append({"status": "success", **result})

                except Exception as e:
                    logger.error(f"Failed to migrate {table_config['source_table']}: {e}")
                    results.append({
                        "status": "failed",
                        "source_table": table_config["source_table"],
                        "error": str(e)
                    })

        logger.info(f"Batch migration completed: {len(results)} tables processed")
        return results

    def validate_migration(
        self,
        source_table: str,
        target_table: str,
        sample_size: int = 10000
    ) -> Dict:
        """
        Validate migration by comparing source and target

        Args:
            source_table: Source RDBMS table
            target_table: Target Delta table
            sample_size: Number of rows to sample for validation

        Returns:
            Validation results
        """
        logger.info(f"Validating migration: {source_table} -> {target_table}")

        # Count rows in source
        source_count_query = f"(SELECT COUNT(*) as cnt FROM {source_table}) AS count_query"
        source_count = self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", source_count_query) \
            .option("user", self.source_config["username"]) \
            .option("password", self.source_config["password"]) \
            .option("driver", self._get_jdbc_driver()) \
            .load().first()["cnt"]

        # Count rows in target
        target_count = self.spark.table(target_table).count()

        # Sample comparison
        source_sample_query = f"(SELECT * FROM {source_table} LIMIT {sample_size}) AS sample_query"
        source_sample = self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", source_sample_query) \
            .option("user", self.source_config["username"]) \
            .option("password", self.source_config["password"]) \
            .option("driver", self._get_jdbc_driver()) \
            .load()

        target_sample = self.spark.table(target_table).limit(sample_size)

        # Compare schemas
        source_cols = set(source_sample.columns)
        target_cols = set([c for c in target_sample.columns if c not in ["migration_timestamp", "source_system", "source_table"]])

        validation_results = {
            "source_row_count": source_count,
            "target_row_count": target_count,
            "row_count_match": source_count == target_count,
            "row_count_difference": abs(source_count - target_count),
            "schema_match": source_cols == target_cols,
            "missing_columns": list(source_cols - target_cols),
            "extra_columns": list(target_cols - source_cols),
            "validation_timestamp": datetime.now().isoformat()
        }

        if validation_results["row_count_match"] and validation_results["schema_match"]:
            logger.info("✓ Migration validation PASSED")
        else:
            logger.warning(f"✗ Migration validation FAILED: {validation_results}")

        return validation_results

    def _get_jdbc_driver(self) -> str:
        """Get JDBC driver class name"""
        db_type = self.source_config["type"]

        drivers = {
            "oracle": "oracle.jdbc.driver.OracleDriver",
            "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "postgresql": "org.postgresql.Driver",
            "mysql": "com.mysql.cj.jdbc.Driver"
        }

        return drivers.get(db_type, "")


# Example usage configuration
EXAMPLE_RDBMS_CONFIG = {
    "type": "postgresql",
    "host": "legacy-db.example.com",
    "port": 5432,
    "database": "production",
    "username": "migration_user",
    "password": "password"  # Use secrets in production
}

EXAMPLE_MIGRATION_TABLES = [
    {
        "source_table": "customers",
        "target_table": "delta_migration.bronze.customers",
        "partition_column": "customer_id",
        "num_partitions": 200,
        "incremental": False
    },
    {
        "source_table": "orders",
        "target_table": "delta_migration.bronze.orders",
        "key_columns": ["order_id"],
        "incremental_column": "updated_at",
        "incremental": True,
        "last_value": "2024-01-01 00:00:00"
    }
]


if __name__ == "__main__":
    # Example usage (run on Databricks)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("RDBMS Migration") \
        .getOrCreate()

    migrator = RDBMSMigrator(spark, EXAMPLE_RDBMS_CONFIG)

    # Perform batch migration
    results = migrator.batch_table_migration(EXAMPLE_MIGRATION_TABLES)

    # Print results
    for result in results:
        print(f"Table: {result.get('source_table')}, Status: {result['status']}")
        if result['status'] == 'success':
            print(f"  Rows: {result['rows_migrated']:,}, Duration: {result['duration_seconds']:.2f}s")
