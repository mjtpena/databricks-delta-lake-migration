"""
Configuration management for Databricks Delta Lake Migration

Centralized configuration for all environments and processing parameters
"""

import os
from typing import Dict, Optional
from dataclasses import dataclass
from enum import Enum


class Environment(Enum):
    """Deployment environments"""
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


@dataclass
class SparkConfig:
    """Spark configuration settings"""
    shuffle_partitions: int = 2000
    max_partition_bytes: int = 134217728  # 128MB
    adaptive_execution: bool = True
    broadcast_threshold: int = 104857600  # 100MB
    compression_codec: str = "snappy"


@dataclass
class DeltaConfig:
    """Delta Lake configuration settings"""
    optimize_write: bool = True
    auto_compact: bool = True
    enable_cdf: bool = True  # Change Data Feed
    retention_hours: int = 168  # 7 days
    log_retention_days: int = 30


@dataclass
class ProcessingConfig:
    """Data processing configuration"""
    batch_size: int = 5000000  # 5M rows
    max_files_per_trigger: int = 100
    checkpoint_interval: str = "30 seconds"
    max_concurrent_jobs: int = 5


@dataclass
class CatalogConfig:
    """Unity Catalog configuration"""
    catalog_name: str
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"


class Config:
    """Main configuration class"""

    def __init__(self, environment: Environment = Environment.DEV):
        self.environment = environment
        self.spark_config = SparkConfig()
        self.delta_config = DeltaConfig()
        self.processing_config = ProcessingConfig()

        # Environment-specific settings
        self._load_environment_config()

    def _load_environment_config(self):
        """Load environment-specific configuration"""
        env = self.environment

        if env == Environment.PROD:
            # Production optimizations
            self.spark_config.shuffle_partitions = 4000
            self.processing_config.batch_size = 10000000  # 10M rows
            self.delta_config.retention_hours = 336  # 14 days
            self.processing_config.max_concurrent_jobs = 10

        elif env == Environment.STAGING:
            # Staging optimizations
            self.spark_config.shuffle_partitions = 1000
            self.processing_config.batch_size = 2000000  # 2M rows
            self.processing_config.max_concurrent_jobs = 3

        elif env == Environment.DEV:
            # Development settings (smaller scale)
            self.spark_config.shuffle_partitions = 200
            self.processing_config.batch_size = 1000000  # 1M rows
            self.delta_config.retention_hours = 24
            self.processing_config.max_concurrent_jobs = 2

    def get_catalog_config(self) -> CatalogConfig:
        """Get catalog configuration for current environment"""
        catalog_name = f"delta_migration_{self.environment.value}"
        return CatalogConfig(catalog_name=catalog_name)

    def get_spark_config_dict(self) -> Dict[str, str]:
        """Get Spark configuration as dictionary"""
        return {
            "spark.sql.shuffle.partitions": str(self.spark_config.shuffle_partitions),
            "spark.sql.files.maxPartitionBytes": str(self.spark_config.max_partition_bytes),
            "spark.sql.adaptive.enabled": str(self.spark_config.adaptive_execution).lower(),
            "spark.sql.autoBroadcastJoinThreshold": str(self.spark_config.broadcast_threshold),
            "spark.sql.parquet.compression.codec": self.spark_config.compression_codec,
            "spark.databricks.delta.optimizeWrite.enabled": str(self.delta_config.optimize_write).lower(),
            "spark.databricks.delta.autoCompact.enabled": str(self.delta_config.auto_compact).lower(),
            "spark.databricks.io.cache.enabled": "true",
        }

    def get_delta_properties(self) -> Dict[str, str]:
        """Get Delta table properties"""
        return {
            "delta.enableChangeDataFeed": str(self.delta_config.enable_cdf).lower(),
            "delta.autoOptimize.optimizeWrite": str(self.delta_config.optimize_write).lower(),
            "delta.autoOptimize.autoCompact": str(self.delta_config.auto_compact).lower(),
            "delta.deletedFileRetentionDuration": f"interval {self.delta_config.retention_hours} hours",
            "delta.logRetentionDuration": f"interval {self.delta_config.log_retention_days} days",
        }

    @staticmethod
    def from_environment_variable(var_name: str = "ENVIRONMENT") -> "Config":
        """Create config from environment variable"""
        env_value = os.getenv(var_name, "dev").lower()
        environment = Environment(env_value)
        return Config(environment)


# Predefined configurations
DEV_CONFIG = Config(Environment.DEV)
STAGING_CONFIG = Config(Environment.STAGING)
PROD_CONFIG = Config(Environment.PROD)


# Table configurations
TABLE_CONFIGS = {
    "events": {
        "partition_columns": ["ingestion_date"],
        "zorder_columns": ["event_date", "user_id"],
        "optimize_interval_hours": 24,
    },
    "transactions": {
        "partition_columns": ["ingestion_date"],
        "zorder_columns": ["transaction_date", "user_id"],
        "optimize_interval_hours": 12,
    },
    "users": {
        "partition_columns": None,
        "zorder_columns": ["user_id"],
        "optimize_interval_hours": 168,  # Weekly
    },
}


def get_table_config(table_name: str) -> Dict:
    """Get configuration for a specific table"""
    return TABLE_CONFIGS.get(table_name, {
        "partition_columns": None,
        "zorder_columns": [],
        "optimize_interval_hours": 24,
    })
