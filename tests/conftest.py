"""
Pytest configuration and shared fixtures for Fabric ELT Framework tests
"""

import pytest
import json
import os
from unittest.mock import MagicMock, patch

# Add project root to path
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

@pytest.fixture
def mock_spark_session():
    """Mock Spark session for unit tests."""
    spark = MagicMock()
    spark.read = MagicMock()
    spark.sql = MagicMock()
    spark.createDataFrame = MagicMock()
    spark.sparkContext = MagicMock()
    spark.sparkContext.applicationId = "test-app-123"
    return spark

@pytest.fixture
def sample_entity_config():
    """Sample entity configuration for testing."""
    return {
        "EntityId": 1,
        "SourceId": 1,
        "EntityName": "Customers",
        "EntityType": "TABLE",
        "SourceSchema": "dbo",
        "TargetLakehouse": "lh_bronze",
        "TargetSchema": "raw",
        "TargetTableName": "customers",
        "LoadType": "INCREMENTAL",
        "WatermarkColumn": "ModifiedDate",
        "WatermarkDataType": "DATETIME",
        "WatermarkOffset": "1900-01-01",
        "ScheduleExpression": "0 2 * * *",
        "ParallelismDegree": 2,
        "Priority": 1,
        "RetryCount": 3,
        "RetryIntervalSec": 60,
        "TimeoutMinutes": 120
    }

@pytest.fixture
def sample_gold_model_config():
    """Sample Gold model configuration for testing."""
    return {
        "ModelId": 1,
        "ModelName": "dim_customers",
        "ModelType": "DIMENSION",
        "TargetWarehouse": "wh_gold",
        "TargetSchema": "curated",
        "TargetTableName": "dim_customers",
        "SourceLakehouse": "lh_silver",
        "SourceSchema": "cleaned",
        "SourceTables": '["customers_scd2"]',
        "GrainColumns": "CustomerID",
        "CalculatedColumns": None,
        "DependencyModels": None,
        "IsActive": True,
        "Priority": 1
    }

@pytest.fixture
def sample_dq_rules():
    """Sample data quality rules for testing."""
    return [
        {
            "RuleId": 1,
            "EntityId": 1,
            "RuleName": "CustomerID Not Null",
            "RuleType": "NOT_NULL",
            "ColumnName": "CustomerID",
            "ExpectedValue": None,
            "Severity": "CRITICAL",
            "QuarantineEnabled": True
        },
        {
            "RuleId": 2,
            "EntityId": 1,
            "RuleName": "Email Valid Format",
            "RuleType": "REGEX",
            "ColumnName": "Email",
            "ExpectedValue": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",  # raw string for regex
            "Severity": "ERROR",
            "QuarantineEnabled": True
        }
    ]

@pytest.fixture
def mock_control_db_response():
    """Mock JDBC response from control database."""
    return [
        {
            "EntityId": 1,
            "SourceName": "azsql-customers",
            "SourceType": "SQL",
            "ConnectionStringRef": "kv-azsql-customers-conn",
            "AuthenticationType": "ManagedIdentity",
            "EntityName": "Customers",
            "EntityType": "TABLE",
            "SourceSchema": "dbo",
            "TargetLakehouse": "lh_bronze",
            "TargetSchema": "raw",
            "TargetTableName": "customers",
            "LoadType": "INCREMENTAL",
            "WatermarkColumn": "ModifiedDate",
            "WatermarkDataType": "DATETIME",
            "WatermarkOffset": "1900-01-01",
            "LastWatermark": "2026-06-01 00:00:00.000",
            "ScheduleExpression": "0 2 * * *",
            "ParallelismDegree": 2,
            "Priority": 1,
            "RetryCount": 3,
            "RetryIntervalSec": 60,
            "TimeoutMinutes": 120,
            "PreIngestSql": None,
            "PostIngestSql": None,
            "SourceFilterClause": None
        }
    ]

@pytest.fixture
def temp_delta_table(tmp_path):
    """Create a temporary Delta table for testing."""
    import pandas as pd
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder
        .appName("TestSession")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate())

    df = spark.createDataFrame([
        (1, "Alice", "alice@example.com", "2026-06-01"),
        (2, "Bob", "bob@example.com", "2026-06-02"),
        (3, "Charlie", "charlie@example.com", "2026-06-03")
    ], ["CustomerID", "CustomerName", "Email", "ModifiedDate"])

    table_path = str(tmp_path / "test_delta_table")
    df.write.format("delta").mode("overwrite").save(table_path)

    yield table_path

    # Cleanup
    spark.stop()
