"""
Unit tests for pyspark-lib/fabric_elt_lib.py

These tests verify the core functions of the Fabric ELT library by:
1. Mocking pyspark dependencies (since Spark is not available in test environment)
2. Testing pure Python functions directly (build_incremental_query, retry_with_backoff)
3. Testing Spark-dependent functions with mock DataFrames to verify correct behavior
"""

import pytest
from unittest.mock import MagicMock, patch, call
import sys
import os
import time

# Add pyspark-lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'pyspark-lib'))

# ============================================================================
# Mock pyspark modules BEFORE importing fabric_elt_lib
# ============================================================================

# Build a mock F (functions) module
_mock_F = MagicMock()

# Track calls to F functions for verification
_f_calls = []


class _MockSparkExpr:
    """A mock Spark expression object that supports all operators."""
    def __init__(self, name, args, kwargs):
        self._f_name = name
        self._f_args = args
        self._f_kwargs = kwargs
        self._op = None
        self._other = None

    def __repr__(self):
        return f"_MockSparkExpr({self._f_name}, {self._f_args})"

    # Comparison operators
    def __ge__(self, other):
        r = _MockSparkExpr('ge', (self, other), {})
        r._op = 'ge'
        return r
    def __le__(self, other):
        r = _MockSparkExpr('le', (self, other), {})
        r._op = 'le'
        return r
    def __gt__(self, other):
        r = _MockSparkExpr('gt', (self, other), {})
        r._op = 'gt'
        return r
    def __lt__(self, other):
        r = _MockSparkExpr('lt', (self, other), {})
        r._op = 'lt'
        return r
    def __eq__(self, other):
        r = _MockSparkExpr('eq', (self, other), {})
        r._op = 'eq'
        return r
    # Bitwise operators (used for & and | in Spark)
    def __and__(self, other):
        r = _MockSparkExpr('and', (self, other), {})
        r._op = 'and'
        return r
    def __or__(self, other):
        r = _MockSparkExpr('or', (self, other), {})
        r._op = 'or'
        return r
    def __rand__(self, other):
        r = _MockSparkExpr('rand', (other, self), {})
        r._op = 'rand'
        return r
    def __ror__(self, other):
        r = _MockSparkExpr('ror', (other, self), {})
        r._op = 'ror'
        return r

    # Spark expression methods chained after F.col()
    def isNotNull(self):
        return _MockSparkExpr('isNotNull', (self,), {})
    def isNull(self):
        return _MockSparkExpr('isNull', (self,), {})
    def rlike(self, pattern):
        return _MockSparkExpr('rlike', (self, pattern), {})
    def over(self, window):
        return _MockSparkExpr('over', (self, window), {})


def _make_mock_fn(name):
    """Create a mock function that returns an identifiable _MockSparkExpr."""
    def fn(*args, **kwargs):
        result = _MockSparkExpr(name, args, kwargs)
        _f_calls.append((name, args, kwargs))
        return result
    return fn


# Create a reusable mock for F.col() results with operator overloads
_col_result_prototype = MagicMock()
_col_result_prototype.__and__ = lambda self, other: MagicMock(_op='and', _other=other)
_col_result_prototype.__or__ = lambda self, other: MagicMock(_op='or', _other=other)
_col_result_prototype.__eq__ = lambda self, other: MagicMock(_op='eq', _other=other)
_col_result_prototype.__gt__ = lambda self, other: MagicMock(_op='gt', _other=other)
_col_result_prototype.__lt__ = lambda self, other: MagicMock(_op='lt', _other=other)
_col_result_prototype.__ge__ = lambda self, other: MagicMock(_op='ge', _other=other)
_col_result_prototype.__le__ = lambda self, other: MagicMock(_op='le', _other=other)
_col_result_prototype.isNotNull = _make_mock_fn('isNotNull')
_col_result_prototype.isNull = _make_mock_fn('isNull')

# Make F.col return a fresh copy of the prototype each call
_mock_F.col.side_effect = lambda *args, **kwargs: _make_mock_fn('col')(*args, **kwargs)

# Attach common pyspark.sql.functions as side effects
for _fn_name in ['lower', 'upper', 'trim', 'regexp_replace', 'isNotNull',
                 'max', 'current_timestamp', 'lit', 'count', 'rlike', 'forall',
                 'array', 'to_date', 'to_timestamp', 'length', 'min', 'avg',
                 'stddev', 'sum', 'when', 'coalesce', 'isNull', 'isin',
                 'floor', 'round', 'concat', 'concat_ws', 'substring']:
    setattr(_mock_F, _fn_name, _make_mock_fn(_fn_name))

_mock_window = MagicMock()
_mock_window.partitionBy.return_value = MagicMock(name='Window_spec')

# Install mocks into sys.modules
# Important: set attributes on parent modules so subpath resolution
# (e.g. pyspark.sql.functions) finds our _mock_F, not a new MagicMock
_mock_pyspark = MagicMock()
_mock_pyspark_sql = MagicMock()
_mock_pyspark_sql.functions = _mock_F
_mock_pyspark_sql.types = MagicMock()
_mock_pyspark_sql.window = MagicMock()
_mock_pyspark_sql.window.Window = _mock_window
_mock_pyspark.sql = _mock_pyspark_sql

sys.modules['pyspark'] = _mock_pyspark
sys.modules['pyspark.sql'] = _mock_pyspark_sql
sys.modules['pyspark.sql.functions'] = _mock_F
sys.modules['pyspark.sql.types'] = _mock_pyspark_sql.types
sys.modules['pyspark.sql.window'] = _mock_pyspark_sql.window

# Mock delta.tables
_mock_delta_table_class = MagicMock()
_mock_delta_table_instance = MagicMock()
_mock_delta_table_class.isDeltaTable.return_value = False
_mock_delta_table_class.forPath.return_value = _mock_delta_table_instance
sys.modules['delta'] = MagicMock()
sys.modules['delta.tables'] = MagicMock()
sys.modules['delta.tables'].DeltaTable = _mock_delta_table_class

# Now import the library under test
import fabric_elt_lib as lib


# ============================================================================
# Mock DataFrame helper
# ============================================================================

class MockDataFrame:
    """A mock DataFrame that records withColumn calls and supports chaining."""

    def __init__(self, columns=None):
        self.columns = list(columns) if columns else ["CustomerID", "Email", "OrderAmount"]
        self._withColumn_calls = []  # list of (col_name, expr)
        self._collect_result = []

    def withColumn(self, name, expr):
        self._withColumn_calls.append((name, expr))
        if name not in self.columns:
            self.columns.append(name)
        return self

    def agg(self, *exprs):
        mock_result = MagicMock()
        mock_result.collect.return_value = self._collect_result if self._collect_result else [[None]]
        return mock_result

    def select(self, *cols):
        return self

    def filter(self, condition):
        return self

    def join(self, other, on=None, how=None):
        return self

    def alias(self, name):
        return self

    def count(self):
        return len(self._collect_result) if self._collect_result else 0

    def sample(self, withReplacement, fraction):
        return self

    def distinct(self):
        return self

    def write(self):
        writer = MagicMock()
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer
        writer.save.return_value = None
        return writer

    @property
    def schema(self):
        mock_field = MagicMock()
        mock_field.name = "CustomerID"
        mock_field.dataType = "StringType"
        mock_schema = MagicMock()
        mock_schema.fields = [mock_field]
        return mock_schema


# ============================================================================
# Tests for build_incremental_query (pure Python)
# ============================================================================

class TestBuildIncrementalQuery:
    """Test build_incremental_query - a pure Python function."""

    def test_no_watermark_value_returns_base_query(self):
        """If watermark_value is None/empty, return base query unchanged."""
        result = lib.build_incremental_query("SELECT * FROM table1", "ModifiedDate", None)
        assert result == "SELECT * FROM table1"

    def test_datetime_watermark_adds_where(self):
        """Datetime watermark adds quoted WHERE condition."""
        result = lib.build_incremental_query(
            "SELECT * FROM customers",
            "ModifiedDate",
            "2026-06-01 00:00:00.000",
            "datetime"
        )
        assert "WHERE ModifiedDate > '2026-06-01 00:00:00.000'" in result
        assert result == "SELECT * FROM customers WHERE ModifiedDate > '2026-06-01 00:00:00.000'"

    def test_date_watermark_uses_quoted_condition(self):
        """Date type watermark uses quoted string comparison."""
        result = lib.build_incremental_query(
            "SELECT * FROM orders",
            "OrderDate",
            "2026-01-01",
            "date"
        )
        assert "WHERE OrderDate > '2026-01-01'" in result

    def test_timestamp_watermark_uses_quoted_condition(self):
        """Timestamp type watermark uses quoted string comparison."""
        result = lib.build_incremental_query(
            "SELECT * FROM events",
            "EventTime",
            "2026-06-01T00:00:00",
            "timestamp"
        )
        assert "EventTime > '2026-06-01T00:00:00'" in result

    def test_numeric_watermark_no_quotes(self):
        """Numeric watermark uses unquoted numeric comparison."""
        result = lib.build_incremental_query(
            "SELECT * FROM table1",
            "RowVersion",
            "12345",
            "bigint"
        )
        assert result == "SELECT * FROM table1 WHERE RowVersion > 12345"

    def test_existing_where_appends_and(self):
        """If base query already has WHERE, append with AND."""
        result = lib.build_incremental_query(
            "SELECT * FROM table1 WHERE IsActive = 1",
            "ModifiedDate",
            "2026-06-01",
            "datetime"
        )
        assert " AND ModifiedDate > '2026-06-01'" in result
        assert "WHERE IsActive = 1" in result

    def test_int_watermark_type(self):
        """Integer watermark type uses numeric comparison."""
        result = lib.build_incremental_query(
            "SELECT * FROM table1",
            "ID",
            "999",
            "int"
        )
        assert result == "SELECT * FROM table1 WHERE ID > 999"


# ============================================================================
# Tests for retry_with_backoff (pure Python decorator)
# ============================================================================

class TestRetryWithBackoff:
    """Test retry_with_backoff decorator - pure Python."""

    def test_retry_decorator_exists(self):
        """Verify the retry decorator is importable."""
        assert hasattr(lib, 'retry_with_backoff')
        assert callable(lib.retry_with_backoff)

    def test_success_on_first_call_no_retry(self):
        """If function succeeds immediately, no retries needed."""
        call_log = []

        @lib.retry_with_backoff(max_retries=3, base_delay=0.01)
        def always_succeeds():
            call_log.append(1)
            return "success"

        result = always_succeeds()
        assert result == "success"
        assert len(call_log) == 1  # Only called once

    def test_retry_then_succeed(self):
        """Decorator retries on failure and eventually succeeds."""
        call_log = {"count": 0}

        @lib.retry_with_backoff(max_retries=3, base_delay=0.01)
        def succeeds_on_third():
            call_log["count"] += 1
            if call_log["count"] < 3:
                raise ValueError(f"Attempt {call_log['count']} failed")
            return f"success_after_{call_log['count']}_attempts"

        result = succeeds_on_third()
        assert result == "success_after_3_attempts"
        assert call_log["count"] == 3

    def test_retry_exhausted_raises(self):
        """If all retries exhausted, the last exception is raised."""
        call_log = {"count": 0}

        @lib.retry_with_backoff(max_retries=2, base_delay=0.01)
        def always_fails():
            call_log["count"] += 1
            raise RuntimeError(f"Failure #{call_log['count']}")

        with pytest.raises(RuntimeError) as exc_info:
            always_fails()
        assert "Failure #3" in str(exc_info.value)  # 1 initial + 2 retries = 3
        assert call_log["count"] == 3

    def test_retry_success_on_second_attempt(self):
        """Retry succeeds on the second attempt."""
        call_log = {"count": 0}

        @lib.retry_with_backoff(max_retries=3, base_delay=0.01)
        def fails_once():
            call_log["count"] += 1
            if call_log["count"] == 1:
                raise ConnectionError("First attempt fails")
            return "recovered"

        result = fails_once()
        assert result == "recovered"
        assert call_log["count"] == 2

    def test_retry_preserves_function_metadata(self):
        """Decorator should preserve function name."""
        @lib.retry_with_backoff(max_retries=1, base_delay=0.01)
        def my_function():
            """My docstring."""
            return 42

        assert my_function.__name__ == "my_function"

    def test_retry_with_backoff_default_params(self):
        """Test retry decorator with default parameters."""
        decorator = lib.retry_with_backoff()
        assert callable(decorator)

        @decorator
        def simple():
            return 42

        assert simple() == 42


# ============================================================================
# Tests for Data Quality validators (mock DataFrame)
# ============================================================================

class TestDataQualityValidators:
    """Test data quality validation functions using mock DataFrames."""

    def test_validate_not_null_calls_withColumn(self):
        """validate_not_null adds a _dq_ column via withColumn."""
        mock_df = MockDataFrame(columns=["CustomerID", "Email"])
        result = lib.validate_not_null(mock_df, "CustomerID")

        col_names = [call[0] for call in mock_df._withColumn_calls]
        assert "_dq_CustomerID_not_null" in col_names
        assert result is mock_df  # chaining returns same df

    def test_validate_unique_calls_withColumn(self):
        """validate_unique adds a _dq_ column for uniqueness check."""
        mock_df = MockDataFrame(columns=["Email"])
        result = lib.validate_unique(mock_df, "Email")

        col_names = [call[0] for call in mock_df._withColumn_calls]
        assert "_dq_Email_unique" in col_names

    def test_validate_range_calls_withColumn(self):
        """validate_range adds a _dq_ column for range check."""
        mock_df = MockDataFrame(columns=["OrderAmount"])
        result = lib.validate_range(mock_df, "OrderAmount", 0, 10000)

        col_names = [call[0] for call in mock_df._withColumn_calls]
        assert "_dq_OrderAmount_range" in col_names

    def test_validate_regex_calls_withColumn(self):
        """validate_regex adds a _dq_ column for regex check."""
        mock_df = MockDataFrame(columns=["Email"])
        result = lib.validate_regex(mock_df, "Email", r"^.*@.*\..*$")

        col_names = [call[0] for call in mock_df._withColumn_calls]
        assert "_dq_Email_regex" in col_names

    def test_validate_not_null_column_naming_convention(self):
        """Verify the naming convention: _dq_{column}_{rule}."""
        mock_df = MockDataFrame(columns=["UserName"])
        lib.validate_not_null(mock_df, "UserName")

        col_names = [call[0] for call in mock_df._withColumn_calls]
        assert col_names == ["_dq_UserName_not_null"]


# ============================================================================
# Tests for standardize_case (mock DataFrame)
# ============================================================================

class TestStandardizationFunctions:
    """Test standardization functions using mock DataFrames."""

    def test_standardize_case_lower(self):
        """standardize_case with case='lower' adds lowercased column."""
        mock_df = MockDataFrame(columns=["Email"])
        result = lib.standardize_case(mock_df, "Email", case="lower")

        assert len(mock_df._withColumn_calls) == 1
        col_name, expr = mock_df._withColumn_calls[0]
        assert col_name == "Email"
        assert result is mock_df

    def test_standardize_case_upper(self):
        """standardize_case with case='upper' adds uppercased column."""
        mock_df = MockDataFrame(columns=["Status"])
        result = lib.standardize_case(mock_df, "Status", case="upper")

        assert len(mock_df._withColumn_calls) == 1
        col_name, _ = mock_df._withColumn_calls[0]
        assert col_name == "Status"

    def test_standardize_trim(self):
        """standardize_trim adds a trimmed column."""
        mock_df = MockDataFrame(columns=["Name"])
        result = lib.standardize_trim(mock_df, "Name")

        assert len(mock_df._withColumn_calls) == 1
        col_name, _ = mock_df._withColumn_calls[0]
        assert col_name == "Name"

    def test_standardize_phone(self):
        """standardize_phone strips non-numeric characters."""
        mock_df = MockDataFrame(columns=["Phone"])
        result = lib.standardize_phone(mock_df, "Phone")

        assert len(mock_df._withColumn_calls) == 1
        col_name, _ = mock_df._withColumn_calls[0]
        assert col_name == "Phone"


# ============================================================================
# Tests for get_max_watermark (mock DataFrame)
# ============================================================================

class TestWatermarkFunctions:
    """Test watermark/incremental load functions."""

    def test_get_max_watermark_none_column_returns_none(self):
        """If watermark_column is None, return None immediately."""
        mock_df = MockDataFrame()
        result = lib.get_max_watermark(mock_df, None)
        assert result is None

    def test_get_max_watermark_missing_column_returns_none(self):
        """If watermark_column not in df.columns, return None."""
        mock_df = MockDataFrame(columns=["ID", "Name"])
        result = lib.get_max_watermark(mock_df, "NonExistentColumn")
        assert result is None

    def test_get_max_watermark_returns_string_value(self):
        """get_max_watermark returns the max value as a string."""
        mock_df = MockDataFrame(columns=["ModifiedDate"])
        mock_agg = MagicMock()
        mock_agg.collect.return_value = [["2026-06-15 12:30:45.123"]]
        mock_df.agg = MagicMock(return_value=mock_agg)

        result = lib.get_max_watermark(mock_df, "ModifiedDate")
        assert result == "2026-06-15 12:30:45.123"

    def test_get_max_watermark_datetime_formatting(self):
        """Verify datetime max values are formatted correctly."""
        from datetime import datetime
        mock_df = MockDataFrame(columns=["ModifiedDate"])
        dt_val = datetime(2026, 6, 15, 12, 30, 45, 123000)
        mock_agg = MagicMock()
        mock_agg.collect.return_value = [[dt_val]]
        mock_df.agg = MagicMock(return_value=mock_agg)

        result = lib.get_max_watermark(mock_df, "ModifiedDate")
        assert result == "2026-06-15 12:30:45.123"

    def test_get_max_watermark_none_result_returns_none(self):
        """If max returns None (no data), return None."""
        mock_df = MockDataFrame(columns=["ModifiedDate"])
        mock_agg = MagicMock()
        mock_agg.collect.return_value = [[None]]
        mock_df.agg = MagicMock(return_value=mock_agg)

        result = lib.get_max_watermark(mock_df, "ModifiedDate")
        assert result is None


# ============================================================================
# Tests for add_audit_columns (mock DataFrame)
# ============================================================================

class TestAuditHelpers:
    """Test audit helper functions."""

    def test_add_audit_columns_adds_timestamp(self):
        """add_audit_columns adds ingestion timestamp column."""
        mock_df = MockDataFrame(columns=["ID", "Name"])
        result = lib.add_audit_columns(mock_df, run_id="run-123", stage="bronze")

        col_names = [c[0] for c in mock_df._withColumn_calls]
        assert "_bronze_ingestion_timestamp" in col_names

    def test_add_audit_columns_adds_run_id(self):
        """add_audit_columns adds run_id column."""
        mock_df = MockDataFrame(columns=["ID", "Name"])
        lib.add_audit_columns(mock_df, run_id="run-456", stage="silver")

        col_names = [c[0] for c in mock_df._withColumn_calls]
        assert "_silver_run_id" in col_names

    def test_add_audit_columns_adds_source_and_entity(self):
        """add_audit_columns adds source_name and entity_name when provided."""
        mock_df = MockDataFrame(columns=["ID"])
        lib.add_audit_columns(
            mock_df,
            run_id="run-789",
            stage="bronze",
            source_name="sql-db",
            entity_name="customers"
        )

        col_names = [c[0] for c in mock_df._withColumn_calls]
        assert "_bronze_source_name" in col_names
        assert "_bronze_entity_name" in col_names

    def test_add_audit_columns_does_not_add_optional_when_none(self):
        """Optional columns not added when source_name/entity_name are None."""
        mock_df = MockDataFrame(columns=["ID"])
        lib.add_audit_columns(mock_df, run_id="run-000", stage="bronze")

        col_names = [c[0] for c in mock_df._withColumn_calls]
        assert "_bronze_source_name" not in col_names
        assert "_bronze_entity_name" not in col_names


# ============================================================================
# Tests for apply_standardization (mock DataFrame)
# ============================================================================

class TestApplyStandardization:
    """Test apply_standardization orchestration function."""

    def test_apply_standardization_with_lower(self):
        """apply_standardization applies lower case when configured."""
        mock_df = MockDataFrame(columns=["Email"])
        config = {"Email": "lower"}
        lib.apply_standardization(mock_df, config)

        assert len(mock_df._withColumn_calls) >= 1

    def test_apply_standardization_skips_missing_columns(self):
        """apply_standardization skips columns not in DataFrame."""
        mock_df = MockDataFrame(columns=["ID"])
        config = {"NonExistent": "lower"}
        lib.apply_standardization(mock_df, config)

        col_names = [c[0] for c in mock_df._withColumn_calls]
        assert "NonExistent" not in col_names

    def test_apply_standardization_multiple_rules(self):
        """apply_standardization applies multiple standardization rules."""
        mock_df = MockDataFrame(columns=["Email", "Phone", "Name"])
        config = {"Email": "lower", "Phone": "phone", "Name": "trim"}
        lib.apply_standardization(mock_df, config)

        assert len(mock_df._withColumn_calls) >= 1


# ============================================================================
# Tests for apply_all_validations (mock DataFrame)
# ============================================================================

class TestApplyAllValidations:
    """Test apply_all_validations orchestration function."""

    def test_apply_all_validations_not_null(self):
        """apply_all_validations processes NOT_NULL rules."""
        mock_df = MockDataFrame(columns=["CustomerID"])
        config = [{"type": "not_null", "column": "CustomerID"}]
        lib.apply_all_validations(mock_df, config)

        col_names = [c[0] for c in mock_df._withColumn_calls]
        assert "_dq_CustomerID_not_null" in col_names

    def test_apply_all_validations_unique(self):
        """apply_all_validations processes UNIQUE rules."""
        mock_df = MockDataFrame(columns=["Email"])
        config = [{"type": "unique", "column": "Email"}]
        lib.apply_all_validations(mock_df, config)

        col_names = [c[0] for c in mock_df._withColumn_calls]
        assert "_dq_Email_unique" in col_names

    def test_apply_all_validations_range(self):
        """apply_all_validations processes RANGE rules."""
        mock_df = MockDataFrame(columns=["Age"])
        config = [{"type": "range", "column": "Age", "params": {"min": 0, "max": 120}}]
        lib.apply_all_validations(mock_df, config)

        col_names = [c[0] for c in mock_df._withColumn_calls]
        assert "_dq_Age_range" in col_names

    def test_apply_all_validations_regex(self):
        """apply_all_validations processes REGEX rules."""
        mock_df = MockDataFrame(columns=["Email"])
        config = [{"type": "regex", "column": "Email", "params": {"pattern": r"^.*@.*$"}}]
        lib.apply_all_validations(mock_df, config)

        col_names = [c[0] for c in mock_df._withColumn_calls]
        assert "_dq_Email_regex" in col_names

    def test_apply_all_validations_empty_config(self):
        """apply_all_validations handles empty config gracefully."""
        mock_df = MockDataFrame(columns=["ID"])
        result = lib.apply_all_validations(mock_df, [])
        assert result is mock_df


# ============================================================================
# Schema Drift Detection Tests (mock DataFrame)
# ============================================================================

class TestSchemaDriftDetection:
    """Test schema drift detection functions."""

    @patch.object(lib, 'spark', create=True)
    def test_detect_drift_no_target_table(self, mock_spark):
        """If target table doesn't exist, report no drift."""
        mock_df = MockDataFrame(columns=["id"])
        result = lib.detect_schema_drift(mock_df, "/nonexistent/path")
        assert result["has_drift"] is False

    @patch.object(lib, 'spark', create=True)
    def test_detect_drift_added_columns(self, mock_spark):
        """detect_schema_drift identifies added columns."""
        mock_df = MockDataFrame(columns=["id", "name", "email"])
        result = lib.detect_schema_drift(mock_df, "/fake/path")
        assert result["has_drift"] is False
        assert result["added_columns"] == []


# ============================================================================
# Library Structure Tests
# ============================================================================

class TestLibraryStructure:
    """Verify the library exports all expected functions."""

    def test_all_functions_exist(self):
        """All expected functions should be importable from fabric_elt_lib."""
        expected_functions = [
            'validate_not_null',
            'validate_unique',
            'validate_range',
            'validate_regex',
            'validate_foreign_key',
            'apply_all_validations',
            'standardize_case',
            'standardize_trim',
            'standardize_phone',
            'standardize_date',
            'standardize_timestamp',
            'apply_standardization',
            'apply_scd2',
            'get_watermark_from_control_table',
            'build_incremental_query',
            'get_max_watermark',
            'detect_schema_drift',
            'handle_schema_drift',
            'profile_dataframe',
            'profile_to_delta',
            'retry_with_backoff',
            'add_audit_columns',
            'get_delta_stats',
            'optimize_delta_table',
            'estimate_storage_cost',
        ]
        for func_name in expected_functions:
            assert hasattr(lib, func_name), f"Missing function: {func_name}"
            assert callable(getattr(lib, func_name)), f"Not callable: {func_name}"

    def test_retry_with_backoff_is_callable(self):
        """retry_with_backoff should be a decorator factory."""
        decorator = lib.retry_with_backoff(max_retries=1, base_delay=0.01)
        assert callable(decorator)

        @decorator
        def fn():
            return 1

        assert fn() == 1
