# ============================================================================
# FABRIC ELT LIBRARY - Python Package
# Reusable PySpark library for Fabric ELT Framework
# ============================================================================

from .fabric_elt_lib import (
    # Fabric SQL Database connection helpers
    build_fabric_jdbc_url,

    # Data quality validation functions
    validate_not_null,
    validate_unique,
    validate_range,
    validate_regex,
    validate_foreign_key,
    apply_all_validations,

    # Standardization functions
    standardize_case,
    standardize_trim,
    standardize_phone,
    standardize_date,
    standardize_timestamp,
    apply_standardization,

    # SCD Type 2
    apply_scd2,

    # Incremental / Watermark functions (Fabric SQL Database)
    get_watermark_from_control_table,
    build_incremental_query,
    get_max_watermark,

    # Schema drift detection & handling
    detect_schema_drift,
    handle_schema_drift,

    # Data profiling
    profile_dataframe,
    profile_to_delta,

    # Retry & audit utilities
    retry_with_backoff,
    add_audit_columns,

    # Delta Lake maintenance
    get_delta_stats,
    optimize_delta_table,
    estimate_storage_cost,
)

__version__ = "1.1.0"
__all__ = [
    # Fabric SQL Database connection helpers
    "build_fabric_jdbc_url",

    # Data quality validation functions
    "validate_not_null",
    "validate_unique",
    "validate_range",
    "validate_regex",
    "validate_foreign_key",
    "apply_all_validations",

    # Standardization functions
    "standardize_case",
    "standardize_trim",
    "standardize_phone",
    "standardize_date",
    "standardize_timestamp",
    "apply_standardization",

    # SCD Type 2
    "apply_scd2",

    # Incremental / Watermark functions (Fabric SQL Database)
    "get_watermark_from_control_table",
    "build_incremental_query",
    "get_max_watermark",

    # Schema drift detection & handling
    "detect_schema_drift",
    "handle_schema_drift",

    # Data profiling
    "profile_dataframe",
    "profile_to_delta",

    # Retry & audit utilities
    "retry_with_backoff",
    "add_audit_columns",

    # Delta Lake maintenance
    "get_delta_stats",
    "optimize_delta_table",
    "estimate_storage_cost",
]
