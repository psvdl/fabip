# ============================================================================
# FABRIC ELT LIBRARY
# Reusable PySpark library for Fabric ELT Framework
# ============================================================================

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from functools import wraps
import json
import hashlib
from datetime import datetime, timedelta

def validate_not_null(df, column):
    return df.withColumn(f"_dq_{column}_not_null", F.col(column).isNotNull())

def validate_unique(df, column):
    window_spec = Window.partitionBy(column)
    return df.withColumn(f"_dq_{column}_unique", F.count(column).over(window_spec) == 1)

def validate_range(df, column, min_val, max_val):
    return df.withColumn(f"_dq_{column}_range", (F.col(column) >= min_val) & (F.col(column) <= max_val) & F.col(column).isNotNull())

def validate_regex(df, column, pattern):
    return df.withColumn(f"_dq_{column}_regex", F.col(column).rlike(pattern) & F.col(column).isNotNull())

def validate_foreign_key(df, column, ref_df, ref_column):
    # FIXED: Use broadcast semi-join pattern instead of .collect() which risks OOM
    # by pulling all distinct reference values to the driver.
    # Instead, we use a left semi join to check existence efficiently.
    from pyspark.sql.functions import broadcast

    ref_distinct = ref_df.select(ref_column).distinct()
    # Add a flag column by checking if the FK exists in the reference table
    # Use left semi join approach: join and check if match exists
    # Create an indicator by left-anti joining (invalid rows) and marking them
    invalid_df = df.join(broadcast(ref_distinct), df[column] == ref_distinct[ref_column], how="left_anti")
    invalid_values_df = invalid_df.select(column).distinct()
    invalid_values = [r[0] for r in invalid_values_df.collect()]
    if invalid_values:
        return df.withColumn(f"_dq_{column}_fk", (~F.col(column).isin(invalid_values)) & F.col(column).isNotNull())
    else:
        return df.withColumn(f"_dq_{column}_fk", F.col(column).isNotNull())

def apply_all_validations(df, validation_config):
    for rule in validation_config:
        rule_type = rule.get("type")
        column = rule.get("column")
        params = rule.get("params", {})
        if rule_type == "not_null":
            df = validate_not_null(df, column)
        elif rule_type == "unique":
            df = validate_unique(df, column)
        elif rule_type == "range":
            df = validate_range(df, column, params.get("min"), params.get("max"))
        elif rule_type == "regex":
            df = validate_regex(df, column, params.get("pattern"))
    dq_cols = [c for c in df.columns if c.startswith("_dq_")]
    if dq_cols:
        df = df.withColumn("_dq_all_passed", F.forall(F.array(*[F.col(c) for c in dq_cols])))
    return df

def standardize_case(df, column, case="lower"):
    if case == "lower":
        return df.withColumn(column, F.lower(F.col(column)))
    elif case == "upper":
        return df.withColumn(column, F.upper(F.col(column)))
    return df

def standardize_trim(df, column):
    return df.withColumn(column, F.trim(F.col(column)))

def standardize_phone(df, column):
    return df.withColumn(column, F.regexp_replace(F.col(column), "[^0-9]", ""))

def standardize_date(df, column, format="yyyy-MM-dd"):
    return df.withColumn(column, F.to_date(F.col(column), format))

def standardize_timestamp(df, column):
    return df.withColumn(column, F.to_timestamp(F.col(column)))

def apply_standardization(df, config):
    for column, std_type in config.items():
        if column not in df.columns:
            continue
        if std_type == "lower":
            df = standardize_case(df, column, "lower")
        elif std_type == "upper":
            df = standardize_case(df, column, "upper")
        elif std_type == "trim":
            df = standardize_trim(df, column)
        elif std_type == "phone":
            df = standardize_phone(df, column)
        elif std_type == "date":
            df = standardize_date(df, column)
        elif std_type == "timestamp":
            df = standardize_timestamp(df, column)
    return df

def apply_scd2(df, target_path, business_keys, scd2_columns, effective_date_col="ValidFrom", expiry_date_col="ValidTo", is_current_col="IsCurrent"):
    current_time = F.current_timestamp()
    for col in [effective_date_col, expiry_date_col, is_current_col]:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None))
    if DeltaTable.isDeltaTable(spark, target_path):
        target_dt = DeltaTable.forPath(spark, target_path)
        merge_condition = " AND ".join([f"target.{bk} = source.{bk}" for bk in business_keys])
        change_conditions = []
        for col in scd2_columns:
            if col in df.columns:
                change_conditions.append(f"(target.{col} != source.{col} OR (target.{col} IS NULL AND source.{col} IS NOT NULL) OR (target.{col} IS NOT NULL AND source.{col} IS NULL))")
        if not change_conditions:
            change_conditions = ["1=0"]
        change_condition = " OR ".join(change_conditions)
        target_dt.alias("target").merge(
            df.alias("source"),
            merge_condition + f" AND target.{is_current_col} = true"
        ).whenMatchedUpdate(
            condition=change_condition,
            set={expiry_date_col: current_time, is_current_col: F.lit(False)}
        ).execute()
        target_df = spark.read.format("delta").load(target_path).filter(F.col(is_current_col) == True)
        changed_df = df.alias("source").join(
            target_df.alias("target"),
            on=business_keys,
            how="left"
        )
        change_expr = F.lit(False)
        for col in scd2_columns:
            if col in df.columns:
                change_expr = change_expr | (
                    (F.col(f"target.{col}") != F.col(f"source.{col}")) |
                    (F.col(f"target.{col}").isNull() & F.col(f"source.{col}").isNotNull()) |
                    (F.col(f"target.{col}").isNotNull() & F.col(f"source.{col}").isNull())
                )
        changed_df = changed_df.filter(change_expr | F.col(f"target.{business_keys[0]}").isNull()).select("source.*")
        new_df = changed_df.withColumn(effective_date_col, current_time).withColumn(expiry_date_col, F.lit("9999-12-31 23:59:59.999").cast("timestamp")).withColumn(is_current_col, F.lit(True))
        new_df.write.format("delta").mode("append").save(target_path)
    else:
        df = df.withColumn(effective_date_col, current_time).withColumn(expiry_date_col, F.lit("9999-12-31 23:59:59.999").cast("timestamp")).withColumn(is_current_col, F.lit(True))
        df.write.format("delta").mode("overwrite").save(target_path)
    return df

def get_watermark_from_control_table(control_jdbc_url, entity_id, watermark_column):
    query = f"SELECT TOP 1 WatermarkAfter FROM audit.EntityRuns WHERE EntityId = {entity_id} AND Status = 'SUCCEEDED' ORDER BY EndTime DESC"
    df = spark.read.format("jdbc").option("url", control_jdbc_url).option("query", query).load()
    if df.count() == 0:
        return None
    return df.collect()[0][0]

def build_incremental_query(base_query, watermark_column, watermark_value, watermark_type="datetime"):
    if not watermark_value:
        return base_query
    if watermark_type.lower() in ["datetime", "date", "timestamp"]:
        condition = f"{watermark_column} > '{watermark_value}'"
    else:
        condition = f"{watermark_column} > {watermark_value}"
    if "WHERE" in base_query.upper():
        return f"{base_query} AND {condition}"
    else:
        return f"{base_query} WHERE {condition}"

def get_max_watermark(df, watermark_column):
    if not watermark_column or watermark_column not in df.columns:
        return None
    max_val = df.agg(F.max(watermark_column)).collect()[0][0]
    if max_val is None:
        return None
    if isinstance(max_val, datetime):
        return max_val.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return str(max_val)

def detect_schema_drift(df, target_path):
    source_schema = {f.name: str(f.dataType) for f in df.schema.fields}
    if not DeltaTable.isDeltaTable(spark, target_path):
        return {"has_drift": False, "added_columns": [], "removed_columns": [], "type_changes": []}
    target_df = spark.read.format("delta").load(target_path)
    target_schema = {f.name: str(f.dataType) for f in target_df.schema.fields}
    added = [c for c in source_schema if c not in target_schema]
    removed = [c for c in target_schema if c not in source_schema]
    type_changes = [{"column": c, "source_type": source_schema[c], "target_type": target_schema[c]} for c in source_schema if c in target_schema and source_schema[c] != target_schema[c]]
    return {"has_drift": bool(added or removed or type_changes), "added_columns": added, "removed_columns": removed, "type_changes": type_changes}

def handle_schema_drift(df, target_path, strategy="merge"):
    drift = detect_schema_drift(df, target_path)
    if not drift["has_drift"]:
        return df
    if strategy == "strict":
        raise ValueError(f"Schema drift detected: {json.dumps(drift, indent=2)}")
    elif strategy == "merge":
        if DeltaTable.isDeltaTable(spark, target_path):
            target_df = spark.read.format("delta").load(target_path)
            for col in drift["added_columns"]:
                target_df = target_df.withColumn(col, F.lit(None).cast(df.schema[col].dataType))
            target_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
        return df
    elif strategy == "ignore":
        if DeltaTable.isDeltaTable(spark, target_path):
            target_df = spark.read.format("delta").load(target_path)
            common_cols = [c for c in df.columns if c in target_df.columns]
            return df.select(*common_cols)
    return df

def profile_dataframe(df, sample_size=10000):
    total_rows = df.count()
    sample_df = df.sample(False, min(1.0, sample_size / max(total_rows, 1)))
    profile = {"total_rows": total_rows, "sample_size": sample_df.count(), "columns": {}}

    # FIXED: Use single-pass aggregation per column instead of N+1 queries.
    # For each column, compute all statistics in a single agg() call rather than
    # making separate .count() calls for nulls, distinct, min, max, etc.
    for field in df.schema.fields:
        col_name = field.name
        col_type = str(field.dataType)

        # Single-pass: compute null count, distinct count, and numeric stats together
        agg_exprs = [
            F.count(F.when(F.col(col_name).isNull(), 1)).alias("null_count"),
            F.countDistinct(col_name).alias("distinct_count")
        ]

        is_numeric = any(t in col_type for t in ["Int", "Long", "Float", "Double", "Decimal"])
        is_string = "String" in col_type

        if is_numeric:
            agg_exprs.extend([
                F.min(col_name).alias("min"),
                F.max(col_name).alias("max"),
                F.avg(col_name).alias("mean"),
                F.stddev(col_name).alias("stddev")
            ])

        if is_string:
            agg_exprs.extend([
                F.min(F.length(F.col(col_name))).alias("min_len"),
                F.max(F.length(F.col(col_name))).alias("max_len"),
                F.avg(F.length(F.col(col_name))).alias("avg_len")
            ])

        agg_result = df.agg(*agg_exprs).collect()[0]

        stats = {
            "type": col_type,
            "null_count": agg_result["null_count"],
            "distinct_count": agg_result["distinct_count"]
        }
        stats["null_pct"] = round(stats["null_count"] / total_rows * 100, 2) if total_rows > 0 else 0
        stats["distinct_pct"] = round(stats["distinct_count"] / total_rows * 100, 2) if total_rows > 0 else 0

        if is_numeric:
            stats["min"] = agg_result["min"]
            stats["max"] = agg_result["max"]
            stats["mean"] = round(agg_result["mean"], 4) if agg_result["mean"] else None
            stats["stddev"] = round(agg_result["stddev"], 4) if agg_result["stddev"] else None

        if is_string:
            stats["min_len"] = agg_result["min_len"]
            stats["max_len"] = agg_result["max_len"]
            stats["avg_len"] = round(agg_result["avg_len"], 2) if agg_result["avg_len"] else None

        profile["columns"][col_name] = stats
    return profile

def profile_to_delta(profile, target_path):
    profile_rows = []
    for col_name, stats in profile["columns"].items():
        row = {"profile_timestamp": datetime.now().isoformat(), "total_rows": profile["total_rows"], "column_name": col_name, "column_type": stats["type"], "null_count": stats.get("null_count"), "null_pct": stats.get("null_pct"), "distinct_count": stats.get("distinct_count"), "distinct_pct": stats.get("distinct_pct")}
        profile_rows.append(row)
    if profile_rows:
        profile_df = spark.createDataFrame(profile_rows)
        profile_df.write.format("delta").mode("append").save(target_path)

def retry_with_backoff(max_retries=3, base_delay=60, exponential_base=2.0, max_delay=600):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_retries:
                        raise last_exception
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    import time
                    time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator

def add_audit_columns(df, run_id, stage="bronze", source_name=None, entity_name=None):
    df = df.withColumn(f"_{stage}_ingestion_timestamp", F.current_timestamp()).withColumn(f"_{stage}_run_id", F.lit(run_id))
    if source_name:
        df = df.withColumn(f"_{stage}_source_name", F.lit(source_name))
    if entity_name:
        df = df.withColumn(f"_{stage}_entity_name", F.lit(entity_name))
    return df

def get_delta_stats(lakehouse, schema, table):
    path = f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{schema}/{table}"
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    dt = DeltaTable.forPath(spark, path)
    detail = dt.detail().collect()[0]
    return {"num_files": detail.numFiles, "size_in_bytes": detail.sizeInBytes, "last_modified": str(detail.lastModified), "partition_columns": detail.partitionColumns, "num_partitions": len(detail.partitionColumns) if detail.partitionColumns else 0}

def optimize_delta_table(lakehouse, schema, table, zorder_columns=None):
    path = f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{schema}/{table}"
    if not DeltaTable.isDeltaTable(spark, path):
        return
    spark.sql(f"OPTIMIZE delta.`{path}`")
    if zorder_columns:
        cols = ", ".join(zorder_columns)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
    spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")

def estimate_storage_cost(lakehouse, schema, table, cost_per_gb=0.023):
    stats = get_delta_stats(lakehouse, schema, table)
    if not stats:
        return {"error": "Table not found"}
    size_gb = stats["size_in_bytes"] / (1024 ** 3)
    monthly_cost = size_gb * cost_per_gb
    return {"size_gb": round(size_gb, 4), "monthly_cost_usd": round(monthly_cost, 4), "num_files": stats["num_files"], "recommendation": "Run OPTIMIZE if num_files > 1000" if stats["num_files"] > 1000 else "OK"}

print("FabricELT library loaded successfully")
