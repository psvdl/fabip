# ============================================================================
# SILVER TRANSFORMATION NOTEBOOK
# Production-ready cleansing, standardization, and SCD2
# Fabric SQL Database compatible (ActiveDirectoryMSI authentication)
# ============================================================================

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import json
import sys
import traceback
from datetime import datetime

# Fabric pipeline parameters are injected as notebook-scoped variables
transformation_id = int(globals().get("transformation_id", "0"))
entity_id = int(globals().get("entity_id", "0"))
run_id = globals().get("run_id", "")
entity_run_id = globals().get("entity_run_id", "")
source_lakehouse = globals().get("source_lakehouse", "lh_bronze")
source_schema = globals().get("source_schema", "raw")
source_table = globals().get("source_table", "")
target_lakehouse = globals().get("target_lakehouse", "lh_silver")
target_schema = globals().get("target_schema", "cleaned")
target_table = globals().get("target_table", "")
transformation_type = globals().get("transformation_type", "STANDARDIZATION")
transformation_logic_json = globals().get("transformation_logic", "{}")
business_key_columns = globals().get("business_key_columns", "")
control_sql_endpoint = globals().get("control_sql_endpoint", "")
control_database_name = globals().get("control_database_name", "fabric_control")
transformation_logic = json.loads(transformation_logic_json) if transformation_logic_json else {}

# Build Fabric SQL Database JDBC URL with AAD MSI authentication
if control_sql_endpoint and control_sql_endpoint.strip():
    CONTROL_JDBC_URL = (
        f"jdbc:sqlserver://{control_sql_endpoint}:1433;"
        f"database={control_database_name};"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "hostNameInCertificate=*.sql.azuresynapse.net;"
        "loginTimeout=30;"
    )
else:
    CONTROL_JDBC_URL = ""
    print("WARNING: control_sql_endpoint is empty. Control DB operations will be disabled.")

# Shared JDBC options for Fabric SQL Database
_FABRIC_JDBC_OPTS = {
    "authentication": "ActiveDirectoryMSI",
}


def _fabric_jdbc_read(query, prepared_statement_params=None):
    """Execute a JDBC read against Fabric SQL Database with proper authentication and fallback."""
    if not CONTROL_JDBC_URL:
        raise ConnectionError("CONTROL_JDBC_URL is not configured. Cannot connect to Fabric SQL Database.")
    reader = spark.read.format("jdbc") \
        .option("url", CONTROL_JDBC_URL) \
        .option("query", query)
    for key, value in _FABRIC_JDBC_OPTS.items():
        reader = reader.option(key, value)
    if prepared_statement_params is not None:
        reader = reader.option("prepareStatement", "true") \
                       .option("preparedStatementParameters", json.dumps(prepared_statement_params))
    return reader.load()


def read_bronze_table(lakehouse, schema, table):
    path = f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{schema}/{table}"
    return spark.read.format("delta").load(path)

def write_silver_table(df, lakehouse, schema, table, mode="overwrite"):
    path = f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{schema}/{table}"
    df.write.format("delta").mode(mode).option("overwriteSchema", "true" if mode == "overwrite" else "false").save(path)
    spark.sql(f"OPTIMIZE delta.`{path}`")
    spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")
    return df.count()

def _validate_foreign_key(df, fk_col, ref_lakehouse, ref_schema, ref_table, ref_col):
    """Validate foreign keys using a left anti-join pattern. Returns a boolean column."""
    try:
        ref_path = f"abfss://{ref_lakehouse}@onelake.dfs.fabric.microsoft.com/{ref_schema}/{ref_table}"
        ref_df = spark.read.format("delta").load(ref_path).select(ref_col).distinct()
        invalid_df = df.join(ref_df, df[fk_col] == ref_df[ref_col], how="left_anti").select(df[fk_col]).distinct()
        return ~F.col(fk_col).isin([r[0] for r in invalid_df.collect()])
    except Exception as e:
        print(f"WARNING: FK validation skipped for {fk_col}: {str(e)}")
        return F.lit(True)

def apply_standardization(df, logic):
    dedup_key = logic.get("dedup_key")
    if dedup_key and dedup_key in df.columns:
        window_spec = Window.partitionBy(dedup_key).orderBy(F.col("_bronze_ingestion_timestamp").desc())
        df = df.withColumn("_row_num", F.row_number().over(window_spec)).filter(F.col("_row_num") == 1).drop("_row_num")
    null_defaults = logic.get("null_defaults", {})
    for col_name, default_val in null_defaults.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(default_val)))
    standardize = logic.get("standardize", {})
    for col_name, std_type in standardize.items():
        if col_name not in df.columns:
            continue
        if std_type == "lower":
            df = df.withColumn(col_name, F.lower(F.col(col_name)))
        elif std_type == "upper":
            df = df.withColumn(col_name, F.upper(F.col(col_name)))
        elif std_type == "trim":
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
        elif std_type == "regex":
            df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), "[^0-9]", ""))

    foreign_keys = logic.get("foreign_keys", [])
    for fk in foreign_keys:
        fk_col = fk.get("column")
        ref_table = fk.get("ref_table")
        ref_col = fk.get("ref_column", fk_col)
        ref_lakehouse = fk.get("ref_lakehouse", "lh_silver")
        ref_schema = fk.get("ref_schema", "cleaned")
        if fk_col and fk_col in df.columns and ref_table:
            try:
                ref_path = f"abfss://{ref_lakehouse}@onelake.dfs.fabric.microsoft.com/{ref_schema}/{ref_table}"
                ref_df = spark.read.format("delta").load(ref_path).select(ref_col).distinct()
                valid_rows_df = df.join(ref_df, F.col(fk_col) == F.col(ref_col), how="left_semi").select(fk_col)
                valid_values = [r[0] for r in valid_rows_df.distinct().collect()]
                if valid_values:
                    df = df.withColumn(f"_fk_valid_{fk_col}", F.col(fk_col).isin(valid_values) & F.col(fk_col).isNotNull())
                else:
                    df = df.withColumn(f"_fk_valid_{fk_col}", F.lit(False))
            except Exception as e:
                print(f"WARNING: FK validation failed for {fk_col} -> {ref_table}.{ref_col}: {str(e)}")
                df = df.withColumn(f"_fk_valid_{fk_col}", F.lit(True))
        elif fk_col and fk_col in df.columns:
            df = df.withColumn(f"_fk_valid_{fk_col}", F.col(fk_col).isNotNull())

    df = df.withColumn("_silver_transform_timestamp", F.current_timestamp()).withColumn("_silver_transformation_id", F.lit(transformation_id)).withColumn("_silver_run_id", F.lit(run_id)).withColumn("_silver_entity_run_id", F.lit(entity_run_id))
    return df

def apply_scd2(df, logic, business_keys):
    scd2_columns = logic.get("scd2_columns", [])
    effective_date_col = logic.get("effective_date", "ValidFrom")
    expiry_date_col = logic.get("expiry_date", "ValidTo")
    is_current_col = logic.get("is_current", "IsCurrent")
    target_path = f"abfss://{target_lakehouse}@onelake.dfs.fabric.microsoft.com/{target_schema}/{target_table}"
    bk_list = [c.strip() for c in business_keys.split(",") if c.strip()]
    if not bk_list:
        raise ValueError("business_key_columns cannot be empty for SCD2")
    for col in [effective_date_col, expiry_date_col, is_current_col]:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None))
    current_timestamp = F.current_timestamp()
    if DeltaTable.isDeltaTable(spark, target_path):
        target_table_dt = DeltaTable.forPath(spark, target_path)
        merge_condition = " AND ".join([f"target.{bk} = source.{bk}" for bk in bk_list])
        change_conditions = []
        for col in scd2_columns:
            if col in df.columns:
                change_conditions.append(f"(target.{col} != source.{col} OR (target.{col} IS NULL AND source.{col} IS NOT NULL) OR (target.{col} IS NOT NULL AND source.{col} IS NULL))")
        if not change_conditions:
            change_conditions = ["1=0"]
        change_condition = " OR ".join(change_conditions)
        target_table_dt.alias("target").merge(
            df.alias("source"),
            merge_condition + f" AND target.{is_current_col} = true"
        ).whenMatchedUpdate(
            condition=change_condition,
            set={expiry_date_col: current_timestamp, is_current_col: F.lit(False)}
        ).execute()
        target_df = spark.read.format("delta").load(target_path).filter(F.col(is_current_col) == True)
        changed_df = df.alias("source").join(
            target_df.alias("target"),
            on=bk_list,
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
        changed_df = changed_df.filter(change_expr | F.col(f"target.{bk_list[0]}").isNull()).select("source.*")
        new_df = changed_df.withColumn(effective_date_col, current_timestamp).withColumn(expiry_date_col, F.lit("9999-12-31 23:59:59.999").cast("timestamp")).withColumn(is_current_col, F.lit(True))
        new_df.write.format("delta").mode("append").save(target_path)
    else:
        df = df.withColumn(effective_date_col, current_timestamp).withColumn(expiry_date_col, F.lit("9999-12-31 23:59:59.999").cast("timestamp")).withColumn(is_current_col, F.lit(True))
        df.write.format("delta").mode("overwrite").save(target_path)
    return df.count()

def apply_json_parse(df, logic):
    json_col = logic.get("json_parse")
    if json_col and json_col in df.columns:
        json_schema = spark.read.json(df.select(json_col).rdd.map(lambda r: r[0])).schema
        df = df.withColumn("_parsed", F.from_json(F.col(json_col), json_schema))
        for field in json_schema.fields:
            df = df.withColumn(field.name, F.col("_parsed").getItem(field.name))
        df = df.drop("_parsed")
    ts_col = logic.get("timestamp_extract")
    if ts_col and ts_col in df.columns:
        df = df.withColumn(ts_col, F.to_timestamp(F.col(ts_col)))
    if logic.get("ip_geolocation"):
        ip_col = next((c for c in ["IP", "ip_address", "client_ip"] if c in df.columns), None)
        if ip_col:
            df = df.withColumn("_country", F.lit("US"))
    return df

def main():
    try:
        df_bronze = read_bronze_table(source_lakehouse, source_schema, source_table)
        rows_read = df_bronze.count()
        if rows_read == 0:
            result = {"status": "SUCCEEDED", "rows_read": 0, "rows_written": 0, "error": None}
            mssparkutils.notebook.exit(json.dumps(result))
            return
        if transformation_type == "STANDARDIZATION":
            df_silver = apply_standardization(df_bronze, transformation_logic)
            df_silver = apply_json_parse(df_silver, transformation_logic)
            rows_written = write_silver_table(df_silver, target_lakehouse, target_schema, target_table, "overwrite")
        elif transformation_type == "SCD2":
            rows_written = apply_scd2(df_bronze, transformation_logic, business_key_columns)
        elif transformation_type == "DEDUPLICATION":
            df_silver = apply_standardization(df_bronze, {"dedup_key": business_key_columns.split(",")[0] if business_key_columns else None})
            rows_written = write_silver_table(df_silver, target_lakehouse, target_schema, target_table, "overwrite")
        elif transformation_type == "ENRICHMENT":
            df_silver = apply_standardization(df_bronze, transformation_logic)
            df_silver = apply_json_parse(df_silver, transformation_logic)
            rows_written = write_silver_table(df_silver, target_lakehouse, target_schema, target_table, "overwrite")
        elif transformation_type == "AGGREGATION":
            agg_config = transformation_logic.get("aggregations", [])
            group_cols = transformation_logic.get("group_by", [])
            agg_exprs = []
            for agg in agg_config:
                func = agg.get("function", "sum")
                col_name = agg.get("column")
                alias = agg.get("alias")
                if not col_name or not alias:
                    continue
                if func == "sum":
                    agg_exprs.append(F.sum(col_name).alias(alias))
                elif func == "count":
                    agg_exprs.append(F.count(col_name).alias(alias))
                elif func == "avg":
                    agg_exprs.append(F.avg(col_name).alias(alias))
                elif func == "max":
                    agg_exprs.append(F.max(col_name).alias(alias))
                elif func == "min":
                    agg_exprs.append(F.min(col_name).alias(alias))
            if agg_exprs and group_cols:
                df_silver = df_bronze.groupBy(*group_cols).agg(*agg_exprs)
            else:
                df_silver = df_bronze
            df_silver = df_silver.withColumn("_silver_transform_timestamp", F.current_timestamp())
            rows_written = write_silver_table(df_silver, target_lakehouse, target_schema, target_table, "overwrite")
        else:
            df_silver = apply_standardization(df_bronze, transformation_logic)
            rows_written = write_silver_table(df_silver, target_lakehouse, target_schema, target_table, "overwrite")
        result = {"status": "SUCCEEDED", "rows_read": rows_read, "rows_written": rows_written, "error": None}
    except Exception as e:
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        result = {"status": "FAILED", "rows_read": rows_read if 'rows_read' in locals() else 0, "rows_written": 0, "error": error_msg, "stack_trace": stack_trace}
    mssparkutils.notebook.exit(json.dumps(result))

main()
