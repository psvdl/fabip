# ============================================================================
# GOLD CURATION NOTEBOOK
# Production-ready dimension, fact, and aggregate model builder
# Fabric SQL Database compatible (ActiveDirectoryMSI authentication)
# ============================================================================

import pyspark.sql.functions as F
from delta.tables import DeltaTable
import json
import traceback
import re
from datetime import datetime

# Fabric pipeline parameters are injected as notebook-scoped variables
model_name = globals().get("model_name", "all")
target_warehouse = globals().get("target_warehouse", "wh_gold")
target_schema = globals().get("target_schema", "curated")
run_id = globals().get("run_id", "")
control_sql_endpoint = globals().get("control_sql_endpoint", "")
control_database_name = globals().get("control_database_name", "fabric_control")
key_vault_url = globals().get("key_vault_url", "")

# Build Fabric SQL Database JDBC URL
if control_sql_endpoint and control_sql_endpoint.strip():
    CONTROL_JDBC_URL = (
        f"jdbc:sqlserver://{control_sql_endpoint}:1433;"
        f"database={control_database_name};"
        f"encrypt=true;"
        f"trustServerCertificate=false;"
        f"hostNameInCertificate=*.sql.azuresynapse.net;"
        f"loginTimeout=30;"
    )
else:
    CONTROL_JDBC_URL = ""
    print("WARNING: control_sql_endpoint is empty. Control DB operations will be skipped.")

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


def _fabric_jdbc_fallback_query(query):
    """Execute a JDBC read using the fallback direct-query method with ActiveDirectoryMSI auth."""
    return spark.read.format("jdbc") \
        .option("url", CONTROL_JDBC_URL) \
        .option("query", query) \
        .option("authentication", "ActiveDirectoryMSI") \
        .load()


def get_gold_models_from_control_db():
    query = "SELECT ModelId, ModelName, ModelType, TargetWarehouse, TargetSchema, TargetTableName, SourceLakehouse, SourceSchema, SourceTables, GrainColumns, CalculatedColumns, Priority FROM cfg.GoldModels WHERE IsActive = 1"
    try:
        if model_name != "all":
            query += " AND ModelName = ?"
            df = _fabric_jdbc_read(query, [model_name])
        else:
            df = _fabric_jdbc_read(query)
        return df.collect()
    except Exception as e:
        print(f"WARNING: Failed to load gold models from control DB: {str(e)}")
        return []

def log_gold_model_start(model_id, model_name, model_type, source_tables):
    try:
        if not CONTROL_JDBC_URL:
            print("WARNING: CONTROL_JDBC_URL not configured. Skipping audit logging.")
            return str(datetime.now().timestamp())
        source_tables_json = json.dumps(source_tables) if isinstance(source_tables, list) else str(source_tables)
        query = f"EXEC audit.usp_LogGoldModelStart @RunId='{run_id}', @ModelId={model_id}, @ModelName='{model_name}', @ModelType='{model_type}', @SourceTables='{source_tables_json}'"
        result_df = _fabric_jdbc_fallback_query(query)
        if result_df.count() > 0:
            return str(result_df.collect()[0][0])
        return str(datetime.now().timestamp())
    except Exception as e:
        print(f"LOGGING WARNING (start): {str(e)}")
        return str(datetime.now().timestamp())

def log_gold_model_end(gold_run_id, status, rows_read=0, rows_written=0, error_message=None, cu_consumed=None, spark_app_id=None):
    try:
        if not CONTROL_JDBC_URL:
            print("WARNING: CONTROL_JDBC_URL not configured. Skipping audit logging.")
            return
        error_msg = error_message.replace("'", "''") if error_message else None
        error_param = f"'{error_msg}'" if error_msg else "NULL"
        cu_param = str(cu_consumed) if cu_consumed else "NULL"
        spark_param = f"'{spark_app_id}'" if spark_app_id else "NULL"
        query = f"EXEC audit.usp_LogGoldModelEnd @GoldRunId='{gold_run_id}', @Status='{status}', @ErrorMessage={error_param}, @RowsRead={rows_read}, @RowsWritten={rows_written}, @CUConsumed={cu_param}, @SparkApplicationId={spark_param}"
        _fabric_jdbc_fallback_query(query)
    except Exception as e:
        print(f"LOGGING WARNING (end): {str(e)}")

def read_silver_table(lakehouse, schema, table):
    path = f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{schema}/{table}"
    return spark.read.format("delta").load(path)

def write_gold_table(df, warehouse, schema, table, mode="overwrite"):
    lakehouse_path = f"abfss://{warehouse}@onelake.dfs.fabric.microsoft.com/{schema}/{table}"
    df.write.format("delta").mode(mode).option("overwriteSchema", "true" if mode == "overwrite" else "false").save(lakehouse_path)
    spark.sql(f"OPTIMIZE delta.`{lakehouse_path}`")
    spark.sql(f"VACUUM delta.`{lakehouse_path}` RETAIN 168 HOURS")
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {warehouse}.{schema}.{table} USING DELTA LOCATION '{lakehouse_path}'")
    except Exception as e:
        print(f"Warehouse registration warning: {str(e)}")
    return df.count()

def build_dimension_model(model_config):
    source_tables = json.loads(model_config.SourceTables)
    primary_table = source_tables[0]
    df = read_silver_table(model_config.SourceLakehouse, model_config.SourceSchema, primary_table)
    if "IsCurrent" in df.columns:
        df = df.filter(F.col("IsCurrent") == True)
    df = df.withColumn("_gold_load_timestamp", F.current_timestamp()).withColumn("_gold_run_id", F.lit(run_id)).withColumn("_gold_model_name", F.lit(model_config.ModelName))
    return df

def build_fact_model(model_config):
    source_tables = json.loads(model_config.SourceTables)
    primary_table = source_tables[0]
    df_fact = read_silver_table(model_config.SourceLakehouse, model_config.SourceSchema, primary_table)

    join_configs = getattr(model_config, 'JoinConfigs', None)
    if join_configs and isinstance(join_configs, str):
        try:
            join_configs = json.loads(join_configs)
        except (json.JSONDecodeError, TypeError):
            join_configs = {}
    if not join_configs:
        join_configs = {}

    for dim_table in source_tables[1:]:
        df_dim = read_silver_table(model_config.SourceLakehouse, model_config.SourceSchema, dim_table)
        join_key = None
        join_type = "left"
        if isinstance(join_configs, dict) and dim_table in join_configs:
            join_key = join_configs[dim_table].get("column")
            join_type = join_configs[dim_table].get("type", "left")
        if not join_key:
            for col in df_dim.columns:
                if col in df_fact.columns and col not in ["_silver_transform_timestamp", "_silver_transformation_id", "_silver_run_id", "_silver_entity_run_id"]:
                    join_key = col
                    break
        if join_key:
            if "IsCurrent" in df_dim.columns:
                df_dim = df_dim.filter(F.col("IsCurrent") == True)
            if join_type == "inner":
                df_fact = df_fact.join(df_dim, on=join_key, how="inner")
            else:
                df_fact = df_fact.join(df_dim, on=join_key, how="left")
    if model_config.CalculatedColumns:
        try:
            calc_cols = json.loads(model_config.CalculatedColumns)
            for col_name, expr in calc_cols.items():
                expr = expr.strip()
                if re.match(r'^[\w\s]+\*[\w\s]+\-[\w\s]+$', expr):
                    parts = [p.strip() for p in expr.replace("*", "|").replace("-", "|").split("|") if p.strip()]
                    if len(parts) == 3:
                        df_fact = df_fact.withColumn(col_name, F.col(parts[0]) * F.col(parts[1]) - F.col(parts[2]))
                elif re.match(r'^[\w\s]+\*[\w\s]+$', expr):
                    parts = [p.strip() for p in expr.split("*") if p.strip()]
                    if len(parts) == 2:
                        df_fact = df_fact.withColumn(col_name, F.col(parts[0]) * F.col(parts[1]))
                elif expr.upper().startswith("SUM("):
                    pass
                else:
                    df_fact = df_fact.withColumn(col_name, F.lit(expr))
        except Exception as e:
            print(f"WARNING: Failed to parse calculated columns: {str(e)}")
    date_cols = [c for c in df_fact.columns if 'Date' in c or 'Timestamp' in c or c.endswith('Date')]
    for date_col in date_cols:
        try:
            df_fact = df_fact.withColumn(f"{date_col}Year", F.year(F.col(date_col))).withColumn(f"{date_col}Month", F.month(F.col(date_col))).withColumn(f"{date_col}Day", F.dayofmonth(F.col(date_col))).withColumn(f"{date_col}Quarter", F.quarter(F.col(date_col)))
        except:
            pass
    df_fact = df_fact.withColumn("_gold_load_timestamp", F.current_timestamp()).withColumn("_gold_run_id", F.lit(run_id)).withColumn("_gold_model_name", F.lit(model_config.ModelName))
    return df_fact

def build_aggregate_model(model_config):
    source_tables = json.loads(model_config.SourceTables)
    primary_table = source_tables[0]
    df = read_silver_table(model_config.SourceLakehouse, model_config.SourceSchema, primary_table)
    grain_cols = [c.strip() for c in model_config.GrainColumns.split(",")] if model_config.GrainColumns else []
    if model_config.CalculatedColumns:
        try:
            calc_cols = json.loads(model_config.CalculatedColumns)
            agg_exprs = []
            for col_name, expr in calc_cols.items():
                expr_upper = expr.upper().strip()
                if expr_upper.startswith("SUM("):
                    source_col = expr[4:].replace(")", "").strip()
                    agg_exprs.append(F.sum(source_col).alias(col_name))
                elif expr_upper.startswith("COUNT(DISTINCT"):
                    source_col = expr[15:].replace(")", "").strip()
                    agg_exprs.append(F.countDistinct(source_col).alias(col_name))
                elif expr_upper.startswith("COUNT("):
                    source_col = expr[6:].replace(")", "").strip()
                    agg_exprs.append(F.count(source_col).alias(col_name))
                elif expr_upper.startswith("AVG("):
                    source_col = expr[4:].replace(")", "").strip()
                    agg_exprs.append(F.avg(source_col).alias(col_name))
            if agg_exprs and grain_cols:
                df = df.groupBy(*grain_cols).agg(*agg_exprs)
        except Exception as e:
            print(f"WARNING: Failed to parse aggregate config: {str(e)}")
    df = df.withColumn("_gold_load_timestamp", F.current_timestamp()).withColumn("_gold_run_id", F.lit(run_id)).withColumn("_gold_model_name", F.lit(model_config.ModelName))
    return df

def process_single_model(model_config):
    gold_run_id = None
    rows_read = 0
    rows_written = 0
    error_msg = None
    try:
        source_tables = json.loads(model_config.SourceTables)
        gold_run_id = log_gold_model_start(model_config.ModelId, model_config.ModelName, model_config.ModelType, source_tables)
        if model_config.ModelType == "DIMENSION":
            df_gold = build_dimension_model(model_config)
        elif model_config.ModelType == "FACT":
            df_gold = build_fact_model(model_config)
        elif model_config.ModelType == "AGGREGATE":
            df_gold = build_aggregate_model(model_config)
        else:
            raise ValueError(f"Unknown model type: {model_config.ModelType}")
        rows_read = df_gold.count()
        if rows_read > 0:
            rows_written = write_gold_table(df_gold, model_config.TargetWarehouse, model_config.TargetSchema, model_config.TargetTableName)
        log_gold_model_end(gold_run_id, "SUCCEEDED", rows_read, rows_written)
        return {"model_name": model_config.ModelName, "status": "SUCCEEDED", "rows_read": rows_read, "rows_written": rows_written, "error": None}
    except Exception as e:
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        if gold_run_id:
            log_gold_model_end(gold_run_id, "FAILED", rows_read, rows_written, error_msg)
        return {"model_name": model_config.ModelName, "status": "FAILED", "rows_read": rows_read, "rows_written": rows_written, "error": error_msg, "stack_trace": stack_trace}

def main():
    results = []
    try:
        models = get_gold_models_from_control_db()
        if not models:
            mssparkutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "models_processed": 0, "results": []}))
            return
        for model_config in models:
            result = process_single_model(model_config)
            results.append(result)
        succeeded = sum(1 for r in results if r["status"] == "SUCCEEDED")
        failed = sum(1 for r in results if r["status"] == "FAILED")
        overall_status = "SUCCEEDED" if failed == 0 else "PARTIAL" if succeeded > 0 else "FAILED"
        final_result = {"status": overall_status, "models_processed": len(results), "succeeded": succeeded, "failed": failed, "results": results}
    except Exception as e:
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        final_result = {"status": "FAILED", "error": error_msg, "stack_trace": stack_trace, "results": results}
    mssparkutils.notebook.exit(json.dumps(final_result))

main()
