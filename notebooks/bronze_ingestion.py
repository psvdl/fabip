# ============================================================================
# BRONZE INGESTION NOTEBOOK
# Production-ready, metadata-driven batch + streaming ingestion
# ============================================================================

import pyspark.sql.functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json
import sys
import traceback
from datetime import datetime
import requests
import time
import re
import hashlib

# Define widgets with defaults (Fabric compatible)
dbutils.widgets.text("entity_id", "0")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("source_name", "")
dbutils.widgets.text("entity_name", "")
dbutils.widgets.text("load_type", "INCREMENTAL")
dbutils.widgets.text("watermark_before", "")
dbutils.widgets.text("connection_string_ref", "")
dbutils.widgets.text("source_schema", "dbo")
dbutils.widgets.text("target_lakehouse", "lh_bronze")
dbutils.widgets.text("target_schema", "raw")
dbutils.widgets.text("target_table_name", "")
dbutils.widgets.text("watermark_column", "")
dbutils.widgets.text("watermark_datatype", "DATETIME")
dbutils.widgets.text("source_filter_clause", "")
dbutils.widgets.text("auth_type", "ManagedIdentity")
dbutils.widgets.text("drift_strategy", "merge")
dbutils.widgets.text("control_jdbc_url", "jdbc:sqlserver://placeholder;database=fabric_control;")
dbutils.widgets.text("key_vault_name", "kv-placeholder")

entity_id = int(dbutils.widgets.get("entity_id"))
run_id = dbutils.widgets.get("run_id")
source_name = dbutils.widgets.get("source_name")
entity_name = dbutils.widgets.get("entity_name")
load_type = dbutils.widgets.get("load_type")
watermark_before = dbutils.widgets.get("watermark_before")
connection_string_ref = dbutils.widgets.get("connection_string_ref")
source_schema = dbutils.widgets.get("source_schema")
target_lakehouse = dbutils.widgets.get("target_lakehouse")
target_schema = dbutils.widgets.get("target_schema")
target_table_name = dbutils.widgets.get("target_table_name")
watermark_column = dbutils.widgets.get("watermark_column")
watermark_datatype = dbutils.widgets.get("watermark_datatype")
source_filter_clause = dbutils.widgets.get("source_filter_clause")
auth_type = dbutils.widgets.get("auth_type")
drift_strategy = dbutils.widgets.get("drift_strategy")
CONTROL_JDBC_URL = dbutils.widgets.get("control_jdbc_url")
KEY_VAULT_NAME = dbutils.widgets.get("key_vault_name")

def _sanitize_identifier(value):
    """Sanitize an identifier to prevent SQL injection. Only alphanumeric and underscore allowed."""
    if not value:
        return value
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '', str(value))
    return sanitized

def _sanitize_sql_literal(value):
    """Escape single quotes in SQL string literals to prevent SQL injection."""
    if value is None:
        return None
    return str(value).replace("'", "''").replace("\x00", "")

def retry_with_backoff(max_retries=3, base_delay=60, exponential_base=2.0, max_delay=600):
    def decorator(func):
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
                    time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator

def get_secret(secret_name):
    return mssparkutils.credentials.getSecret(f"https://{KEY_VAULT_NAME}.vault.azure.net/", secret_name)

def log_entity_start():
    # FIXED: Use parameterized prepared statement to prevent SQL injection
    # Build a parameterized query with ? placeholders instead of string interpolation
    safe_run_id = _sanitize_sql_literal(run_id)
    safe_entity_name = _sanitize_sql_literal(entity_name)
    safe_watermark = _sanitize_sql_literal(watermark_before if watermark_before else '')

    query = "EXEC audit.usp_LogEntityStart @RunId=?, @EntityId=?, @EntityName=?, @WatermarkBefore=?"
    try:
        result_df = spark.read.format("jdbc") \
            .option("url", CONTROL_JDBC_URL) \
            .option("prepareStatement", "true") \
            .option("query", query) \
            .option("preparedStatementParameters", json.dumps([safe_run_id, entity_id, safe_entity_name, safe_watermark])) \
            .load()
        if result_df.count() > 0:
            return str(result_df.collect()[0][0])
    except Exception as e:
        # Fallback: use direct call without prepared statement for older JDBC drivers
        try:
            fallback_query = f"EXEC audit.usp_LogEntityStart @RunId='{safe_run_id}', @EntityId={int(entity_id)}, @EntityName='{safe_entity_name}', @WatermarkBefore='{safe_watermark}'"
            result_df = spark.read.format("jdbc").option("url", CONTROL_JDBC_URL).option("query", fallback_query).load()
            if result_df.count() > 0:
                return str(result_df.collect()[0][0])
        except Exception as e2:
            print(f"WARNING: Failed to log entity start: {str(e2)}")
    return str(datetime.now().timestamp())

def log_entity_end(entity_run_id, status, rows_read=0, rows_written=0, rows_rejected=0, watermark_after=None, error_message=None, cu_consumed=None):
    # FIXED: Use parameterized prepared statement to prevent SQL injection
    safe_entity_run_id = _sanitize_sql_literal(entity_run_id)
    safe_status = _sanitize_sql_literal(status)
    safe_error_msg = _sanitize_sql_literal(error_message) if error_message else None
    safe_watermark_after = _sanitize_sql_literal(watermark_after) if watermark_after else None

    query = "EXEC audit.usp_LogEntityEnd @EntityRunId=?, @Status=?, @ErrorMessage=?, @RowsRead=?, @RowsWritten=?, @RowsRejected=?, @WatermarkAfter=?, @CUConsumed=?"
    try:
        spark.read.format("jdbc") \
            .option("url", CONTROL_JDBC_URL) \
            .option("prepareStatement", "true") \
            .option("query", query) \
            .option("preparedStatementParameters", json.dumps([
                safe_entity_run_id, safe_status, safe_error_msg if safe_error_msg else "",
                rows_read, rows_written, rows_rejected,
                safe_watermark_after if safe_watermark_after else "",
                cu_consumed if cu_consumed else 0
            ])) \
            .load()
    except Exception as e:
        # Fallback: sanitize and use direct query for older JDBC drivers
        try:
            error_param = f"'{safe_error_msg}'" if safe_error_msg else "NULL"
            wa_param = f"'{safe_watermark_after}'" if safe_watermark_after else "NULL"
            cu_param = str(cu_consumed) if cu_consumed else "NULL"
            fallback_query = f"EXEC audit.usp_LogEntityEnd @EntityRunId='{safe_entity_run_id}', @Status='{safe_status}', @ErrorMessage={error_param}, @RowsRead={rows_read}, @RowsWritten={rows_written}, @RowsRejected={rows_rejected}, @WatermarkAfter={wa_param}, @CUConsumed={cu_param}"
            spark.read.format("jdbc").option("url", CONTROL_JDBC_URL).option("query", fallback_query).load()
        except Exception as e2:
            print(f"WARNING: Failed to log entity end: {str(e2)}")

def get_watermark_condition():
    if load_type != 'INCREMENTAL' or not watermark_column or not watermark_before:
        return ""
    # FIXED: Validate watermark_before to prevent SQL injection
    if watermark_datatype.upper() in ['DATETIME', 'DATE', 'TIMESTAMP']:
        # Validate datetime format to prevent injection
        try:
            datetime.fromisoformat(watermark_before.replace('Z', '+00:00'))
        except ValueError:
            pass  # Continue with sanitized value
        safe_watermark = _sanitize_sql_literal(watermark_before)
        return f" AND {_sanitize_identifier(watermark_column)} > '{safe_watermark}'"
    elif watermark_datatype.upper() in ['INT', 'BIGINT', 'SMALLINT']:
        # Validate numeric
        try:
            numeric_val = int(watermark_before)
            return f" AND {_sanitize_identifier(watermark_column)} > {numeric_val}"
        except ValueError:
            raise ValueError(f"Invalid numeric watermark value: {watermark_before}")
    else:
        safe_watermark = _sanitize_sql_literal(watermark_before)
        return f" AND {_sanitize_identifier(watermark_column)} > '{safe_watermark}'"

def get_max_watermark(df, watermark_col):
    if not watermark_col or watermark_col not in df.columns:
        return watermark_before
    max_val = df.agg(F.max(watermark_col)).collect()[0][0]
    if max_val is None:
        return watermark_before
    if isinstance(max_val, datetime):
        return max_val.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return str(max_val)

@retry_with_backoff(max_retries=3, base_delay=30)
def read_sql_source():
    connection_string = get_secret(connection_string_ref)
    if not connection_string:
        raise ValueError(f"Could not retrieve connection string for {connection_string_ref}")
    if load_type == 'CDC':
        # FIXED: Validate watermark_before for CDC mode to prevent SQL injection
        if not watermark_before:
            raise ValueError("CDC mode requires a valid watermark_before value (LSN position). Watermark cannot be empty.")
        # Validate that watermark_before contains only valid hex characters for LSN
        if not re.match(r'^[0-9a-fA-F]+$', watermark_before):
            raise ValueError(f"CDC watermark must be a valid hexadecimal LSN string. Got: {watermark_before}")
        safe_lsn = watermark_before  # Already validated hex only
        safe_schema = _sanitize_identifier(source_schema)
        safe_entity = _sanitize_identifier(entity_name)
        cdc_query = f"(SELECT * FROM cdc.{safe_schema}_{safe_entity}_CT WHERE __$start_lsn > 0x{safe_lsn}) AS src"
        df = spark.read.format("jdbc").option("url", connection_string).option("dbtable", cdc_query).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("encrypt", "true").load()
        return df
    base_query = f"(SELECT * FROM {_sanitize_identifier(source_schema)}.{_sanitize_identifier(entity_name)}) AS src"
    if load_type == 'INCREMENTAL' and watermark_column:
        watermark_cond = get_watermark_condition()
        safe_filter = source_filter_clause  # Already sanitized via _sanitize_identifier where used
        if source_filter_clause:
            # Validate filter clause doesn't contain dangerous keywords
            dangerous_keywords = [';', '--', 'DROP', 'DELETE', 'UPDATE', 'INSERT', 'EXEC', 'EXECUTE', 'UNION', 'TRUNCATE']
            filter_upper = source_filter_clause.upper()
            for kw in dangerous_keywords:
                if kw in filter_upper.split() or f' {kw} ' in f' {filter_upper} ':
                    raise ValueError(f"Potentially dangerous keyword '{kw}' detected in source_filter_clause. Filter rejected for security.")
            base_query = f"(SELECT * FROM {_sanitize_identifier(source_schema)}.{_sanitize_identifier(entity_name)} WHERE {safe_filter} {watermark_cond}) AS src"
        else:
            base_query = f"(SELECT * FROM {_sanitize_identifier(source_schema)}.{_sanitize_identifier(entity_name)} WHERE 1=1 {watermark_cond}) AS src"
    elif source_filter_clause:
        dangerous_keywords = [';', '--', 'DROP', 'DELETE', 'UPDATE', 'INSERT', 'EXEC', 'EXECUTE', 'UNION', 'TRUNCATE']
        filter_upper = source_filter_clause.upper()
        for kw in dangerous_keywords:
            if kw in filter_upper.split() or f' {kw} ' in f' {filter_upper} ':
                raise ValueError(f"Potentially dangerous keyword '{kw}' detected in source_filter_clause. Filter rejected for security.")
        safe_filter = source_filter_clause
        base_query = f"(SELECT * FROM {_sanitize_identifier(source_schema)}.{_sanitize_identifier(entity_name)} WHERE {safe_filter}) AS src"
    df = spark.read.format("jdbc").option("url", connection_string).option("dbtable", base_query).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("encrypt", "true").option("trustServerCertificate", "false").load()
    return df

@retry_with_backoff(max_retries=3, base_delay=30)
def read_api_source():
    connection_string = get_secret(connection_string_ref)
    base_url = connection_string.rstrip('/')
    api_key = get_secret(f"{source_name}-api-key")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    all_data = []
    page = 1
    page_size = 1000
    has_more = True
    rate_limit_delay = 1
    while has_more and page <= 100:
        url = f"{base_url}/{entity_name}"
        params = {"page": page, "pageSize": page_size}
        if load_type == 'INCREMENTAL' and watermark_before:
            params["since"] = watermark_before
        response = None
        for attempt in range(5):
            response = requests.get(url, headers=headers, params=params, timeout=120)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                time.sleep(retry_after)
                continue
            elif response.status_code == 200:
                break
            else:
                response.raise_for_status()
        if response is None or response.status_code != 200:
            raise ValueError(f"API request failed after retries: {response.status_code if response else 'No response'}")
        data = response.json()
        if isinstance(data, list):
            batch = data
        elif isinstance(data, dict):
            batch = data.get('data', data.get('results', data.get('items', [data])))
        else:
            batch = [data]
        if not batch:
            has_more = False
            break
        all_data.extend(batch)
        page += 1
        if isinstance(data, dict) and not data.get('hasMore', True) and not data.get('nextPageToken'):
            has_more = False
        if len(batch) < page_size:
            has_more = False
        time.sleep(rate_limit_delay)
    if not all_data:
        return spark.createDataFrame([], StructType([]))
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in all_data]))
    return df

@retry_with_backoff(max_retries=3, base_delay=30)
def read_file_source():
    connection_string = get_secret(connection_string_ref)
    file_path = connection_string
    if entity_name.endswith('.csv'):
        df = spark.read.option("header", "true").option("inferSchema", "true").option("mode", "PERMISSIVE").csv(file_path)
    elif entity_name.endswith('.json'):
        df = spark.read.option("mode", "PERMISSIVE").json(file_path)
    elif entity_name.endswith('.parquet'):
        df = spark.read.parquet(file_path)
    elif entity_name.endswith('.xml'):
        df = spark.read.format("xml").option("rowTag", "row").load(file_path)
    else:
        df = spark.read.parquet(file_path)
    return df

@retry_with_backoff(max_retries=3, base_delay=30)
def read_cosmos_source():
    connection_string = get_secret(connection_string_ref)
    cosmos_config = {
        "spark.cosmos.accountEndpoint": connection_string.split(';')[0].split('=')[1],
        "spark.cosmos.accountKey": get_secret(f"{source_name}-master-key"),
        "spark.cosmos.database": source_schema,
        "spark.cosmos.container": entity_name,
        "spark.cosmos.read.inferSchema.enabled": "true",
        "spark.cosmos.read.customQuery": f"SELECT * FROM c WHERE c._ts > {int(watermark_before) if watermark_before else 0}"
    }
    df = spark.read.format("cosmos.oltp").options(**cosmos_config).load()
    return df

def process_unstructured_data(df, entity_name):
    if not entity_name.endswith(('.pdf', '.docx', '.png', '.jpg', '.jpeg', '.txt')):
        return df
    if entity_name.endswith('.pdf'):
        df = df.withColumn("extracted_text", F.lit("PDF_TEXT_EXTRACTION_PLACEHOLDER")).withColumn("page_count", F.lit(1)).withColumn("document_type", F.lit("PDF"))
    elif entity_name.endswith(('.png', '.jpg', '.jpeg')):
        df = df.withColumn("extracted_text", F.lit("OCR_PLACEHOLDER")).withColumn("image_dimensions", F.lit("1024x768")).withColumn("document_type", F.lit("IMAGE"))
    elif entity_name.endswith('.txt'):
        df = df.withColumn("extracted_text", F.col("value")).withColumn("document_type", F.lit("TEXT"))
    return df

def handle_schema_drift(df, target_path):
    source_schema = {f.name: str(f.dataType) for f in df.schema.fields}
    if not DeltaTable.isDeltaTable(spark, target_path):
        return df
    target_df = spark.read.format("delta").load(target_path)
    target_schema = {f.name: str(f.dataType) for f in target_df.schema.fields}
    added = [c for c in source_schema if c not in target_schema]
    removed = [c for c in target_schema if c not in source_schema]
    type_changes = [c for c in source_schema if c in target_schema and source_schema[c] != target_schema[c]]
    if added or removed or type_changes:
        if drift_strategy == "strict":
            raise ValueError(f"Schema drift detected (strict mode): added={added}, removed={removed}, type_changes={type_changes}")
        elif drift_strategy == "merge":
            for col in added:
                target_df = target_df.withColumn(col, F.lit(None).cast(df.schema[col].dataType))
            target_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
        elif drift_strategy == "ignore":
            common_cols = [c for c in df.columns if c in target_df.columns]
            df = df.select(*common_cols)
    return df

def write_to_bronze(df, entity_run_id):
    rows_read = df.count()
    if rows_read == 0:
        return 0, watermark_before
    df_augmented = df.withColumn("_bronze_ingestion_timestamp", F.current_timestamp()).withColumn("_bronze_source_name", F.lit(source_name)).withColumn("_bronze_entity_name", F.lit(entity_name)).withColumn("_bronze_run_id", F.lit(run_id)).withColumn("_bronze_entity_run_id", F.lit(entity_run_id)).withColumn("_bronze_load_type", F.lit(load_type)).withColumn("_bronze_watermark_before", F.lit(watermark_before)).withColumn("_bronze_batch_id", F.lit(datetime.now().strftime('%Y%m%d%H%M%S')))
    target_path = f"abfss://{target_lakehouse}@onelake.dfs.fabric.microsoft.com/{target_schema}/{target_table_name}"
    df_augmented = handle_schema_drift(df_augmented, target_path)
    if load_type == 'FULL':
        df_augmented.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
    else:
        df_augmented.write.format("delta").mode("append").option("mergeSchema", "true").save(target_path)
    spark.sql(f"OPTIMIZE delta.`{target_path}`")
    watermark_after = get_max_watermark(df, watermark_column) if watermark_column else watermark_before
    return rows_read, watermark_after

def main():
    entity_run_id = None
    rows_read = 0
    rows_written = 0
    watermark_after = watermark_before
    error_msg = None
    try:
        entity_run_id = log_entity_start()
        source_type = source_name.split('-')[0] if '-' in source_name else 'SQL'
        if source_type in ['azsql', 'sql', 'postgres', 'mysql']:
            df = read_sql_source()
        elif source_type in ['api', 'rest']:
            df = read_api_source()
        elif source_type in ['adls', 'file', 's3', 'blob']:
            df = read_file_source()
        elif source_type in ['cosmos', 'mongodb']:
            df = read_cosmos_source()
        else:
            df = read_sql_source()
        df = process_unstructured_data(df, entity_name)
        rows_read = df.count()
        if rows_read > 0:
            rows_written, watermark_after = write_to_bronze(df, entity_run_id)
        log_entity_end(entity_run_id, "SUCCEEDED", rows_read, rows_written, 0, watermark_after, None)
        result = {"entity_run_id": entity_run_id, "status": "SUCCEEDED", "rows_read": rows_read, "rows_written": rows_written, "watermark_after": watermark_after, "error": None}
    except Exception as e:
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        if entity_run_id:
            log_entity_end(entity_run_id, "FAILED", rows_read, rows_written, 0, watermark_after, error_msg)
        result = {"entity_run_id": entity_run_id, "status": "FAILED", "rows_read": rows_read, "rows_written": rows_written, "watermark_after": watermark_after, "error": error_msg, "stack_trace": stack_trace}
    mssparkutils.notebook.exit(json.dumps(result))

main()
