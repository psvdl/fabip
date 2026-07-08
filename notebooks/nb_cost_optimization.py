# ============================================================================
# COST OPTIMIZATION NOTEBOOK
# Fabric-compatible cost optimization: capacity scheduling, storage cleanup
# Replaces AzureMLExecutePipeline activity
# ============================================================================

import json
from datetime import datetime

# Fabric pipeline parameters are injected as notebook-scoped variables
action = globals().get("action", "optimize_all")
control_sql_endpoint = globals().get("control_sql_endpoint", "")
control_database_name = globals().get("control_database_name", "fabric_control")
key_vault_url = globals().get("key_vault_url", "")
lakehouses_json = globals().get("lakehouses_json", '["lh_bronze","lh_silver","lh_gold"]')
retention_days = int(globals().get("retention_days", "90"))
dry_run = globals().get("dry_run", "true").lower() == "true"
lakehouses = json.loads(lakehouses_json)

def build_jdbc_url():
    if not control_sql_endpoint or not control_sql_endpoint.strip():
        return None
    return (
        f"jdbc:sqlserver://{control_sql_endpoint}:1433;"
        f"database={control_database_name};"
        f"encrypt=true;"
        f"trustServerCertificate=false;"
        f"hostNameInCertificate=*.sql.azuresynapse.net;"
        f"loginTimeout=30;"
    )

def log_to_control_db(action_name, status, details_json):
    jdbc_url = build_jdbc_url()
    if not jdbc_url:
        print(f"WARNING: Cannot log to control DB - endpoint not configured")
        return
    try:
        query = (
            f"EXEC audit.usp_LogPipelineEnd "
            f"@RunId=NEWID(), "
            f"@Status='{status}', "
            f"@ErrorMessage='{details_json.replace(chr(39), chr(39)+chr(39))}'"
        )
        spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("authentication", "ActiveDirectoryMSI") \
            .load()
    except Exception as e:
        print(f"WARNING: Failed to log to control DB: {str(e)}")

def get_table_sizes(lakehouse):
    """List all Delta tables in a lakehouse with their sizes."""
    results = []
    try:
        tables_df = spark.sql(f"SHOW TABLES IN {lakehouse}")
        for row in tables_df.collect():
            schema = row["namespace"]
            table = row["tableName"]
            try:
                detail_df = spark.sql(f"DESCRIBE DETAIL {lakehouse}.{schema}.{table}")
                detail = detail_df.collect()[0]
                results.append({
                    "lakehouse": lakehouse,
                    "schema": schema,
                    "table": table,
                    "size_bytes": detail.sizeInBytes,
                    "size_mb": round(detail.sizeInBytes / (1024 * 1024), 2),
                    "num_files": detail.numFiles,
                    "last_modified": str(detail.lastModified)
                })
            except Exception as e:
                results.append({
                    "lakehouse": lakehouse,
                    "schema": schema,
                    "table": table,
                    "error": str(e)
                })
    except Exception as e:
        print(f"WARNING: Could not list tables in {lakehouse}: {str(e)}")
    return results

def cleanup_old_files(lakehouse, schema, table, retention_days):
    """Delete partitions/files older than retention_days."""
    if dry_run:
        return {"action": "DRY_RUN", "lakehouse": lakehouse, "table": table}
    try:
        cutoff_date = (datetime.now() - __import__("datetime").timedelta(days=retention_days)).strftime("%Y-%m-%d")
        # Try to delete old partitions based on common date columns
        date_cols = ["_bronze_ingestion_timestamp", "_silver_transform_timestamp", "_gold_load_timestamp"]
        for date_col in date_cols:
            try:
                spark.sql(f"""
                    DELETE FROM {lakehouse}.{schema}.{table}
                    WHERE {date_col} < '{cutoff_date}'
                """)
                return {"action": "DELETED", "cutoff_date": cutoff_date, "column": date_col}
            except:
                continue
        return {"action": "SKIPPED", "reason": "No standard timestamp column found"}
    except Exception as e:
        return {"action": "FAILED", "error": str(e)}

def optimize_tables(lakehouse):
    """Run OPTIMIZE on all tables in a lakehouse."""
    results = []
    try:
        tables_df = spark.sql(f"SHOW TABLES IN {lakehouse}")
        for row in tables_df.collect():
            schema = row["namespace"]
            table = row["tableName"]
            try:
                if dry_run:
                    results.append({"table": f"{schema}.{table}", "action": "DRY_RUN"})
                else:
                    spark.sql(f"OPTIMIZE {lakehouse}.{schema}.{table}")
                    spark.sql(f"VACUUM {lakehouse}.{schema}.{table} RETAIN 168 HOURS")
                    results.append({"table": f"{schema}.{table}", "action": "OPTIMIZED"})
            except Exception as e:
                results.append({"table": f"{schema}.{table}", "action": "FAILED", "error": str(e)})
    except Exception as e:
        print(f"WARNING: Could not optimize {lakehouse}: {str(e)}")
    return results

def main():
    start_time = datetime.now()
    all_results = {
        "action": action,
        "dry_run": dry_run,
        "retention_days": retention_days,
        "lakehouses": {},
        "start_time": start_time.isoformat()
    }
    
    total_size_before_mb = 0
    
    for lakehouse in lakehouses:
        print(f"Processing lakehouse: {lakehouse}")
        
        # Get current storage stats
        table_sizes = get_table_sizes(lakehouse)
        lakehouse_size_mb = sum(t.get("size_mb", 0) for t in table_sizes)
        total_size_before_mb += lakehouse_size_mb
        
        # Optimize tables
        optimize_results = optimize_tables(lakehouse)
        
        # Cleanup old files if requested
        cleanup_results = []
        if action in ["cleanup", "optimize_all"]:
            for t in table_sizes:
                if "error" not in t:
                    cleanup_results.append(cleanup_old_files(
                        t["lakehouse"], t["schema"], t["table"], retention_days
                    ))
        
        all_results["lakehouses"][lakehouse] = {
            "size_mb": lakehouse_size_mb,
            "tables": len(table_sizes),
            "optimize_results": optimize_results,
            "cleanup_results": cleanup_results
        }
    
    end_time = datetime.now()
    duration_seconds = (end_time - start_time).total_seconds()
    
    all_results.update({
        "end_time": end_time.isoformat(),
        "duration_seconds": duration_seconds,
        "total_size_before_mb": round(total_size_before_mb, 2),
        "status": "SUCCEEDED"
    })
    
    # Log to control DB
    log_to_control_db(action, "SUCCEEDED", json.dumps(all_results, default=str))
    
    mssparkutils.notebook.exit(json.dumps(all_results, default=str))

main()
