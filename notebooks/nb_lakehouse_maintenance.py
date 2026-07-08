# ============================================================================
# LAKEHOUSE MAINTENANCE NOTEBOOK
# Automated VACUUM, OPTIMIZE, compaction, and partition cleanup
# Fabric SQL Database compatible (uses lakehouse paths - no JDBC required)
# ============================================================================

import pyspark.sql.functions as F
from delta.tables import DeltaTable
import json
from datetime import datetime, timedelta

# Fabric pipeline parameters are injected as notebook-scoped variables
lakehouse_name = globals().get("lakehouse_name", "lh_bronze")
schema_name = globals().get("schema_name", "raw")
dry_run = globals().get("dry_run", "true").lower() == "true"
retention_days = int(globals().get("retention_days", "90"))
timestamp_column = globals().get("timestamp_column", "_bronze_ingestion_timestamp")

def get_delta_tables(lakehouse, schema):
    schema_path = f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{schema}"
    try:
        tables = spark.sql(f"SHOW TABLES IN {lakehouse}.{schema}").collect()
        return [row.tableName for row in tables]
    except:
        return []

def optimize_table(lakehouse, schema, table):
    path = f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{schema}/{table}"
    if not DeltaTable.isDeltaTable(spark, path):
        return {"status": "SKIPPED", "reason": "Not a Delta table"}
    try:
        pre_detail = DeltaTable.forPath(spark, path).detail().collect()[0]
        pre_files = pre_detail.numFiles
        pre_size = pre_detail.sizeInBytes
        if dry_run:
            return {"status": "DRY_RUN", "pre_files": pre_files, "pre_size_mb": round(pre_size / (1024*1024), 2)}
        spark.sql(f"OPTIMIZE delta.`{path}`")
        spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")
        post_detail = DeltaTable.forPath(spark, path).detail().collect()[0]
        post_files = post_detail.numFiles
        post_size = post_detail.sizeInBytes
        return {"status": "OPTIMIZED", "pre_files": pre_files, "post_files": post_files, "files_reduced": pre_files - post_files, "pre_size_mb": round(pre_size / (1024*1024), 2), "post_size_mb": round(post_size / (1024*1024), 2), "size_reduction_mb": round((pre_size - post_size) / (1024*1024), 2)}
    except Exception as e:
        return {"status": "FAILED", "error": str(e)}

def analyze_table_stats(lakehouse, schema, table):
    path = f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{schema}/{table}"
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    try:
        detail = DeltaTable.forPath(spark, path).detail().collect()[0]
        history = DeltaTable.forPath(spark, path).history().limit(10).collect()
        last_optimize = None
        for h in history:
            if 'OPTIMIZE' in str(h.get('operation', '')):
                last_optimize = str(h.get('timestamp', ''))
                break
        return {"table": table, "num_files": detail.numFiles, "size_mb": round(detail.sizeInBytes / (1024*1024), 2), "partition_columns": detail.partitionColumns, "last_modified": str(detail.lastModified), "last_optimize": last_optimize, "needs_optimize": detail.numFiles > 1000}
    except Exception as e:
        return {"table": table, "error": str(e)}

def compact_small_files(lakehouse, schema, table, min_file_size_mb=128):
    path = f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{schema}/{table}"
    if not DeltaTable.isDeltaTable(spark, path):
        return {"status": "SKIPPED"}
    if dry_run:
        return {"status": "DRY_RUN", "action": f"Would compact files < {min_file_size_mb} MB"}
    try:
        df = spark.read.format("delta").load(path)
        target_partitions = max(1, df.count() // 1000000)
        df.coalesce(target_partitions).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        return {"status": "COMPACTED", "target_partitions": target_partitions}
    except Exception as e:
        return {"status": "FAILED", "error": str(e)}

def cleanup_old_partitions(lakehouse, schema, table, retention_days=90):
    path = f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{schema}/{table}"
    if not DeltaTable.isDeltaTable(spark, path):
        return {"status": "SKIPPED"}
    cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d')
    if dry_run:
        return {"status": "DRY_RUN", "action": f"Would delete partitions before {cutoff_date}"}
    try:
        if timestamp_column in [f.name for f in spark.read.format("delta").load(path).schema.fields]:
            spark.sql(f"DELETE FROM delta.`{path}` WHERE {timestamp_column} < '{cutoff_date}'")
            return {"status": "CLEANED", "cutoff_date": cutoff_date}
        else:
            return {"status": "SKIPPED", "reason": f"Timestamp column {timestamp_column} not found"}
    except Exception as e:
        return {"status": "FAILED", "error": str(e)}

def main():
    tables = get_delta_tables(lakehouse_name, schema_name)
    results = []
    for table in tables:
        stats = analyze_table_stats(lakehouse_name, schema_name, table)
        if stats and stats.get("needs_optimize"):
            opt_result = optimize_table(lakehouse_name, schema_name, table)
        else:
            opt_result = {"status": "SKIPPED", "reason": "Files under threshold"}
        compact_result = compact_small_files(lakehouse_name, schema_name, table)
        cleanup_result = cleanup_old_partitions(lakehouse_name, schema_name, table, retention_days)
        results.append({"table": table, "stats": stats, "optimize": opt_result, "compact": compact_result, "cleanup": cleanup_result})
    optimized = sum(1 for r in results if r["optimize"].get("status") == "OPTIMIZED")
    compacted = sum(1 for r in results if r["compact"].get("status") == "COMPACTED")
    failed = sum(1 for r in results if r["optimize"].get("status") == "FAILED" or r["compact"].get("status") == "FAILED")
    result = {"status": "SUCCEEDED" if failed == 0 else "PARTIAL", "tables_processed": len(results), "optimized": optimized, "compacted": compacted, "failed": failed, "details": results}
    mssparkutils.notebook.exit(json.dumps(result))

main()
