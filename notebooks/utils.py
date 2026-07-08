# ============================================================================
# UTILITIES NOTEBOOK
# Reusable helper functions for Fabric ELT
# ============================================================================

import json
from datetime import datetime
from delta.tables import DeltaTable

def get_spark_ui_url():
    return spark.sparkContext.uiWebUrl

def log_to_control_table(message, level="INFO", lakehouse="lh_system"):
    log_data = [{"timestamp": datetime.now().isoformat(), "level": level, "message": message, "spark_app_id": spark.sparkContext.applicationId}]
    df = spark.createDataFrame(log_data)
    df.write.format("delta").mode("append").save(f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/system/logs")

def safe_cast(df, column, target_type):
    from pyspark.sql.functions import col, when
    if target_type == "int":
        return df.withColumn(column, col(column).cast("int"))
    elif target_type == "double":
        return df.withColumn(column, col(column).cast("double"))
    elif target_type == "timestamp":
        return df.withColumn(column, col(column).cast("timestamp"))
    elif target_type == "date":
        return df.withColumn(column, col(column).cast("date"))
    else:
        return df.withColumn(column, col(column).cast(target_type))

def add_audit_columns(df, run_id, stage="bronze"):
    from pyspark.sql.functions import current_timestamp, lit
    return df.withColumn(f"_{stage}_ingestion_timestamp", current_timestamp()).withColumn(f"_{stage}_run_id", lit(run_id))

def get_delta_table_stats(lakehouse, schema, table):
    path = f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{schema}/{table}"
    if DeltaTable.isDeltaTable(spark, path):
        dt = DeltaTable.forPath(spark, path)
        detail = dt.detail().collect()[0]
        return {"num_files": detail.numFiles, "size_in_bytes": detail.sizeInBytes, "last_modified": str(detail.lastModified), "partition_columns": detail.partitionColumns}
    return None

print("Utilities loaded successfully")
