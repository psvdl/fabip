# ============================================================================
# DATA PROFILING NOTEBOOK
# Automated data profiling and statistics collection
# Fabric SQL Database compatible (uses lakehouse paths - no JDBC required)
# ============================================================================

import pyspark.sql.functions as F
from pyspark.sql.types import *
import json
from datetime import datetime

# Fabric pipeline parameters are injected as notebook-scoped variables
lakehouse = globals().get("lakehouse", "lh_bronze")
schema = globals().get("schema", "raw")
table = globals().get("table", "")
sample_size = int(globals().get("sample_size", "100000"))
control_sql_endpoint = globals().get("control_sql_endpoint", "")
control_database_name = globals().get("control_database_name", "fabric_control")
key_vault_url = globals().get("key_vault_url", "")

def profile_table(lakehouse, schema, table, sample_size=100000):
    path = f"abfss://{lakehouse}@onelake.dfs.fabric.microsoft.com/{schema}/{table}"
    df = spark.read.format("delta").load(path)
    total_rows = df.count()
    if total_rows > sample_size:
        df = df.sample(False, sample_size / total_rows)
    profile = {"profile_timestamp": datetime.now().isoformat(), "lakehouse": lakehouse, "schema": schema, "table": table, "total_rows": total_rows, "sample_size": df.count(), "columns": {}}
    for field in df.schema.fields:
        col_name = field.name
        col_type = str(field.dataType)
        col_profile = {"type": col_type, "null_count": df.filter(F.col(col_name).isNull()).count(), "distinct_count": df.select(col_name).distinct().count()}
        col_profile["null_pct"] = round(col_profile["null_count"] / total_rows * 100, 2) if total_rows > 0 else 0
        col_profile["distinct_pct"] = round(col_profile["distinct_count"] / total_rows * 100, 2) if total_rows > 0 else 0
        if any(t in col_type for t in ["Int", "Long", "Float", "Double", "Decimal"]):
            agg = df.select(F.min(col_name).alias("min"), F.max(col_name).alias("max"), F.avg(col_name).alias("mean"), F.stddev(col_name).alias("stddev"), F.percentile_approx(col_name, 0.5).alias("median")).collect()[0]
            col_profile["min"] = agg["min"]
            col_profile["max"] = agg["max"]
            col_profile["mean"] = round(agg["mean"], 4) if agg["mean"] else None
            col_profile["stddev"] = round(agg["stddev"], 4) if agg["stddev"] else None
            col_profile["median"] = agg["median"]
        elif "String" in col_type:
            len_df = df.withColumn("_len", F.length(F.col(col_name)))
            agg = len_df.select(F.min("_len").alias("min_len"), F.max("_len").alias("max_len"), F.avg("_len").alias("avg_len")).collect()[0]
            col_profile["min_len"] = agg["min_len"]
            col_profile["max_len"] = agg["max_len"]
            col_profile["avg_len"] = round(agg["avg_len"], 2) if agg["avg_len"] else None
            top_values = df.groupBy(col_name).count().orderBy(F.desc("count")).limit(10).collect()
            col_profile["top_values"] = [{"value": str(r[0]), "count": r[1]} for r in top_values]
        elif "Date" in col_type or "Timestamp" in col_type:
            agg = df.select(F.min(col_name).alias("min_date"), F.max(col_name).alias("max_date")).collect()[0]
            col_profile["min_date"] = str(agg["min_date"])
            col_profile["max_date"] = str(agg["max_date"])
        profile["columns"][col_name] = col_profile
    return profile

def save_profile_to_delta(profile, target_lakehouse="lh_system", target_schema="profiles"):
    profile_rows = []
    for col_name, stats in profile["columns"].items():
        row = {"profile_timestamp": profile["profile_timestamp"], "lakehouse": profile["lakehouse"], "schema": profile["schema"], "table": profile["table"], "total_rows": profile["total_rows"], "column_name": col_name, "column_type": stats["type"], "null_count": stats.get("null_count"), "null_pct": stats.get("null_pct"), "distinct_count": stats.get("distinct_count"), "distinct_pct": stats.get("distinct_pct")}
        profile_rows.append(row)
    if profile_rows:
        profile_df = spark.createDataFrame(profile_rows)
        path = f"abfss://{target_lakehouse}@onelake.dfs.fabric.microsoft.com/{target_schema}/data_profiles"
        profile_df.write.format("delta").mode("append").save(path)

def main():
    profile = profile_table(lakehouse, schema, table, sample_size)
    save_profile_to_delta(profile)
    mssparkutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "profile": profile}, default=str))

main()
