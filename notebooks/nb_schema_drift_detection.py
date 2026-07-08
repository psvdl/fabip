# ============================================================================
# SCHEMA DRIFT DETECTION NOTEBOOK
# Detects and optionally handles schema drift between source and target
# Fabric SQL Database compatible (uses lakehouse paths - no JDBC required)
# ============================================================================

import pyspark.sql.functions as F
from delta.tables import DeltaTable
import json

# Fabric pipeline parameters are injected as notebook-scoped variables
entity_id = int(globals().get("entity_id", "0"))
source_lakehouse = globals().get("source_lakehouse", "lh_bronze")
source_schema = globals().get("source_schema", "raw")
source_table = globals().get("source_table", "")
drift_strategy = globals().get("drift_strategy", "merge")
control_sql_endpoint = globals().get("control_sql_endpoint", "")
control_database_name = globals().get("control_database_name", "fabric_control")
key_vault_url = globals().get("key_vault_url", "")

def detect_drift(source_df, target_path):
    source_schema = {f.name: str(f.dataType) for f in source_df.schema.fields}
    if not DeltaTable.isDeltaTable(spark, target_path):
        return {"has_drift": True, "drift_type": "NEW_TABLE", "message": "Target table does not exist. Full load required.", "added_columns": list(source_schema.keys()), "removed_columns": [], "type_changes": []}
    target_df = spark.read.format("delta").load(target_path)
    target_schema = {f.name: str(f.dataType) for f in target_df.schema.fields}
    added = [c for c in source_schema if c not in target_schema]
    removed = [c for c in target_schema if c not in source_schema]
    type_changes = [{"column": c, "source_type": source_schema[c], "target_type": target_schema[c]} for c in source_schema if c in target_schema and source_schema[c] != target_schema[c]]
    has_drift = bool(added or removed or type_changes)
    drift_type = None
    if has_drift:
        if added and not removed and not type_changes:
            drift_type = "ADDED_COLUMNS"
        elif removed and not added and not type_changes:
            drift_type = "REMOVED_COLUMNS"
        elif type_changes and not added and not removed:
            drift_type = "TYPE_CHANGES"
        else:
            drift_type = "MIXED"
    return {"has_drift": has_drift, "drift_type": drift_type, "added_columns": added, "removed_columns": removed, "type_changes": type_changes, "message": f"Schema drift detected: {drift_type}" if has_drift else "No schema drift detected"}

def handle_drift(source_df, target_path, strategy):
    drift = detect_drift(source_df, target_path)
    if not drift["has_drift"]:
        return {"action": "NONE", "drift": drift, "dataframe": source_df}
    if strategy == "strict":
        raise ValueError(f"Schema drift detected (strict mode): {json.dumps(drift, indent=2)}")
    elif strategy == "alert":
        print(f"ALERT: Schema drift detected for entity {entity_id}")
        return {"action": "ALERT", "drift": drift, "dataframe": source_df}
    elif strategy == "merge":
        if DeltaTable.isDeltaTable(spark, target_path):
            target_df = spark.read.format("delta").load(target_path)
            for col in drift["added_columns"]:
                target_df = target_df.withColumn(col, F.lit(None).cast(source_df.schema[col].dataType))
            target_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
        return {"action": "MERGE", "drift": drift, "dataframe": source_df}
    elif strategy == "ignore":
        if DeltaTable.isDeltaTable(spark, target_path):
            target_df = spark.read.format("delta").load(target_path)
            common_cols = [c for c in source_df.columns if c in target_df.columns]
            filtered_df = source_df.select(*common_cols)
            return {"action": "IGNORE", "drift": drift, "dataframe": filtered_df}
    return {"action": "UNKNOWN", "drift": drift, "dataframe": source_df}

def main():
    target_path = f"abfss://{source_lakehouse}@onelake.dfs.fabric.microsoft.com/{source_schema}/{source_table}"
    source_path = f"abfss://{source_lakehouse}@onelake.dfs.fabric.microsoft.com/{source_schema}/{source_table}"

    try:
        if DeltaTable.isDeltaTable(spark, source_path):
            source_df = spark.read.format("delta").load(source_path)
        else:
            result = {
                "entity_id": entity_id,
                "target_path": target_path,
                "strategy": drift_strategy,
                "status": "NO_SOURCE",
                "message": f"Source table does not exist at {source_path}"
            }
            mssparkutils.notebook.exit(json.dumps(result))
            return

        drift_result = detect_drift(source_df, target_path)

        if drift_strategy in ["strict", "merge", "ignore"] and drift_result["has_drift"]:
            handle_result = handle_drift(source_df, target_path, drift_strategy)
            result = {
                "entity_id": entity_id,
                "target_path": target_path,
                "strategy": drift_strategy,
                "status": handle_result["action"],
                "has_drift": drift_result["has_drift"],
                "drift_type": drift_result.get("drift_type"),
                "added_columns": drift_result["added_columns"],
                "removed_columns": drift_result["removed_columns"],
                "type_changes": drift_result["type_changes"],
                "message": drift_result["message"]
            }
        elif drift_strategy == "alert" and drift_result["has_drift"]:
            handle_result = handle_drift(source_df, target_path, drift_strategy)
            result = {
                "entity_id": entity_id,
                "target_path": target_path,
                "strategy": drift_strategy,
                "status": "ALERT",
                "has_drift": True,
                "drift_type": drift_result.get("drift_type"),
                "added_columns": drift_result["added_columns"],
                "removed_columns": drift_result["removed_columns"],
                "type_changes": drift_result["type_changes"],
                "message": drift_result["message"]
            }
        else:
            result = {
                "entity_id": entity_id,
                "target_path": target_path,
                "strategy": drift_strategy,
                "status": "CHECKED",
                "has_drift": drift_result["has_drift"],
                "drift_type": drift_result.get("drift_type"),
                "added_columns": drift_result["added_columns"],
                "removed_columns": drift_result["removed_columns"],
                "type_changes": drift_result["type_changes"],
                "message": drift_result["message"]
            }
    except Exception as e:
        result = {
            "entity_id": entity_id,
            "target_path": target_path,
            "strategy": drift_strategy,
            "status": "ERROR",
            "message": str(e)
        }

    mssparkutils.notebook.exit(json.dumps(result))

main()
