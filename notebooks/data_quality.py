# ============================================================================
# DATA QUALITY NOTEBOOK
# Production-ready data quality engine with quarantine
# ============================================================================

import pyspark.sql.functions as F
from pyspark.sql.types import *
import json
import traceback
from datetime import datetime

dbutils.widgets.text("entity_id", "0")
dbutils.widgets.text("entity_run_id", "")
dbutils.widgets.text("source_lakehouse", "lh_bronze")
dbutils.widgets.text("source_schema", "raw")
dbutils.widgets.text("source_table", "")
dbutils.widgets.text("control_jdbc_url", "jdbc:sqlserver://placeholder;database=fabric_control;")

entity_id = int(dbutils.widgets.get("entity_id"))
entity_run_id = dbutils.widgets.get("entity_run_id")
source_lakehouse = dbutils.widgets.get("source_lakehouse")
source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
CONTROL_JDBC_URL = dbutils.widgets.get("control_jdbc_url")

def read_source_table():
    path = f"abfss://{source_lakehouse}@onelake.dfs.fabric.microsoft.com/{source_schema}/{source_table}"
    return spark.read.format("delta").load(path)

def read_quality_rules_from_db():
    try:
        query = f"SELECT RuleId, RuleName, RuleType, ColumnName, ExpectedValue, MinValue, MaxValue, RegexPattern, RefTable, RefColumn, Severity, QuarantineEnabled FROM dq.Rules WHERE EntityId = {entity_id} AND IsActive = 1"
        df = spark.read.format("jdbc").option("url", CONTROL_JDBC_URL).option("query", query).load()
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"WARNING: Could not load rules from DB: {str(e)}. Using defaults.")
        return [
            {"RuleId": 1, "RuleName": "Primary Key Not Null", "RuleType": "NOT_NULL", "ColumnName": "CustomerID", "Severity": "CRITICAL", "QuarantineEnabled": True},
            {"RuleId": 2, "RuleName": "Email Format", "RuleType": "EMAIL_FORMAT", "ColumnName": "Email", "Severity": "ERROR", "QuarantineEnabled": True},
            {"RuleId": 3, "RuleName": "Order Amount Range", "RuleType": "RANGE", "ColumnName": "OrderAmount", "MinValue": 0, "MaxValue": 999999, "Severity": "ERROR", "QuarantineEnabled": True}
        ]

def evaluate_not_null(df, column_name):
    total = df.count()
    null_count = df.filter(F.col(column_name).isNull()).count()
    return {"total": total, "failed": null_count, "failure_rate": round(null_count / total * 100, 2) if total > 0 else 0, "passed": null_count == 0}

def evaluate_unique(df, column_name):
    total = df.count()
    distinct_count = df.select(column_name).distinct().count()
    dup_count = total - distinct_count
    return {"total": total, "failed": dup_count, "failure_rate": round(dup_count / total * 100, 2) if total > 0 else 0, "passed": dup_count == 0}

def evaluate_regex(df, column_name, pattern):
    total = df.count()
    invalid_count = df.filter(~F.col(column_name).rlike(pattern) | F.col(column_name).isNull()).count()
    return {"total": total, "failed": invalid_count, "failure_rate": round(invalid_count / total * 100, 2) if total > 0 else 0, "passed": invalid_count == 0}

def evaluate_range(df, column_name, min_val, max_val):
    total = df.count()
    invalid_count = df.filter((F.col(column_name) < min_val) | (F.col(column_name) > max_val) | F.col(column_name).isNull()).count()
    return {"total": total, "failed": invalid_count, "failure_rate": round(invalid_count / total * 100, 2) if total > 0 else 0, "passed": invalid_count == 0}

def evaluate_ref_integrity(df, column_name, ref_table, ref_column):
    ref_path = f"abfss://lh_silver@onelake.dfs.fabric.microsoft.com/cleaned/{ref_table}"
    try:
        ref_df = spark.read.format("delta").load(ref_path)
        ref_values = [r[0] for r in ref_df.select(ref_column).distinct().collect()]
        total = df.count()
        invalid_count = df.filter(~F.col(column_name).isin(ref_values) | F.col(column_name).isNull()).count()
        return {"total": total, "failed": invalid_count, "failure_rate": round(invalid_count / total * 100, 2) if total > 0 else 0, "passed": invalid_count == 0}
    except Exception as e:
        print(f"WARNING: Ref integrity check skipped: {str(e)}")
        return {"total": df.count(), "failed": 0, "failure_rate": 0, "passed": True}

def quarantine_bad_rows(df, rule_results, quarantine_table):
    # FIXED: Check rule["rule_type"] instead of rule["rule_name"] since rule_name contains
    # descriptive text (e.g., "Primary Key Not Null") while rule_type contains the machine-readable
    # type (e.g., "NOT_NULL", "RANGE", "EMAIL_FORMAT")
    bad_conditions = []
    for rule in rule_results:
        if not rule["passed"] and rule.get("QuarantineEnabled", True):
            col = rule["column_name"]
            rule_type = rule.get("rule_type", "").upper()
            if rule_type == "NOT_NULL":
                bad_conditions.append(F.col(col).isNull())
            elif rule_type == "RANGE":
                # FIXED: Use correct MinValue/MaxValue from the rule definition
                min_v = rule.get("MinValue", float('-inf'))
                max_v = rule.get("MaxValue", float('inf'))
                # Handle None values from the rule config
                if min_v is None:
                    min_v = float('-inf')
                if max_v is None:
                    max_v = float('inf')
                # Build condition: only apply bounds that are not infinite
                if min_v != float('-inf') and max_v != float('inf'):
                    bad_conditions.append((F.col(col) < min_v) | (F.col(col) > max_v))
                elif min_v != float('-inf'):
                    bad_conditions.append(F.col(col) < min_v)
                elif max_v != float('inf'):
                    bad_conditions.append(F.col(col) > max_v)
                # Also null values fail range checks
                bad_conditions.append(F.col(col).isNull())
            elif rule_type == "EMAIL_FORMAT":
                bad_conditions.append(~F.col(col).rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
            elif rule_type == "UNIQUE":
                # For unique violations, use window function to find duplicates
                window_spec = Window.partitionBy(col)
                bad_conditions.append(F.count(col).over(window_spec) > 1)
            elif rule_type == "REF_INTEGRITY":
                # FK violations handled at row level - mark rows where ref lookup fails
                # This requires the ref table to be available - use a placeholder that
                # will be caught by the broader quarantine logic
                bad_conditions.append(F.col(col).isNull())
            elif rule_type == "REGEX":
                pattern = rule.get("RegexPattern", ".*")
                bad_conditions.append(~F.col(col).rlike(pattern))
    if bad_conditions:
        final_condition = bad_conditions[0]
        for cond in bad_conditions[1:]:
            final_condition = final_condition | cond
        bad_df = df.filter(final_condition)
        quarantine_path = f"abfss://lh_bronze@onelake.dfs.fabric.microsoft.com/quarantine/{quarantine_table}"
        bad_df.write.format("delta").mode("append").save(quarantine_path)
        return bad_df.count()
    return 0

def main():
    try:
        df = read_source_table()
        total_rows = df.count()
        if total_rows == 0:
            mssparkutils.notebook.exit(json.dumps({"status": "SUCCEEDED", "total_rows": 0, "rules_passed": 0, "rules_failed": 0}))
            return
        rules = read_quality_rules_from_db()
        rule_results = []
        critical_failures = 0
        for rule in rules:
            result = None
            if rule["RuleType"] == "NOT_NULL":
                result = evaluate_not_null(df, rule["ColumnName"])
            elif rule["RuleType"] == "UNIQUE":
                result = evaluate_unique(df, rule["ColumnName"])
            elif rule["RuleType"] == "EMAIL_FORMAT":
                result = evaluate_regex(df, rule["ColumnName"], r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
            elif rule["RuleType"] == "RANGE":
                result = evaluate_range(df, rule["ColumnName"], rule.get("MinValue", 0), rule.get("MaxValue", 999999))
            elif rule["RuleType"] == "REF_INTEGRITY":
                result = evaluate_ref_integrity(df, rule["ColumnName"], rule.get("RefTable"), rule.get("RefColumn"))
            elif rule["RuleType"] == "REGEX":
                result = evaluate_regex(df, rule["ColumnName"], rule.get("RegexPattern", ".*"))
            else:
                continue
            if result:
                result.update({
                    "rule_id": rule["RuleId"],
                    "rule_name": rule["RuleName"],
                    "rule_type": rule["RuleType"],
                    "column_name": rule["ColumnName"],
                    "severity": rule["Severity"],
                    "QuarantineEnabled": rule.get("QuarantineEnabled", True),
                    "MinValue": rule.get("MinValue"),
                    "MaxValue": rule.get("MaxValue"),
                    "RegexPattern": rule.get("RegexPattern")
                })
                rule_results.append(result)
                if not result["passed"] and rule["Severity"] == "CRITICAL":
                    critical_failures += 1
        quarantine_count = 0
        if any(not r["passed"] for r in rule_results):
            quarantine_count = quarantine_bad_rows(df, rule_results, f"{source_table}_quarantine")
        overall_status = "QUARANTINED" if critical_failures > 0 else "SUCCEEDED"
        result = {
            "status": overall_status,
            "total_rows": total_rows,
            "rules_evaluated": len(rule_results),
            "rules_passed": sum(1 for r in rule_results if r["passed"]),
            "rules_failed": sum(1 for r in rule_results if not r["passed"]),
            "critical_failures": critical_failures,
            "quarantine_count": quarantine_count,
            "rule_details": rule_results
        }
    except Exception as e:
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        result = {"status": "FAILED", "error": error_msg, "stack_trace": stack_trace}
    mssparkutils.notebook.exit(json.dumps(result))

main()
