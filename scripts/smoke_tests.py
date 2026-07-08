# ============================================================================
# SMOKE TESTS
# Post-deployment validation for Fabric ELT Framework
# ============================================================================

import argparse
import json
import sys
import os
import requests

def test_workspace_access(workspace_id, token):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.status_code == 200

def test_notebooks_exist(workspace_id, token):
    required_notebooks = ["nb_bronze_ingestion", "nb_silver_transform", "nb_gold_curate", "nb_data_quality", "nb_lakehouse_maintenance", "nb_schema_drift_detection", "nb_data_profiling"]
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return False
    existing = {item["displayName"] for item in response.json().get("value", [])}
    return all(nb in existing for nb in required_notebooks)

def test_pipelines_exist(workspace_id, token):
    required_pipelines = ["pl_master_orchestrator", "pl_gold_orchestrator"]
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/pipelines"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return False
    existing = {item["displayName"] for item in response.json().get("value", [])}
    return all(pl in existing for pl in required_pipelines)

def test_lakehouse_access(workspace_id, token):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    return response.status_code == 200

def test_control_database(connection_string):
    try:
        query = "SELECT 1 as Test"
        df = spark.read.format("jdbc").option("url", connection_string).option("query", query).load()
        return df.count() == 1
    except Exception as e:
        print(f"Control DB test failed: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', required=True)
    parser.add_argument('--workspace-id', required=True)
    parser.add_argument('--token', required=True)
    parser.add_argument('--control-jdbc-url', default=None)
    args = parser.parse_args()
    token = args.token
    if not token:
        print("ERROR: No token provided")
        sys.exit(1)
    results = []
    results.append(("Workspace Access", test_workspace_access(args.workspace_id, token)))
    results.append(("Notebooks", test_notebooks_exist(args.workspace_id, token)))
    results.append(("Pipelines", test_pipelines_exist(args.workspace_id, token)))
    results.append(("Lakehouses", test_lakehouse_access(args.workspace_id, token)))
    if args.control_jdbc_url:
        results.append(("Control DB", test_control_database(args.control_jdbc_url)))
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"{status}: {name}")
    all_passed = all(r[1] for r in results)
    sys.exit(0 if all_passed else 1)

if __name__ == "__main__":
    main()
