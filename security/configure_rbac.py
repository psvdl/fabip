# ============================================================================
# RBAC CONFIGURATION
# Assigns Fabric workspace roles using Azure AD Object IDs
# ============================================================================

import argparse
import json
import requests
import os
import sys

ROLE_ASSIGNMENTS = {
    "dev": {"data_engineers": ["user1@company.com"], "data_analysts": ["analyst1@company.com"], "admins": ["admin@company.com"]},
    "test": {"data_engineers": ["user1@company.com"], "data_analysts": ["analyst1@company.com"], "admins": ["admin@company.com"]},
    "prod": {"data_engineers": [], "data_analysts": ["analyst1@company.com"], "admins": ["admin@company.com"]}
}

FABRIC_ROLES = {"admins": "Admin", "data_engineers": "Contributor", "data_analysts": "Viewer"}

def resolve_user_to_object_id(user_email, token):
    url = f"https://graph.microsoft.com/v1.0/users?$filter=mail eq '{user_email}' or userPrincipalName eq '{user_email}'"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json().get("value", [])
        if data:
            return data[0].get("id")
    print(f"WARNING: Could not resolve Object ID for {user_email}")
    return None

def assign_role(workspace_id, user_email, role, token, graph_token):
    object_id = resolve_user_to_object_id(user_email, graph_token)
    if not object_id:
        print(f"SKIPPED: {user_email} - could not resolve Object ID")
        return False
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/roleAssignments"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"principal": {"id": object_id, "type": "User"}, "role": role}
    response = requests.post(url, headers=headers, json=body)
    if response.status_code in [200, 201]:
        print(f"Assigned {role} to {user_email} ({object_id})")
        return True
    else:
        print(f"Failed to assign {role} to {user_email}: {response.status_code} - {response.text}")
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', required=True, choices=['dev', 'test', 'prod'])
    parser.add_argument('--workspace-id', required=True)
    # FIXED: Token arguments are no longer strictly required (required=False).
    # Tokens are now read from environment variables as the secure default,
    # with CLI arguments available as an override for backward compatibility.
    parser.add_argument('--token', required=False, default=None, help="Fabric API token. Prefer FABRIC_TOKEN env var.")
    parser.add_argument('--graph-token', required=False, default=None, help="Microsoft Graph API token. Prefer GRAPH_TOKEN env var.")
    args = parser.parse_args()

    # FIXED: Read tokens from environment variables first, fall back to CLI args.
    # This prevents tokens from being exposed in process listings (ps, /proc, etc.)
    # and shell history while maintaining backward compatibility.
    token = args.token or os.environ.get('FABRIC_TOKEN')
    graph_token = args.graph_token or os.environ.get('GRAPH_TOKEN')

    if not token:
        print("ERROR: No Fabric token provided. Set FABRIC_TOKEN environment variable or pass --token.")
        sys.exit(1)
    if not graph_token:
        print("ERROR: No Graph token provided. Set GRAPH_TOKEN environment variable or pass --graph-token.")
        sys.exit(1)

    assignments = ROLE_ASSIGNMENTS.get(args.environment, {})
    for group, users in assignments.items():
        role = FABRIC_ROLES.get(group)
        if not role:
            continue
        for user in users:
            assign_role(args.workspace_id, user, role, token, graph_token)

if __name__ == "__main__":
    main()
