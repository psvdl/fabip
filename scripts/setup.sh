#!/bin/bash
###############################################################################
# Master Setup Script for Fabric ELT Framework
# Azure CLI / Bicep / PowerShell version (No Terraform)
###############################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENVIRONMENT="${1:-}"
ACTION="${2:-all}"

if [ -z "$ENVIRONMENT" ]; then
    echo "Usage: $0 <environment> [all|infra|sql|fabric|security]"
    echo "  environment: dev, test, prod"
    echo "  action: all (default), infra, sql, fabric, security"
    exit 1
fi

echo "========================================"
echo "Fabric ELT Framework Setup"
echo "Environment: $ENVIRONMENT"
echo "Action: $ACTION"
echo "========================================"

# Validate prerequisites
echo "[CHECK] Validating prerequisites..."
for tool in az python git jq; do
    if command -v "$tool" &> /dev/null; then
        echo "  ✓ $tool installed"
    else
        echo "  ✗ $tool not found. Please install it."
        exit 1
    fi
done

# Verify Azure login
if ! az account show &> /dev/null; then
    echo "  ✗ Not logged into Azure. Run: az login --use-device-code"
    exit 1
fi
echo "  ✓ Azure CLI logged in: $(az account show --query name -o tsv)"

deploy_infra() {
    echo ""
    echo "[DEPLOY] Azure Infrastructure..."
    echo "Choose deployment method:"
    echo "  1) Azure CLI (Bash script)"
    echo "  2) Bicep (az bicep)"
    echo "  3) ARM Template (az deployment)"
    read -p "Select method [1-3]: " method

    case $method in
        1)
            echo "Deploying via Azure CLI..."
            chmod +x "$SCRIPT_DIR/../infrastructure/azure-cli/deploy.sh"
            ENVIRONMENT=$ENVIRONMENT bash "$SCRIPT_DIR/../infrastructure/azure-cli/deploy.sh"
            ;;
        2)
            echo "Deploying via Bicep..."
            az bicep build --file "$SCRIPT_DIR/../infrastructure/bicep/main.bicep" --outfile "$SCRIPT_DIR/../infrastructure/bicep/main.json"
            az deployment group create                 --resource-group "rg-$ENVIRONMENT-fabric-elt-$(az account show --query id -o tsv | md5sum | cut -c1-3)"                 --template-file "$SCRIPT_DIR/../infrastructure/bicep/main.json"                 --parameters "$SCRIPT_DIR/../infrastructure/bicep/parameters.bicepparam"
            ;;
        3)
            echo "Deploying via ARM Template..."
            az deployment group create                 --resource-group "rg-$ENVIRONMENT-fabric-elt"                 --template-file "$SCRIPT_DIR/../infrastructure/arm-templates/main.json"                 --parameters "$SCRIPT_DIR/../infrastructure/arm-templates/parameters.json"
            ;;
        *)
            echo "Invalid selection"
            exit 1
            ;;
    esac
    echo "  ✓ Infrastructure deployed"
}

deploy_sql() {
    echo ""
    echo "[DEPLOY] Control Database Schema..."
    echo ""
    echo "  ℹ Execute the following SQL script against your Fabric SQL Database:"
    echo "     File: sql/control_database.sql"
    echo ""
    echo "  You can use:"
    echo "    - Azure Data Studio"
    echo "    - Fabric Portal Query Editor"
    echo "    - sqlcmd"
    echo ""
    read -p "Press Enter after executing the SQL script..."
    echo "  ✓ SQL schema deployed"
}

deploy_fabric() {
    echo ""
    echo "[DEPLOY] Fabric Items..."
    pip install -r "$SCRIPT_DIR/../fabric-cicd/requirements.txt"

    # Get workspace ID from config
    WORKSPACE_ID=$(python -c "import json; print(json.load(open('$SCRIPT_DIR/../fabric-cicd/workspace_config.json'))['workspaces']['$ENVIRONMENT']['id'])")

    # Get token
    TENANT_ID=$(az account show --query tenantId -o tsv)
    KEY_VAULT_NAME="${KEY_VAULT_NAME:-kv-fabric-elt}"
    SPN_CLIENT_ID=$(az keyvault secret show --name fabric-cicd-client-id --vault-name "$KEY_VAULT_NAME" --query value -o tsv 2>/dev/null || read -p "Enter SPN Client ID: " SPN_CLIENT_ID)
    SPN_CLIENT_SECRET=$(az keyvault secret show --name fabric-cicd-client-secret --vault-name "$KEY_VAULT_NAME" --query value -o tsv 2>/dev/null || read -s -p "Enter SPN Client Secret: " SPN_CLIENT_SECRET)

    TOKEN=$(curl -s -X POST -H "Content-Type: application/x-www-form-urlencoded"         -d "grant_type=client_credentials"         -d "client_id=$SPN_CLIENT_ID"         -d "client_secret=$SPN_CLIENT_SECRET"         -d "scope=https://api.fabric.microsoft.com/.default"         "https://login.microsoftonline.com/$TENANT_ID/oauth2/v2.0/token" | jq -r '.access_token')

    python "$SCRIPT_DIR/../fabric-cicd/deploy.py"         --environment "$ENVIRONMENT"         --workspace-id "$WORKSPACE_ID"         --repository-directory "$SCRIPT_DIR/.."         --token "$TOKEN"
    echo "  ✓ Fabric items deployed"
}

deploy_security() {
    echo ""
    echo "[DEPLOY] Security Configuration..."

    WORKSPACE_ID=$(python -c "import json; print(json.load(open('$SCRIPT_DIR/../fabric-cicd/workspace_config.json'))['workspaces']['$ENVIRONMENT']['id'])")
    TENANT_ID=$(az account show --query tenantId -o tsv)
    KEY_VAULT_NAME="${KEY_VAULT_NAME:-kv-fabric-elt}"
    SPN_CLIENT_ID=$(az keyvault secret show --name fabric-cicd-client-id --vault-name "$KEY_VAULT_NAME" --query value -o tsv 2>/dev/null || echo "")
    SPN_CLIENT_SECRET=$(az keyvault secret show --name fabric-cicd-client-secret --vault-name "$KEY_VAULT_NAME" --query value -o tsv 2>/dev/null || echo "")

    if [ -n "$SPN_CLIENT_ID" ] && [ -n "$SPN_CLIENT_SECRET" ]; then
        # Acquire Fabric API token for security configuration
        TOKEN=$(curl -s -X POST -H "Content-Type: application/x-www-form-urlencoded"             -d "grant_type=client_credentials"             -d "client_id=$SPN_CLIENT_ID"             -d "client_secret=$SPN_CLIENT_SECRET"             -d "scope=https://api.fabric.microsoft.com/.default"             "https://login.microsoftonline.com/$TENANT_ID/oauth2/v2.0/token" | jq -r '.access_token')

        # PowerShell security configuration (configure_security.ps1 expects -WorkspaceId, -Token, -Environment)
        if command -v pwsh &> /dev/null; then
            pwsh "$SCRIPT_DIR/../security/configure_security.ps1"                 -WorkspaceId "$WORKSPACE_ID"                 -Token "$TOKEN"                 -Environment "$ENVIRONMENT"
        else
            echo "  ⚠ PowerShell 7 not found. Run security/configure_security.ps1 manually."
        fi

        # RBAC configuration (TOKEN already acquired above)
        python "$SCRIPT_DIR/../security/configure_rbac.py"             --environment "$ENVIRONMENT"             --workspace-id "$WORKSPACE_ID"             --token "$TOKEN"
    else
        echo "  ⚠ SPN credentials not found. Run security scripts manually."
    fi
    echo "  ✓ Security configured"
}

# Execute based on action
case $ACTION in
    all)
        deploy_infra
        deploy_sql
        deploy_fabric
        deploy_security
        ;;
    infra)
        deploy_infra
        ;;
    sql)
        deploy_sql
        ;;
    fabric)
        deploy_fabric
        ;;
    security)
        deploy_security
        ;;
    *)
        echo "Invalid action: $ACTION"
        echo "Valid actions: all, infra, sql, fabric, security"
        exit 1
        ;;
esac

echo ""
echo "========================================"
echo "Setup Complete"
echo "========================================"
