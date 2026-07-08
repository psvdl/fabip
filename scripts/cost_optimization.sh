#!/bin/bash
# ============================================================================
# COST OPTIMIZATION SCRIPT
# Analyzes and optimizes Fabric capacity usage
# ============================================================================

# Resolve required variables with sensible defaults
SUBSCRIPTION_ID="${SUBSCRIPTION_ID:-$(az account show --query id -o tsv)}"
RG="${RG:-rg-fabric-elt}"
CAPACITY_NAME="${CAPACITY_NAME:-fabric-elt-capacity}"

echo "Fabric Capacity Cost Analysis"
echo "=============================="
echo "Subscription: $SUBSCRIPTION_ID"
echo "Resource Group: $RG"
echo "Capacity: $CAPACITY_NAME"
echo ""

# Get capacity usage metrics (using Azure Monitor API)
az monitor metrics list \
  --resource "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RG/providers/Microsoft.Fabric/capacities/$CAPACITY_NAME" \
  --metric "FabricCapacityUtilization" \
  --interval PT1H \
  --aggregation Average Maximum \
  --output table

echo ""
echo "Recommendations:"
echo "1. If avg utilization < 30% for 7 days, consider downgrading capacity"
echo "2. If max utilization > 95% frequently, consider upgrading or optimizing pipelines"
echo "3. Use auto-pause for dev/test environments during off-hours"
echo "4. Schedule maintenance jobs during low-usage periods"
