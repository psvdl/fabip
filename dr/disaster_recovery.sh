#!/bin/bash
# ============================================================================
# DISASTER RECOVERY SCRIPT
# Fabric DR procedures
# ============================================================================

set -euo pipefail

# Resolve required variables from environment or use placeholder defaults
CONTROL_DB_SERVER="${CONTROL_DB_SERVER:-sql-fabric-control.database.windows.net}"
CONTROL_DB_USER="${CONTROL_DB_USER:-fabric_admin}"
CONTROL_DB_PASSWORD="${CONTROL_DB_PASSWORD:-${FABRIC_CONTROL_DB_PASSWORD:-}}"
FABRIC_TOKEN="${FABRIC_TOKEN:-${FABRIC_API_TOKEN:-}}"

PRIMARY_WORKSPACE_ID="${PRIMARY_WORKSPACE_ID:-primary-workspace-id}"
DR_WORKSPACE_ID="${DR_WORKSPACE_ID:-dr-workspace-id}"
BACKUP_STORAGE="abfss://backup@storage.dfs.core.windows.net"
LOCAL_BACKUP_DIR="${LOCAL_BACKUP_DIR:-/tmp/fabric_dr_backups}"

echo "Starting Disaster Recovery procedures..."

# Validate required secrets are present
if [ -z "$CONTROL_DB_PASSWORD" ]; then
    echo "ERROR: CONTROL_DB_PASSWORD (or FABRIC_CONTROL_DB_PASSWORD) must be set."
    exit 1
fi
if [ -z "$FABRIC_TOKEN" ]; then
    echo "ERROR: FABRIC_TOKEN (or FABRIC_API_TOKEN) must be set."
    exit 1
fi

# 1. Backup critical tables from Gold layer
echo "Backing up Gold layer tables..."
spark-sql -e "
  CREATE TABLE IF NOT EXISTS delta.\`$BACKUP_STORAGE/gold_backup/dim_customers\`
  USING DELTA
  AS SELECT * FROM wh_gold.curated.dim_customers;

  CREATE TABLE IF NOT EXISTS delta.\`$BACKUP_STORAGE/gold_backup/fact_orders\`
  USING DELTA
  AS SELECT * FROM wh_gold.curated.fact_orders;
"

# 2. Export control database to LOCAL path first (abfss redirection from shell is invalid)
echo "Exporting control database..."
mkdir -p "$LOCAL_BACKUP_DIR"
LOCAL_SQL_FILE="$LOCAL_BACKUP_DIR/control_db_backup_$(date +%Y%m%d_%H%M%S).sql"

mssql-scripter -S "$CONTROL_DB_SERVER" -d fabric_control -U "$CONTROL_DB_USER" -P "$CONTROL_DB_PASSWORD" \
  --schema-and-data \
  > "$LOCAL_SQL_FILE"

echo "  Control DB exported to local path: $LOCAL_SQL_FILE"
echo "  Upload $LOCAL_SQL_FILE to $BACKUP_STORAGE/control_db_backup.sql via Azure Storage Explorer, AzCopy, or dbfs cp."

# 3. Verify DR workspace is accessible
echo "Verifying DR workspace..."
curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $FABRIC_TOKEN" \
  "https://api.fabric.microsoft.com/v1/workspaces/$DR_WORKSPACE_ID" | grep -q "200" && echo "  DR workspace is accessible." || echo "  WARNING: DR workspace check returned non-200."

echo "DR backup completed successfully!"
echo "Backup location: $BACKUP_STORAGE (Spark Delta tables)"
echo "Local SQL backup: $LOCAL_SQL_FILE"
