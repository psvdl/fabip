# ============================================================================
# SECURITY CONFIGURATION (PowerShell)
# Configures Fabric workspace security settings
# ============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory=$true)]
    [string]$Token,

    [Parameter(Mandatory=$true)]
    [ValidateSet("dev","test","prod")]
    [string]$Environment
)

$headers = @{
    "Authorization" = "Bearer $Token"
    "Content-Type" = "application/json"
}

# 1. Configure workspace settings
$workspaceSettings = @{
    workspaceId = $WorkspaceId
    settings = @{
        # Disable external file sharing in production
        externalFileSharingEnabled = ($Environment -ne "prod")
        # Enable sensitivity labels
        sensitivityLabelsEnabled = $true
        # Restrict allowed domains
        allowedDomains = @("company.com", "subsidiary.com")
    }
}

# Apply workspace settings (Fabric API)
$uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId"
Invoke-RestMethod -Uri $uri -Method Patch -Headers $headers -Body ($workspaceSettings | ConvertTo-Json -Depth 10)

Write-Host "Security configuration applied for $Environment environment"
Write-Host "Workspace: $WorkspaceId"
Write-Host "External sharing: $(if ($Environment -eq 'prod') { 'DISABLED' } else { 'ENABLED' })"
