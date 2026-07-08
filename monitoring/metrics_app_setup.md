# Metrics App Setup

## Real-Time Monitoring Dashboard

### 1. Create Real-Time Dashboard

1. In Fabric Portal, go to **Real-Time Dashboards**
2. Create new dashboard: `ELT-Observability`
3. Add tiles using KQL queries from `monitoring/kql_queries.kql`

### 2. Key Metrics to Track

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| Pipeline Success Rate | > 99% | < 95% |
| Avg Pipeline Duration | < 30 min | > 60 min |
| Data Quality Score | > 98% | < 95% |
| Capacity Utilization | < 80% | > 95% |
| Failed Entities/Day | 0 | > 5 |

### 3. KQL Query Examples

```kql
// Pipeline success rate over time
PipelineRuns
| summarize SuccessRate = countif(Status == "SUCCEEDED") * 100.0 / count() by bin(StartTime, 1h)
| render timechart

// Top failing entities
EntityRuns
| where Status == "FAILED"
| summarize FailureCount = count() by EntityName
| top 10 by FailureCount
| render barchart

// Capacity utilization
AzureMetrics
| where MetricName == "FabricCapacityUtilization"
| summarize AvgCU = avg(Maximum) by bin(TimeGenerated, 5m)
| render timechart
```

### 4. Integration with Azure Monitor

1. Export Fabric logs to Log Analytics workspace
2. Import `monitoring/azure_monitor_alerts.json`
3. Configure action groups for email/Teams notifications
