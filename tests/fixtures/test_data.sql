-- Test Fixture Data
-- Run this in fabric_control database to create test data for integration tests

-- Test Source
IF NOT EXISTS (SELECT 1 FROM cfg.Sources WHERE SourceName = 'test-source')
BEGIN
    INSERT INTO cfg.Sources (SourceName, SourceType, ConnectionStringRef, AuthenticationType, IsActive)
    VALUES ('test-source', 'SQL', 'kv-test-conn', 'ManagedIdentity', 1);
END;

-- Test Entity
IF NOT EXISTS (SELECT 1 FROM cfg.Entities WHERE EntityName = 'TestTable')
BEGIN
    INSERT INTO cfg.Entities (SourceId, EntityName, EntityType, SourceSchema, TargetLakehouse, TargetSchema, TargetTableName, LoadType, WatermarkColumn, WatermarkDataType, WatermarkOffset, ScheduleExpression, ParallelismDegree, Priority)
    VALUES ((SELECT SourceId FROM cfg.Sources WHERE SourceName = 'test-source'), 'TestTable', 'TABLE', 'dbo', 'lh_bronze', 'raw', 'test_table', 'FULL', NULL, NULL, NULL, '0 0 * * *', 1, 10);
END;

-- Test DQ Rules
IF NOT EXISTS (SELECT 1 FROM dq.Rules WHERE RuleName = 'Test Not Null' AND EntityId = (SELECT EntityId FROM cfg.Entities WHERE EntityName = 'TestTable'))
BEGIN
    INSERT INTO dq.Rules (EntityId, RuleName, RuleType, ColumnName, ExpectedValue, Severity, QuarantineEnabled)
    VALUES 
    ((SELECT EntityId FROM cfg.Entities WHERE EntityName = 'TestTable'), 'Test Not Null', 'NOT_NULL', 'ID', NULL, 'ERROR', 1),
    ((SELECT EntityId FROM cfg.Entities WHERE EntityName = 'TestTable'), 'Test Range', 'RANGE', 'Value', '0|100', 'WARNING', 0);
END;

-- Test Gold Model
IF NOT EXISTS (SELECT 1 FROM cfg.GoldModels WHERE ModelName = 'test_dim')
BEGIN
    INSERT INTO cfg.GoldModels (ModelName, ModelType, TargetWarehouse, TargetSchema, TargetTableName, SourceLakehouse, SourceSchema, SourceTables, GrainColumns, Priority)
    VALUES ('test_dim', 'DIMENSION', 'wh_gold', 'curated', 'test_dim', 'lh_silver', 'cleaned', '["test_table"]', 'ID', 99);
END;

PRINT 'Test fixtures created successfully';
