-- ============================================================================
-- FABRIC ELT CONTROL DATABASE SCHEMA
-- Compatible with Microsoft Fabric SQL Database (no GO batch separators)
-- ============================================================================

-- Create schemas if they don't exist
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'cfg')
    EXEC('CREATE SCHEMA cfg');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
    EXEC('CREATE SCHEMA audit');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dq')
    EXEC('CREATE SCHEMA dq');

-- ============================================================================
-- CFG: Configuration Tables
-- ============================================================================

CREATE TABLE cfg.Sources (
    SourceId INT IDENTITY(1,1) PRIMARY KEY,
    SourceName NVARCHAR(128) NOT NULL,
    SourceType NVARCHAR(50) NOT NULL, -- 'SQL', 'API', 'FILE', 'EVENTHUB', 'COSMOS', 'SNOWFLAKE'
    ConnectionStringRef NVARCHAR(256) NOT NULL, -- Key Vault secret name reference
    AuthenticationType NVARCHAR(50) NOT NULL DEFAULT 'ManagedIdentity', -- 'ManagedIdentity', 'ServicePrincipal', 'OAuth2', 'SQLAuth'
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CreatedBy NVARCHAR(128) NOT NULL DEFAULT SYSTEM_USER,
    ModifiedBy NVARCHAR(128) NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT UQ_Sources_SourceName UNIQUE (SourceName)
);

CREATE TABLE cfg.Entities (
    EntityId INT IDENTITY(1,1) PRIMARY KEY,
    SourceId INT NOT NULL FOREIGN KEY REFERENCES cfg.Sources(SourceId),
    EntityName NVARCHAR(128) NOT NULL, -- Source object name
    EntityType NVARCHAR(50) NOT NULL DEFAULT 'TABLE', -- 'TABLE', 'VIEW', 'FILE', 'API_ENDPOINT', 'TOPIC'
    SourceSchema NVARCHAR(128) NULL,
    TargetLakehouse NVARCHAR(128) NOT NULL,
    TargetSchema NVARCHAR(128) NOT NULL,
    TargetTableName NVARCHAR(128) NOT NULL,
    LoadType NVARCHAR(50) NOT NULL DEFAULT 'INCREMENTAL', -- 'FULL', 'INCREMENTAL', 'CDC', 'STREAMING'
    WatermarkColumn NVARCHAR(128) NULL, -- Column for incremental load (timestamp, ID, etc.)
    WatermarkDataType NVARCHAR(50) NULL DEFAULT 'DATETIME', -- 'DATETIME', 'INT', 'BIGINT', 'ROWVERSION'
    WatermarkOffset NVARCHAR(256) NULL DEFAULT '1900-01-01', -- Initial watermark value
    SourceFilterClause NVARCHAR(MAX) NULL,
    ScheduleExpression NVARCHAR(128) NULL DEFAULT '0 2 * * *', -- Cron expression
    ParallelismDegree INT NOT NULL DEFAULT 2, -- Max concurrent tasks for this entity
    Priority INT NOT NULL DEFAULT 5, -- 1=Critical, 10=Low
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CreatedBy NVARCHAR(128) NOT NULL DEFAULT SYSTEM_USER,
    ModifiedBy NVARCHAR(128) NOT NULL DEFAULT SYSTEM_USER,
    CONSTRAINT UQ_Entities_EntityName_SourceId UNIQUE (EntityName, SourceId)
);

CREATE TABLE cfg.Transformations (
    TransformationId INT IDENTITY(1,1) PRIMARY KEY,
    EntityId INT NOT NULL FOREIGN KEY REFERENCES cfg.Entities(EntityId),
    TransformationType NVARCHAR(50) NOT NULL DEFAULT 'STANDARDIZATION',  -- 'DEDUPLICATION', 'STANDARDIZATION', 'ENRICHMENT', 'AGGREGATION', 'SCD2'
    TransformationLogicJson NVARCHAR(MAX) NULL,  -- JSON or SQL logic definition
    BusinessKeyColumns NVARCHAR(256) NULL, -- Comma-separated for SCD2/dedup
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE cfg.GoldModels (
    ModelId INT IDENTITY(1,1) PRIMARY KEY,
    ModelName NVARCHAR(128) NOT NULL,
    ModelType NVARCHAR(50) NOT NULL DEFAULT 'DIMENSION',
    TargetWarehouse NVARCHAR(128) NOT NULL,
    TargetSchema NVARCHAR(128) NOT NULL,
    TargetTableName NVARCHAR(128) NOT NULL,
    SourceLakehouse NVARCHAR(128) NOT NULL,
    SourceSchema NVARCHAR(128) NOT NULL,
    SourceTables NVARCHAR(MAX) NOT NULL,
    GrainColumns NVARCHAR(256) NULL,
    CalculatedColumns NVARCHAR(MAX) NULL,
    DependencyModels NVARCHAR(MAX) NULL,
    Priority INT NOT NULL DEFAULT 5,
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_GoldModels_ModelName UNIQUE (ModelName)
);

-- ============================================================================
-- AUDIT: Observability Tables
-- ============================================================================

CREATE TABLE audit.PipelineRuns (
    RunId UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    PipelineName NVARCHAR(128) NOT NULL,
    TriggerType NVARCHAR(50) NULL,  -- 'SCHEDULED', 'MANUAL', 'EVENT'
    TriggerId NVARCHAR(128) NULL,
    WorkspaceId NVARCHAR(128) NULL,
    ParametersJson NVARCHAR(MAX) NULL,
    Status NVARCHAR(50) NOT NULL DEFAULT 'RUNNING',  -- 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'QUARANTINED'
    StartTime DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    EndTime DATETIME2 NULL,
    ErrorMessage NVARCHAR(MAX) NULL,
    RowsRead BIGINT NULL DEFAULT 0,
    RowsWritten BIGINT NULL DEFAULT 0,
    CUConsumed DECIMAL(18,4) NULL,
    SparkApplicationId NVARCHAR(128) NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE audit.EntityRuns (
    EntityRunId UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
    RunId UNIQUEIDENTIFIER NOT NULL FOREIGN KEY REFERENCES audit.PipelineRuns(RunId),
    EntityId INT NOT NULL FOREIGN KEY REFERENCES cfg.Entities(EntityId),
    EntityName NVARCHAR(128) NOT NULL,
    Status NVARCHAR(50) NOT NULL DEFAULT 'RUNNING',
    StartTime DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    EndTime DATETIME2 NULL,
    ErrorMessage NVARCHAR(MAX) NULL,
    RowsRead BIGINT NULL DEFAULT 0,
    RowsWritten BIGINT NULL DEFAULT 0,
    RowsRejected BIGINT NULL DEFAULT 0,
    WatermarkBefore NVARCHAR(256) NULL,
    WatermarkAfter NVARCHAR(256) NULL,
    CUConsumed DECIMAL(18,4) NULL,
    SparkApplicationId NVARCHAR(128) NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE audit.GoldModelRuns (
    GoldRunId UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
    RunId UNIQUEIDENTIFIER NOT NULL FOREIGN KEY REFERENCES audit.PipelineRuns(RunId),
    ModelId INT NOT NULL FOREIGN KEY REFERENCES cfg.GoldModels(ModelId),
    ModelName NVARCHAR(128) NOT NULL,
    ModelType NVARCHAR(50) NOT NULL,
    Status NVARCHAR(50) NOT NULL DEFAULT 'RUNNING',
    StartTime DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    EndTime DATETIME2 NULL,
    ErrorMessage NVARCHAR(MAX) NULL,
    RowsRead BIGINT NULL DEFAULT 0,
    RowsWritten BIGINT NULL DEFAULT 0,
    CUConsumed DECIMAL(18,4) NULL,
    SparkApplicationId NVARCHAR(128) NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

-- ============================================================================
-- DQ: Data Quality Tables
-- ============================================================================

CREATE TABLE dq.Rules (
    RuleId INT IDENTITY(1,1) PRIMARY KEY,
    EntityId INT NOT NULL FOREIGN KEY REFERENCES cfg.Entities(EntityId),
    RuleName NVARCHAR(128) NOT NULL,
    RuleType NVARCHAR(50) NOT NULL, -- 'NOT_NULL', 'UNIQUE', 'RANGE', 'REGEX', 'REF_INTEGRITY', 'CUSTOM'
    ColumnName NVARCHAR(128) NOT NULL,
    ExpectedValue NVARCHAR(256) NULL, -- For RANGE: min|max, for REGEX: patter
    MinValue DECIMAL(18,4) NULL,
    MaxValue DECIMAL(18,4) NULL,
    RegexPattern NVARCHAR(256) NULL,
    RefTable NVARCHAR(128) NULL,
    RefColumn NVARCHAR(128) NULL,
    Severity NVARCHAR(20) NOT NULL DEFAULT 'ERROR',  -- 'WARNING', 'ERROR', 'CRITICAL'
    QuarantineEnabled BIT NOT NULL DEFAULT 1,
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dq.Results (
    ResultId BIGINT IDENTITY(1,1) PRIMARY KEY,
    EntityRunId UNIQUEIDENTIFIER NOT NULL FOREIGN KEY REFERENCES audit.EntityRuns(EntityRunId),
    RuleId INT NOT NULL FOREIGN KEY REFERENCES dq.Rules(RuleId),
    TotalRows BIGINT NOT NULL,
    FailedRows BIGINT NOT NULL,
    FailureRate DECIMAL(5,2) NOT NULL,
    Passed BIT NOT NULL,
    EvaluatedTime DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT CHK_dq_Results_NoDivZero CHECK (TotalRows <> 0)
);

-- ============================================================================
-- STORED PROCEDURES
-- ============================================================================

CREATE OR ALTER PROCEDURE audit.usp_LogPipelineStart
    @RunId UNIQUEIDENTIFIER,
    @PipelineName NVARCHAR(128),
    @TriggerType NVARCHAR(50) = NULL,
    @TriggerId NVARCHAR(128) = NULL,
    @WorkspaceId NVARCHAR(128) = NULL,
    @ParametersJson NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM audit.PipelineRuns WHERE RunId = @RunId)
    BEGIN
        INSERT INTO audit.PipelineRuns (RunId, PipelineName, TriggerType, TriggerId, WorkspaceId, ParametersJson, Status, StartTime)
        VALUES (@RunId, @PipelineName, @TriggerType, @TriggerId, @WorkspaceId, @ParametersJson, 'RUNNING', SYSUTCDATETIME());
    END
END;

CREATE OR ALTER PROCEDURE audit.usp_LogPipelineEnd
    @RunId UNIQUEIDENTIFIER,
    @Status NVARCHAR(50),
    @ErrorMessage NVARCHAR(MAX) = NULL,
    @RowsRead BIGINT = NULL,
    @RowsWritten BIGINT = NULL,
    @CUConsumed DECIMAL(18,4) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE audit.PipelineRuns
    SET Status = @Status,
        EndTime = SYSUTCDATETIME(),
        ErrorMessage = @ErrorMessage,
        RowsRead = ISNULL(@RowsRead, RowsRead),
        RowsWritten = ISNULL(@RowsWritten, RowsWritten),
        CUConsumed = ISNULL(@CUConsumed, CUConsumed)
    WHERE RunId = @RunId;
END;

CREATE OR ALTER PROCEDURE audit.usp_LogEntityStart
    @RunId UNIQUEIDENTIFIER,
    @EntityId INT,
    @EntityName NVARCHAR(128),
    @WatermarkBefore NVARCHAR(256) = NULL,
    @EntityRunId UNIQUEIDENTIFIER = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @EntityRunId = NEWID();
    INSERT INTO audit.EntityRuns (EntityRunId, RunId, EntityId, EntityName, Status, StartTime, WatermarkBefore)
    VALUES (@EntityRunId, @RunId, @EntityId, @EntityName, 'RUNNING', SYSUTCDATETIME(), @WatermarkBefore);
    SELECT @EntityRunId AS EntityRunId;
END;

CREATE OR ALTER PROCEDURE audit.usp_LogEntityEnd
    @EntityRunId UNIQUEIDENTIFIER,
    @Status NVARCHAR(50),
    @ErrorMessage NVARCHAR(MAX) = NULL,
    @RowsRead BIGINT = NULL,
    @RowsWritten BIGINT = NULL,
    @RowsRejected BIGINT = NULL,
    @WatermarkAfter NVARCHAR(256) = NULL,
    @CUConsumed DECIMAL(18,4) = NULL,
    @SparkApplicationId NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE audit.EntityRuns
    SET Status = @Status,
        EndTime = SYSUTCDATETIME(),
        ErrorMessage = @ErrorMessage,
        RowsRead = ISNULL(@RowsRead, RowsRead),
        RowsWritten = ISNULL(@RowsWritten, RowsWritten),
        RowsRejected = ISNULL(@RowsRejected, RowsRejected),
        WatermarkAfter = @WatermarkAfter,
        CUConsumed = ISNULL(@CUConsumed, CUConsumed),
        SparkApplicationId = @SparkApplicationId
    WHERE EntityRunId = @EntityRunId;
END;

CREATE OR ALTER PROCEDURE audit.usp_LogGoldModelStart
    @RunId UNIQUEIDENTIFIER,
    @ModelId INT,
    @ModelName NVARCHAR(128),
    @ModelType NVARCHAR(50),
    @SourceTables NVARCHAR(MAX) = NULL,
    @GoldRunId UNIQUEIDENTIFIER = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @GoldRunId = NEWID();
    INSERT INTO audit.GoldModelRuns (GoldRunId, RunId, ModelId, ModelName, ModelType, Status, StartTime, RowsRead, RowsWritten)
    VALUES (@GoldRunId, @RunId, @ModelId, @ModelName, @ModelType, 'RUNNING', SYSUTCDATETIME(), 0, 0);
    SELECT @GoldRunId AS GoldRunId;
END;

CREATE OR ALTER PROCEDURE audit.usp_LogGoldModelEnd
    @GoldRunId UNIQUEIDENTIFIER,
    @Status NVARCHAR(50),
    @ErrorMessage NVARCHAR(MAX) = NULL,
    @RowsRead BIGINT = NULL,
    @RowsWritten BIGINT = NULL,
    @CUConsumed DECIMAL(18,4) = NULL,
    @SparkApplicationId NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE audit.GoldModelRuns
    SET Status = @Status,
        EndTime = SYSUTCDATETIME(),
        ErrorMessage = @ErrorMessage,
        RowsRead = ISNULL(@RowsRead, RowsRead),
        RowsWritten = ISNULL(@RowsWritten, RowsWritten),
        CUConsumed = ISNULL(@CUConsumed, CUConsumed),
        SparkApplicationId = @SparkApplicationId
    WHERE GoldRunId = @GoldRunId;
END;

CREATE OR ALTER PROCEDURE cfg.usp_GetEntitiesToProcess
    @PipelineName NVARCHAR(128),
    @MaxParallelism INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    SELECT e.EntityId,
           e.EntityName,
           s.SourceName,
           s.SourceType,
           e.SourceSchema,
           e.TargetLakehouse,
           e.TargetSchema,
           e.TargetTableName,
           e.LoadType,
           e.WatermarkColumn,
           e.WatermarkDataType,
           ISNULL(er.WatermarkAfter, e.WatermarkOffset) AS LastWatermark,
           s.ConnectionStringRef,
           e.SourceFilterClause,
           s.AuthenticationType,
           e.ParallelismDegree,
           e.Priority
    FROM cfg.Entities e
    INNER JOIN cfg.Sources s ON e.SourceId = s.SourceId
    OUTER APPLY (
        SELECT TOP 1 WatermarkAfter
        FROM audit.EntityRuns
        WHERE EntityId = e.EntityId AND Status = 'SUCCEEDED'
        ORDER BY EndTime DESC
    ) er
    WHERE e.IsActive = 1
      AND s.IsActive = 1
    ORDER BY e.Priority ASC, e.EntityId ASC;
END;

CREATE OR ALTER PROCEDURE cfg.usp_GetGoldModelsToProcess
    @ModelId INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SELECT ModelId,
           ModelName,
           ModelType,
           TargetWarehouse,
           TargetSchema,
           TargetTableName,
           SourceLakehouse,
           SourceSchema,
           SourceTables,
           GrainColumns,
           CalculatedColumns,
           Priority,
           DependencyModels
    FROM cfg.GoldModels
    WHERE IsActive = 1
      AND (@ModelId IS NULL OR ModelId = @ModelId)
    ORDER BY Priority ASC;
END;

-- ============================================================================
-- VIEWS
-- ============================================================================

CREATE OR ALTER VIEW audit.vw_PipelineRunSummary
AS
SELECT RunId,
       PipelineName,
       Status,
       StartTime,
       EndTime,
       DATEDIFF(SECOND, StartTime, ISNULL(EndTime, SYSUTCDATETIME())) AS DurationSeconds,
       RowsRead,
       RowsWritten,
       ErrorMessage
FROM audit.PipelineRuns;

CREATE OR ALTER VIEW audit.vw_GoldModelRunSummary
AS
SELECT g.GoldRunId,
       g.RunId,
       g.ModelName,
       g.ModelType,
       g.Status,
       g.StartTime,
       g.EndTime,
       DATEDIFF(SECOND, g.StartTime, ISNULL(g.EndTime, SYSUTCDATETIME())) AS DurationSeconds,
       g.RowsRead,
       g.RowsWritten,
       g.ErrorMessage
FROM audit.GoldModelRuns g;

-- ============================================================================
-- INDEXES
-- ============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_EntityRuns_RunId' AND object_id = OBJECT_ID('audit.EntityRuns'))
    CREATE NONCLUSTERED INDEX IX_EntityRuns_RunId ON audit.EntityRuns(RunId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_EntityRuns_EntityId' AND object_id = OBJECT_ID('audit.EntityRuns'))
    CREATE NONCLUSTERED INDEX IX_EntityRuns_EntityId ON audit.EntityRuns(EntityId, StartTime DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_GoldModelRuns_RunId' AND object_id = OBJECT_ID('audit.GoldModelRuns'))
    CREATE NONCLUSTERED INDEX IX_GoldModelRuns_RunId ON audit.GoldModelRuns(RunId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_GoldModelRuns_ModelId' AND object_id = OBJECT_ID('audit.GoldModelRuns'))
    CREATE NONCLUSTERED INDEX IX_GoldModelRuns_ModelId ON audit.GoldModelRuns(ModelId, StartTime DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dq_Results_EntityRunId' AND object_id = OBJECT_ID('dq.Results'))
    CREATE NONCLUSTERED INDEX IX_dq_Results_EntityRunId ON dq.Results(EntityRunId);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dq_Results_RuleId' AND object_id = OBJECT_ID('dq.Results'))
    CREATE NONCLUSTERED INDEX IX_dq_Results_RuleId ON dq.Results(RuleId);

-- ============================================================================
-- SAMPLE DATA (idempotent)
-- ============================================================================

--IF NOT EXISTS (SELECT 1 FROM cfg.Sources WHERE SourceName = 'azsql-customers')
--BEGIN
    INSERT INTO cfg.Sources (SourceName, SourceType, ConnectionStringRef, AuthenticationType, IsActive)
    VALUES 
    ('azsql-customers', 'SQL', 'kv-azsql-customers-conn', 'ManagedIdentity', 1),
    ('api-sales', 'API', 'kv-api-sales-baseurl', 'OAuth2', 1),
    ('adls-logs', 'FILE', 'kv-adls-logs-conn', 'ManagedIdentity', 1),
    ('cosmos-products', 'COSMOS', 'kv-cosmos-products-conn', 'ManagedIdentity', 1);
--END;

--IF NOT EXISTS (SELECT 1 FROM cfg.Entities WHERE EntityName = 'customers')
--BEGIN
    INSERT INTO cfg.Entities (SourceId, EntityName, EntityType, SourceSchema, TargetLakehouse, TargetSchema, TargetTableName, 
    LoadType, WatermarkColumn, WatermarkDataType, WatermarkOffset, ScheduleExpression, ParallelismDegree, Priority)
    VALUES
    (1, 'Customers', 'TABLE', 'dbo', 'lh_bronze', 'raw', 'customers', 'INCREMENTAL', 'ModifiedDate', 'DATETIME', '1900-01-01', '0 2 * * *', 2, 1),
    (1, 'Orders', 'TABLE', 'dbo', 'lh_bronze', 'raw', 'orders', 'INCREMENTAL', 'OrderDate', 'DATETIME', '1900-01-01', '0 2 * * *', 2, 1),
    (2, 'sales-transactions', 'API_ENDPOINT', NULL, 'lh_bronze', 'raw', 'sales_transactions', 'FULL', NULL, NULL, NULL, '0 3 * * *', 1, 2),
    (3, 'web-logs', 'FILE', 'logs', 'lh_bronze', 'raw', 'web_logs', 'INCREMENTAL', 'LastModified', 'DATETIME', '1900-01-01', '0 1 * * *', 3, 3),
    (4, 'Products', 'TABLE', 'productdb', 'lh_bronze', 'raw', 'products', 'CDC', NULL, NULL, NULL, '0 */4 * * *', 1, 2);
--END;

-- Sample Transformations
INSERT INTO cfg.Transformations (EntityId, TransformationType, TransformationLogic, BusinessKeyColumns)
VALUES
(1, 'STANDARDIZATION', '{"dedup_key": "CustomerID", "standardize": {"Email": "lower", "Phone": "regex"}, "null_defaults": {"Country": "Unknown"}}', 'CustomerID'),
(1, 'SCD2', '{"scd2_columns": ["Email", "Phone", "Address"], "effective_date": "ValidFrom", "expiry_date": "ValidTo", "is_current": "IsCurrent"}', 'CustomerID'),
(2, 'STANDARDIZATION', '{"dedup_key": "OrderID", "foreign_keys": [{"column": "CustomerID", "ref_table": "customers", "ref_column": "CustomerID"}]}', 'OrderID'),
(3, 'STANDARDIZATION', '{"json_parse": "LogData", "timestamp_extract": "LogTimestamp", "ip_geolocation": true}', 'LogId');

 -- Sample Data Quality Rules
INSERT INTO dq.Rules (EntityId, RuleName, RuleType, ColumnName, ExpectedValue, Severity, QuarantineEnabled)
VALUES
(1, 'CustomerID Not Null', 'NOT_NULL', 'CustomerID', NULL, 'CRITICAL', 1),
(1, 'Email Valid Format', 'REGEX', 'Email', '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', 'ERROR', 1),
(1, 'Email Unique', 'UNIQUE', 'Email', NULL, 'ERROR', 1),
(2, 'OrderAmount Positive', 'RANGE', 'OrderAmount', '0|999999999.99', 'ERROR', 1),
(2, 'CustomerID FK Check', 'REF_INTEGRITY', 'CustomerID', 'customers.CustomerID', 'CRITICAL', 1),
(3, 'LogTimestamp Not Null', 'NOT_NULL', 'LogTimestamp', NULL, 'WARNING', 0);

-- Sample Gold Models
IF NOT EXISTS (SELECT 1 FROM cfg.GoldModels WHERE ModelName = 'dim_customers')
BEGIN
    INSERT INTO cfg.GoldModels (ModelName, ModelType, TargetWarehouse, TargetSchema, TargetTableName, SourceLakehouse, SourceSchema, SourceTables, GrainColumns, CalculatedColumns, Priority)
    VALUES ('dim_customers', 'DIMENSION', 'wh_gold', 'curated', 'dim_customers', 'lh_silver', 'cleaned', '["customers"]', 'CustomerID', NULL, 1);
END;

IF NOT EXISTS (SELECT 1 FROM cfg.GoldModels WHERE ModelName = 'fact_orders')
BEGIN
    INSERT INTO cfg.GoldModels (ModelName, ModelType, TargetWarehouse, TargetSchema, TargetTableName, SourceLakehouse, SourceSchema, SourceTables, GrainColumns, CalculatedColumns, Priority, DependencyModels)
    VALUES ('fact_orders', 'FACT', 'wh_gold', 'curated', 'fact_orders', 'lh_silver', 'cleaned', '["orders","dim_customers"]', 'OrderID', '{"OrderTotal": "UnitPrice * Quantity - Discount"}', 2, '["dim_customers"]');
END;

IF NOT EXISTS (SELECT 1 FROM dq.Rules WHERE RuleName = 'Primary Key Not Null' AND EntityId = (SELECT EntityId FROM cfg.Entities WHERE EntityName = 'customers'))
BEGIN
    INSERT INTO dq.Rules (EntityId, RuleName, RuleType, ColumnName, ExpectedValue, Severity, QuarantineEnabled)
    VALUES (
        (SELECT EntityId FROM cfg.Entities WHERE EntityName = 'customers'),
        'Primary Key Not Null', 'NOT_NULL', 'CustomerID', NULL, 'CRITICAL', 1
    );
END;

PRINT 'Control Database Schema Created Successfully';
