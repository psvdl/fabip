# Fabric SQL Execute Notebook - nb_sql_execute
# Generic SQL execution notebook to replace SqlServerStoredProcedure activities
# Uses PySpark JDBC with ActiveDirectoryMSI auth
# Returns status JSON via mssparkutils.notebook.exit()

# COMMAND ----------

# Fabric pipeline parameters are injected as notebook-scoped variables
control_sql_endpoint = globals().get("control_sql_endpoint", "")
control_database_name = globals().get("control_database_name", "fabric_control")
stored_procedure_name = globals().get("stored_procedure_name", "")
stored_procedure_params_json = globals().get("stored_procedure_params_json", "{}")
key_vault_url = globals().get("key_vault_url", "")

# Build JDBC URL for Fabric SQL Database
jdbc_url = (
    f"jdbc:sqlserver://{control_sql_endpoint}:1433;"
    f"database={control_database_name};"
    f"encrypt=true;"
    f"trustServerCertificate=false;"
    f"loginTimeout=30;"
)

# COMMAND ----------

import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Alias for downstream code
sql_endpoint = control_sql_endpoint
database_name = control_database_name
sp_name = stored_procedure_name
sp_params_json = stored_procedure_params_json

# COMMAND ----------

# Build the EXEC statement from stored procedure name and parameters
sp_params = json.loads(sp_params_json)

# Build parameter string for the EXEC call
param_list = []
for key, value in sp_params.items():
    if value is None:
        param_list.append(f"@{key}=NULL")
    elif isinstance(value, (int, float)):
        param_list.append(f"@{key}={value}")
    else:
        # Escape single quotes for SQL strings
        escaped = str(value).replace("'", "''")
        param_list.append(f"@{key}='{escaped}'")

if param_list:
    exec_sql = f"EXEC {sp_name} {', '.join(param_list)}"
else:
    exec_sql = f"EXEC {sp_name}"

# COMMAND ----------

# Execute stored procedure via PySpark JDBC
try:
    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("query", exec_sql)
        .option("authentication", "ActiveDirectoryMSI")
        .load()
    )

    # Collect any results (e.g., identity columns, status)
    results = [row.asDict() for row in df.collect()]

    exit_result = {
        "status": "SUCCEEDED",
        "storedProcedure": sp_name,
        "results": results
    }

    mssparkutils.notebook.exit(json.dumps(exit_result))

except Exception as e:
    error_result = {
        "status": "FAILED",
        "storedProcedure": sp_name,
        "error": str(e)
    }
    mssparkutils.notebook.exit(json.dumps(error_result))
