# Fabric SQL Lookup Notebook - nb_sql_lookup
# Generic SQL lookup notebook to replace ADF Lookup activities
# Uses PySpark JDBC with ActiveDirectoryMSI auth
# Returns JSON results via mssparkutils.notebook.exit()

# COMMAND ----------

# Fabric pipeline parameters are injected as notebook-scoped variables
control_sql_endpoint = globals().get("control_sql_endpoint", "")
control_database_name = globals().get("control_database_name", "fabric_control")
query = globals().get("query", "")
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

# Execute query via PySpark JDBC with MSI authentication
try:
    df = (
        spark.read
        .format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", jdbc_url)
        .option("databaseName", control_database_name)
        .option("query", query)
        .option("authentication", "ActiveDirectoryMSI")
        .load()
    )

    # Collect results and convert to JSON
    results = [row.asDict() for row in df.collect()]

    # Return JSON array of results
    mssparkutils.notebook.exit(json.dumps(results))

except Exception as e:
    error_result = {
        "error": str(e),
        "status": "FAILED",
        "query": query
    }
    mssparkutils.notebook.exit(json.dumps(error_result))
