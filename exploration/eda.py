# COMMAND ----------
from datetime import datetime, timedelta

import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import polars as pl
import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, lit, sum, when

spark = DatabricksSession.builder.getOrCreate()
# print(f"host from dbconnect: {spark.conf.get('spark.databricks.workspaceUrl')}")
# print(f"cluster id: {spark.conf.get('spark.databricks.clusterUsageTags.clusterId')}")
# w = WorkspaceClient(profile="dev")
# print(f"host from sdk client: {w.config.host}")

# spark.table("samples.nyctaxi.trips").show(10, False)

# COMMAND ----------
tables = spark.catalog.listTables("bu1_dev.analytics")
for table in tables:
    print(table.name)

# COMMAND ----------
spark.table("bu1_dev.analytics.uber_pickups").count()

# COMMAND ----------
spark.table("bu1_dev.analytics.uber_pickups").show(10, False)

# COMMAND ----------
spark.sql(
    "grant all privileges on catalog bu1_dev to `aec9002d-d47a-431a-8886-4c8a40baa9c1`"
)

# COMMAND ----------
