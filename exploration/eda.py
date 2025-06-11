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


def create_spark_session():
    try:
        spark = DatabricksSession.builder.serverless().getOrCreate()
        return spark
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        raise


spark = create_spark_session()
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
spark.sql("grant all privileges on catalog bu1_dev to `<add_uuid_here>`")

# COMMAND ----------
