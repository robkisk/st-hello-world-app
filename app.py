# COMMAND ----------
import os
import streamlit as st
import pandas as pd
import numpy as np

from databricks.sdk.core import Config
from pyspark.sql import DataFrame
import polars as pl
from utils import get_spark_session

# COMMAND ----------

st.title("Uber pickups in NYC")


def get_taxi_df(query, spark_session) -> DataFrame:
    return spark_session.sql(query)


def save_to_delta_table(
    pl_df, spark_session, table_name="bu1_dev.analytics.uber_pickups"
):
    try:
        # Convert Polars to pandas (intermediate step)
        pd_df = pl_df.to_pandas()

        # Clean column names (replace spaces with underscores)
        for col in pd_df.columns:
            if " " in col:
                pd_df = pd_df.rename(columns={col: col.replace(" ", "_")})

        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark_session.createDataFrame(pd_df)

        # Save as Delta table
        spark_df.write.format("delta").mode("append").saveAsTable(table_name)

        return True, "Response saved to Delta table."
    except Exception as e:
        return False, f"Failed to save to Delta table: {e}"


def getData(spark):
    # This example query depends on the nyctaxi data set in Unity Catalog, see https://docs.databricks.com/en/discover/databricks-datasets.html for details
    # return sqlQuery("select * from samples.nyctaxi.trips limit 5000")
    return pl.from_pandas(
        get_taxi_df("select * from samples.nyctaxi.trips limit 10", spark).toPandas()
    )


# commen
# data = pd.read_csv(DATA_URL, nrows=nrows)
# lowercase = lambda x: str(x).lower()
# data.rename(lowercase, axis="columns", inplace=True)
# data[DATE_COLUMN] = pd.to_datetime(data[DATE_COLUMN])
# return data


# @st.cache_data
# def load_data(nrows):
#     data = pd.read_csv(DATA_URL, nrows=nrows)
#     lowercase = lambda x: str(x).lower()
#     data.rename(lowercase, axis="columns", inplace=True)
#     data[DATE_COLUMN] = pd.to_datetime(data[DATE_COLUMN])
#     return data


# COMMAND ----------
data_load_state = st.text("Loading data...")
spark = get_spark_session()
data = getData(spark)
data_load_state.text("Done! (using st.cache_data)")

# if st.checkbox("Show raw data"):
st.subheader("Raw data")
st.write(data)

# Save to Delta table
success, message = save_to_delta_table(data, spark)
if success:
    st.success(message)
else:
    st.error(message)
    # Fallback: save to local CSV
    data.write_csv("survey_responses_local.csv", separator=",")
    st.warning("Response saved to local file as fallback.")


# st.subheader("Number of pickups by hour")
# hist_values = np.histogram(data[DATE_COLUMN].dt.hour, bins=24, range=(0, 24))[0]
# st.bar_chart(hist_values)
#
# # Some number in the range 0-23
# hour_to_filter = st.slider("hour", 0, 23, 17)
# filtered_data = data[data[DATE_COLUMN].dt.hour == hour_to_filter]
#
# st.subheader("Map of all pickups at %s:00" % hour_to_filter)
# st.map(filtered_data)

# COMMAND ----------
