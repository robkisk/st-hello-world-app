import os
from datetime import datetime

import numpy as np
import polars as pl
import streamlit as st
from pyspark.sql import SparkSession

st.title("Store Closure Survey")

# Inputs
name = st.text_input("1. Please state your Name")
district = st.selectbox("2. Please select your District", ["Outlet East", "None"])
storeNumber = st.selectbox(
    "3. Please select your Store/Number", ["8043 - Deer Park", "None"]
)

emergency = st.selectbox(
    label="4. Is this an emergency store closure?",
    options=["Yes", "No"],
    help="""Emergencies include: Armed robbery, bomb threats, active shooter, civil unrest, fire and flood.\n
                                   A non-emergency include: staffing, power outage or snow.""",
)
arrange = st.selectbox(
    "5. Identify what accommodations your store needs", ["Closing early", "None"]
)
districtManager = st.selectbox(
    "6. Have you partnered with your District Manager in regards to Closing early?",
    ["Yes", "No"],
)
closeSuccess = st.selectbox(
    "7. Were you able to close the store successfully?", ["Yes", "No"]
)
unsuccessReason = st.text_input(
    "8. Why was the store unable to be closed successfully?"
)
hourAdjust = st.selectbox(
    "9. Do your store hours need to be adjusted on the website?", ["Yes", "No"]
)
newHours = st.text_input("10. Please identify the adjusted hours?")
start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")
closeEarly = st.text_input("What time is the store Closing early? (ex: 4:00 pm)")
disables = st.multiselect(
    "Please disable (select all that apply)", ["None of the above", "NA"]
)

# Create selectbox
options = ["Weather", "Flood"] + ["Other:"]
reason = st.selectbox("Please select why you are Closing early", options=options)

explanation = st.text_area("Please explain further the reason behind Closing early")
shipment = st.selectbox("Are you expecting shipment?", ["Yes", "No"])
comments = st.text_area("Any comments?")


def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession
        from pyspark.sql import SparkSession

        return DatabricksSession.builder.serverless().getOrCreate()

    except ImportError:
        return SparkSession.builder.getOrCreate()


def save_to_delta_table(pl_df, table_name="bu1_dev.analytics.surveys"):
    """
    Convert Polars DataFrame to Spark DataFrame and save as Delta table

    Args:
        pl_df: Polars DataFrame
        table_name: Full name of the Delta table to save to
    """
    try:
        # Convert Polars to pandas (intermediate step)
        pd_df = pl_df.to_pandas()

        # Initialize Spark session
        spark = get_spark()

        # Clean column names (replace spaces with underscores)
        for col in pd_df.columns:
            if " " in col:
                pd_df = pd_df.rename(columns={col: col.replace(" ", "_")})

        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(pd_df)

        # Save as Delta table
        spark_df.write.format("delta").mode("append").saveAsTable(table_name)

        return True, "Response saved to Delta table."
    except Exception as e:
        return False, f"Failed to save to Delta table: {e}"


submit = st.button("Submit")

# Process form submission
if submit:
    timestamp = datetime.now().isoformat()

    # Create DataFrame with Polars
    response_df = pl.DataFrame(
        {
            "timestamp": [timestamp],
            "name": [name],
            "district": [district],
            "storeNumber": [storeNumber],
            "emergency": [emergency],
            "arrange": [arrange],
            "districtManager": [districtManager],
            "closeSuccess": [closeSuccess],
            "unsuccessReason": [unsuccessReason],
            "hourAdjust": [hourAdjust],
            "newHours": [newHours],
            "start_date": [start_date.isoformat() if start_date else None],
            "end_date": [end_date.isoformat() if end_date else None],
            "closeEarly": [closeEarly],
            "disables": [str(disables)],
            "reason": [reason],
            "explanation": [explanation],
            "shipment": [shipment],
            "comments": [comments],
        }
    )

    # Display the DataFrame
    st.subheader("Survey Responses")
    st.write(response_df)

    # Save to Delta table
    success, message = save_to_delta_table(response_df)
    if success:
        st.success(message)
    else:
        st.error(message)
        # Fallback: save to local CSV
        response_df.write_csv("survey_responses_local.csv", append=True)
        st.warning("Response saved to local file as fallback.")
