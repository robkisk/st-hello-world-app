"""
Script to demonstrate Databricks Connect functionality by connecting to a Databricks workspace
and querying the NYC taxi trips sample dataset.

This script:
1. Establishes a connection to Databricks using Databricks Connect
2. Prints workspace configuration information
3. Displays Python version information
4. Shows a sample of NYC taxi trip data
"""

import sys

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


def get_spark_session():
    try:
        # return SparkSession.builder.profile("dev").serverless().getOrCreate()
        return DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


def print_workspace_info(spark: SparkSession) -> None:
    """
    Print Databricks workspace configuration information.

    Args:
        spark (SparkSessionType): Active Spark session
    """
    print(spark.conf.get("spark.databricks.workspaceUrl"))
    print(spark.conf.get("spark.databricks.clusterUsageTags.clusterId"))


def print_python_version() -> None:
    """Print the current Python version information."""
    print(sys.version_info)


def show_sample_data(
    spark: SparkSession, table_name: str = "samples.nyctaxi.trips", limit: int = 10
) -> None:
    """
    Display a sample of data from the specified table.

    Args:
        spark (SparkSessionType): Active Spark session
        table_name (str): Name of the table to query
        limit (int): Number of rows to display
    """
    spark.table(table_name).show(limit, False)


def main() -> None:
    """
    Main function to execute the Databricks Connect demonstration.
    """
    spark = get_spark_session()
    print_workspace_info(spark)
    print_python_version()
    show_sample_data(spark, table_name="samples.nyctaxi.trips")


if __name__ == "__main__":
    main()
