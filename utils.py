import sys

from databricks.connect import DatabricksSession


def get_spark_session():
    try:
        return DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        return None


def print_workspace_info(spark: DatabricksSession) -> None:
    print(spark.conf.get("spark.databricks.workspaceUrl"))
    print(spark.conf.get("spark.databricks.clusterUsageTags.clusterId"))


def print_python_version() -> None:
    print(sys.version_info)


def show_sample_data(
    spark: DatabricksSession, table_name: str = "samples.nyctaxi.trips", limit: int = 10
) -> None:
    spark.table(table_name).show(limit, False)


def main() -> None:
    print_workspace_info(spark)
    print_python_version()
    show_sample_data(spark, table_name="samples.nyctaxi.trips")


if __name__ == "__main__":
    main()
