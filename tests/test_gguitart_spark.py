import pytest
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config
from src.gg_spark_wheel.main import process_devices

#TODO - The Cluster ID should be parameterized or obtained from the bundle (would that be possible at all?)
@pytest.fixture(scope="session")
def spark():
    config = Config(
    profile    = "DEFAULT",
    cluster_id = "0707-183655-c0cqjuw9"
    )
    try:
        spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
        yield spark
        spark.stop()
    except Exception as e:
        print(f"Failed to create DatabricksSession: {e}")


def test_main_package(spark):
    expected_schema = StructType([
        StructField("chip_id", IntegerType(), True),
        StructField("chip_name", StringType(), True),
        StructField("chip_type", IntegerType(), True),
        StructField("chip_manufacturer", StringType(), True),
        StructField("last_service_date", DateType(), True),
        StructField("chip_status", StringType(), True),
    ])
    assert process_devices(spark).schema == expected_schema