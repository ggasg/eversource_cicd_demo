import pytest
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession

from src.gg_spark_wheel.main import main

def start_spark(app_name="Tests"):
    spark_builder = (
        SparkSession
        .builder
        .appName(app_name))

    spark_sess = spark_builder.getOrCreate()
    return spark_sess

@pytest.fixture(scope="session")
def spark():
    try:
        spark = DatabricksSession.builder.getOrCreate()
        yield spark
        spark.stop()
    except Exception as e:
        print(f"Failed to create DatabricksSession: {e}")


def test_main_package():
    expected_schema = StructType([
        StructField("chip_id", IntegerType(), True),
        StructField("chip_name", StringType(), True),
        StructField("chip_type", IntegerType(), True),
        StructField("chip_manufacturer", StringType(), True),
        StructField("last_service_date", DateType(), True),
        StructField("chip_status", StringType(), True),
    ])
    assert main() == expected_schema
    assert "chicha" == "Chachi"