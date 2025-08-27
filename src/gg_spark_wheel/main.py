from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, ArrayType, StringType, DateType
from datetime import date

devices_schema = StructType([
  StructField("chip_id", IntegerType(), True),
  StructField("chip_name", StringType(), True),
  StructField("chip_type", IntegerType(), True),
  StructField("chip_manufacturer", StringType(), True),
  StructField("last_service_date", DateType(), True),
  StructField("chip_status", StringType(), True),
])

sample_devices = [
  (21762, "Raspberry Pi 3", 3, "Research in Motion", date.fromisoformat("2019-01-01"), "ACTIVE"),
  (6773, "Mega Bpard 40", 3, "Arduino", date.fromisoformat("2017-10-31"), "AT_REPAIRS"),
  (3741, "SPX 2000", 4, "Yamaha", date.fromisoformat("2011-10-31"), "DECOMISSIONED")
]

device_lookup_df = spark.createDataFrame(sample_devices, schema=devices_schema)
device_lookup_df.show()

#device_lookup_df.write.mode("overwrite").saveAsTable("gguitarts_own_cat.home_temp.sensor_chip_lookup_dab")