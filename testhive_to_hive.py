from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_replace, row_number, when, hour
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("Hive Table Insert with Auto Increment Record ID") \
    .enableHiveSupport() \
    .getOrCreate()

# Define database and tables
HIVE_DB = "default"
SOURCE_TABLE = "tfl_undergroundrecord"
TARGET_TABLE = "TFL_Underground_Result_N"

# Load data from the source table
df_source = spark.sql("SELECT * FROM default.tfl_undergroundrecord")

# Add an "ingestion_timestamp" column
df_transformed = df_source.withColumn("ingestion_timestamp", current_timestamp())

# Remove ALL leading and trailing quotes from "route" and "delay_time" columns
df_transformed = df_transformed.withColumn("route", regexp_replace(col("route"), r'^[\'"]+|[\'"]+$', ''))
df_transformed = df_transformed.withColumn("delay_time", regexp_replace(col("delay_time"), r'^[\'"]+|[\'"]+$', ''))

# Remove NULL values from the route column
df_transformed = df_transformed.filter(col("route").isNotNull())

# Retrieve the maximum existing record_id from the target table
try:
    max_record_id = spark.sql("SELECT MAX(record_id) FROM default.TFL_Underground_Result_N").collect()[0][0]
    if max_record_id is None:
        max_record_id = 0  # If table is empty, start from 1
except:
    max_record_id = 0  # If table doesn't exist, start from 1

# Generate an auto-incremented record_id based on row_number() with deterministic ordering
window_spec = Window.orderBy("timedetails", "route", "delay_time")  # Ordering to avoid duplicate IDs
df_transformed = df_transformed.withColumn("record_id", row_number().over(window_spec) + max_record_id)

# Cast record_id to Integer
df_transformed = df_transformed.withColumn("record_id", col("record_id").cast(IntegerType()))

# Add PeakHour and OffHour columns based on `ingestion_timestamp`
df_transformed = df_transformed.withColumn(
    "peakhour",
    when((hour(col("timedetails")) >= 7) & (hour(col("timedetails")) < 9), 1).otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "offhour",
    when((hour(col("timedetails")) >= 16) & (hour(col("timedetails")) < 19), 1).otherwise(0)
)

# Debugging: Ensure record_id is not NULL before writing
df_transformed.select("record_id", "timedetails", "route", "delay_time", "peakhour", "offhour").show(10)

# Ensure column order matches Hive table
expected_columns = ["record_id", "timedetails", "line", "status", "reason", "delay_time", "route", "ingestion_timestamp", "peakhour", "offhour"]
df_final = df_transformed.select(*expected_columns)

# Append data into the existing Hive table
df_final.write.mode("append").insertInto("default.TFL_Underground_Result_N")

# Stop Spark session
spark.stop()
