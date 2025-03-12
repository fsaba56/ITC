from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_replace, row_number, when, hour, trim, lower
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
df_source.show()
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
    if max_record_id is None:  # If the table is empty, start from 0
        max_record_id = 1
except:
    max_record_id = 1  # If table doesn't exist, start from 0


# Keep only the first occurrence of each "timedetails" and "route"
df_transformed = df_transformed.filter(col("row_num") == 1).drop("row_num")

# Create a unique key based on "timedetails" and "route" to avoid inserting duplicates
window_spec = Window.partitionBy("timedetails", "route").orderBy("ingestion_timestamp")

# Generate an auto-incrementing `record_id` (with correct casting applied to the column)
df_transformed = df_transformed.withColumn(
    "record_id", 
    (row_number().over(window_spec) + (1 if max_record_id == 0 else max_record_id))
)
# Cast the 'record_id' column to Integer
df_transformed = df_transformed.withColumn("record_id", col("record_id").cast(IntegerType()))

# Generate an auto-incrementing `record_id`
#df_transformed = df_transformed.withColumn("record_id", (row_number().over(Window.orderBy("ingestion_timestamp")) + max_record_id).cast(IntegerType()))

# Keep only the first occurrence of each "timedetails" and "route"
df_transformed = df_transformed.filter(col("row_num") == 1).drop("row_num")

# Remove rows where 'timedetails' contains "timedetails" OR is NULL/empty
df_transformed = df_transformed.filter(
    (~lower(col("timedetails")).contains("timedetails")) &  # Remove rows containing "timedetails"
    (col("timedetails").isNotNull()) &  # Remove NULL values
    (trim(col("timedetails")) != "")    # Remove empty values
)

# Add PeakHour and OffHour columns based on `ingestion_timestamp`
df_transformed = df_transformed.withColumn(
    "peakhour",
    when((hour(col("timedetails")) >= 7) & (hour(col("timedetails")) < 9), 1).otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "offhour",
    when((hour(col("timedetails")) >= 16) & (hour(col("timedetails")) < 19), 1).otherwise(0)
)
df_transformed.show()

# Debugging: Ensure record_id is not NULL before writing
df_transformed.select("record_id", "timedetails", "route", "delay_time", "peakhour", "offhour").show(10)

# Ensure column order matches Hive table
expected_columns = ["record_id", "timedetails", "line", "status", "reason", "delay_time", "route", "ingestion_timestamp", "peakhour", "offhour"]
df_final = df_transformed.select(*expected_columns)

# Append data into the existing Hive table
df_final.write.mode("append").insertInto("default.TFL_Underground_Result_N")
