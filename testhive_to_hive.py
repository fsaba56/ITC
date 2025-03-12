from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_replace, row_number, when, hour, trim, lower, lit
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("Hive Incremental Load with Auto Increment Record ID") \
    .enableHiveSupport() \
    .getOrCreate()

# Define database and tables
HIVE_DB = "default"
SOURCE_TABLE = "tfl_undergroundrecord"
TARGET_TABLE = "tfl_underground_result_n"

# Step 1: Read latest data from the source table
df_source = spark.sql("SELECT * FROM HIVE_DB.tfl_undergroundrecord")
df_source = df_source.withColumn("ingestion_timestamp", current_timestamp())

# Step 2: Read existing data from the target table (if exists)
try:
    df_target = spark.sql("SELECT * FROM HIVE_DB.tfl_underground_result_n")
    target_exists = True
except:
    target_exists = False  # Table does not exist yet

# Step 3: Remove NULL, empty values, and unnecessary records
df_transformed = df_source \
    .withColumn("route", regexp_replace(col("route"), r'^[\'"]+|[\'"]+$', '')) \
    .withColumn("delay_time", regexp_replace(col("delay_time"), r'^[\'"]+|[\'"]+$', '')) \
    .filter(col("route").isNotNull()) \
    .filter(
        (~lower(col("timedetails")).contains("timedetails")) &  # Remove "timedetails"
        (col("timedetails").isNotNull()) &  # Remove NULLs
        (trim(col("timedetails")) != "") &  # Remove empty values
        (col("timedetails") != "N/A")  # Remove "N/A"
    )

# Step 4: Remove duplicates based on key columns (Ensure only NEW data is inserted)
key_columns = ["timedetails", "line", "status", "reason", "delay_time", "route"]

if target_exists:
    df_transformed = df_transformed.alias("new").join(
        df_target.alias("existing"),
        on=[df_transformed[col] == df_target[col] for col in key_columns],
        how="left_anti"  # Keeps only records that are NOT in the target
    )

# Step 5: Get max record_id from target table (to continue numbering)
if target_exists:
    max_record_id = spark.sql("SELECT MAX(record_id) FROM HIVE_DB.tfl_underground_result_n").collect()[0][0]
    max_record_id = max_record_id if max_record_id else 0  # If empty, start from 1
else:
    max_record_id = 0  # If table doesn't exist, start from 1

# Step 6: Assign new `record_id`, continuing from the last inserted ID
window_spec = Window.orderBy("ingestion_timestamp")
df_transformed = df_transformed.withColumn("record_id", row_number().over(window_spec) + lit(max_record_id))
df_transformed = df_transformed.withColumn("record_id", col("record_id").cast(IntegerType()))

# Step 7: Add PeakHour and OffHour columns
df_transformed = df_transformed.withColumn(
    "peakhour", when((hour(col("timedetails")) >= 7) & (hour(col("timedetails")) < 9), 1).otherwise(0)
)
df_transformed = df_transformed.withColumn(
    "offhour", when((hour(col("timedetails")) >= 16) & (hour(col("timedetails")) < 19), 1).otherwise(0)
)

# Step 8: Ensure column order matches Hive table
expected_columns = ["record_id", "timedetails", "line", "status", "reason", "delay_time", "route", "ingestion_timestamp", "peakhour", "offhour"]
df_final = df_transformed.select(*expected_columns)

# Step 9: Append only new data into the existing Hive table
df_final.write.format("hive").mode("append").saveAsTable("HIVE_DB.tfl_underground_result_n")

print("Incremental Load Completed Successfully!")
