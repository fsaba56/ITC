from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, regexp_replace, row_number, when, hour, trim, lower, lit, expr, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("Hive Table Insert with Auto Increment ID") \
    .enableHiveSupport() \
    .getOrCreate()

# Define database and tables
HIVE_DB = "default"
SOURCE_TABLE = "tfl_undergroundrecord"
TARGET_TABLE = "TFL_Underground_Result_N"

# Load data from the source table
df_source = spark.sql("SELECT * FROM default.tfl_undergroundrecord")
df_source = df_source.withColumn("ingestion_timestamp", current_timestamp())
df_source.show()

# Step 2: Read existing data from the target table (if exists)
try:
    df_target = spark.sql("SELECT * FROM default.TFL_Underground_Result_N")
    target_exists = True
except:
    target_exists = False  # Table does not exist yet

# Add an "ingestion_timestamp" column
df_transformed = df_source.withColumn("ingestion_timestamp", current_timestamp())

# Remove ALL leading and trailing quotes from "route" and "delay_time" columns
df_transformed = df_transformed.withColumn("route", regexp_replace(col("route"), r'^[\'"]+|[\'"]+$', ''))
df_transformed = df_transformed.withColumn("delay_time", regexp_replace(col("delay_time"), r'^[\'"]+|[\'"]+$', ''))

# Remove NULL values from the route column
df_transformed = df_transformed.filter(col("route").isNotNull())

# Remove rows where 'timedetails' contains "timedetails" OR is NULL/empty
df_transformed = df_transformed.filter(
    (~lower(col("timedetails")).contains("timedetails")) &  # Remove rows containing "timedetails"
    (col("timedetails").isNotNull()) &  # Remove NULL values
    (trim(col("timedetails")) != "") &  # Remove empty values
    (col("timedetails") != "") & 
    (col("timedetails") != "N/A")
)
# Remove duplicates (based on key columns to prevent re-inserting same data)
df_transformed = df_transformed.dropDuplicates(["timedetails", "line", "status", "reason", "delay_time", "route"])

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
    max_record_id = spark.sql("SELECT MAX(record_id) FROM default.TFL_Underground_Result_N").collect()[0][0]
    max_record_id = max_record_id if max_record_id else 0  # If empty, start from 1
else:
    max_record_id = 0  # If table doesn't exist, start from 1

# Generate an auto-incrementing record_id starting from max_record_id + 1
window_spec = Window.orderBy("ingestion_timestamp")
df_transformed = df_transformed.withColumn("record_id", row_number().over(window_spec) + lit(max_record_id))

# Ensure "record_id" is Integer
df_transformed = df_transformed.withColumn("record_id", expr("CAST(record_id AS INT)"))


# Convert 'timedetails' to timestamp if not already in timestamp format
# Use the full path to the function to avoid conflicts
df_transformed = df_transformed.withColumn("timedetails", F.to_timestamp(F.col("timedetails")))

# Add PeakHour and OffHour columns based on `timedetails`
# Replace 'hour' with 'F.hour'
df_transformed = df_transformed.withColumn(
    "peakhour",
    F.when((F.hour(F.col("timedetails")) >= 7) & (F.hour(F.col("timedetails")) < 9), 1).otherwise(0)
)

df_transformed = df_transformed.withColumn(
    "offhour",
    F.when((F.hour(F.col("timedetails")) >= 16) & (F.hour(F.col("timedetails")) < 19), 1).otherwise(0)
)

# For other hours (not peak or off hours), assign peakhour = 0, offhour = 1
#df_transformed = df_transformed.withColumn(
 #   "peakhour",
  #  when((hour(col("timedetails")) >= 7) & (hour(col("timedetails")) < 9), 1)
   # .when((hour(col("timedetails")) >= 16) & (hour(col("timedetails")) < 19), 0)
    #.otherwise(0)
#)

#df_transformed = df_transformed.withColumn(
 #   "offhour",
  #  when((hour(col("timedetails")) >= 16) & (hour(col("timedetails")) < 19), 1)
   # .otherwise(1)
#)

# Debugging: Ensure record_id and new columns are properly created before writing
df_transformed.select("record_id", "timedetails", "route", "delay_time").show()

# Ensure column order matches Hive table
expected_columns = ["record_id", "timedetails", "line", "status", "reason", "delay_time", "route", "ingestion_timestamp"]
df_final = df_transformed.select(*expected_columns)

# Append data into the existing Hive table
df_final.write.format("hive").mode("append").saveAsTable("default.TFL_Underground_Result_N")

# Stop Spark session at the end of the script
spark.stop()
