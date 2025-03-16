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
TARGET_TABLE = "tfl_underground_result_"

# Load data from the source table
df_source = spark.sql("SELECT * FROM default.tfl_undergroundrecord")
df_source = df_source.withColumn("ingestion_timestamp", current_timestamp())
df_source.show()

# Step 2: Read existing data from the target table (if exists)
try:
    df_target = spark.sql("SELECT * FROM default.tfl_underground_result_")
    target_exists = True
except:
    target_exists = False  # Table does not exist yet

# Add an "ingestion_timestamp" column
df_transformed = df_source.withColumn("ingestion_timestamp", current_timestamp())

# Remove ALL leading and trailing quotes from "route" and "delay_time" columns
df_transformed = df_transformed.withColumn("route", regexp_replace(col("route"), r'^[\'"]+|[\'"]+$', ''))
df_transformed = df_transformed.withColumn("timedetails", regexp_replace(col("timedetails"), r'^[\'"]+|[\'"]+$', ''))
df_transformed = df_transformed.withColumn("reason", regexp_replace(col("reason"), r'^[\'"]+|[\'"]+$', ''))
df_transformed = df_transformed.withColumn("status", regexp_replace(col("status"), r'^[\'"]+|[\'"]+$', ''))
df_transformed = df_transformed.withColumn("delay_time", regexp_replace(col("delay_time"), r'^[\'"]+|[\'"]+$', ''))

# Remove NULL values from the route column
df_transformed = df_transformed.filter(col("route").isNotNull())

# Remove rows where 'timedetails' contains "timedetails" OR is NULL/empty
df_transformed = df_transformed.filter(
    (~lower(col("timedetails")).contains("timedetails")) &  # Remove rows containing "timedetails"
    (col("timedetails").isNotNull()) &  # Remove NULL values
    (trim(col("timedetails")) != "")    # Remove empty values
)
# Remove duplicates (based on key columns to prevent re-inserting same data)
df_transformed = df_transformed.dropDuplicates(["timedetails", "line", "status", "reason", "delay_time", "route"])

# Step 4: Remove duplicates based on key columns (Ensure only NEW data is inserted)
key_columns = ["timedetails", "line", "status", "reason", "delay_time", "route"]

# We need to use a more robust filtering approach for removing duplicates
if target_exists:
    df_transformed = df_transformed.alias("new").join(
        df_target.alias("existing"),
        on=[df_transformed[col] == df_target[col] for col in key_columns],
        how="left_anti"  # Keeps only records that are NOT in the target
    )
else:
    # If target doesn't exist, we don't need the filter
    df_transformed = df_transformed

# Step 5: Ensure record_id starts from max_record_id if the table exists, otherwise from 1
if target_exists:
    max_record_id = spark.sql("SELECT MAX(record_id) FROM default.tfl_underground_result_").collect()[0][0]
    max_record_id = max_record_id if max_record_id else 0  # If empty, start from 1
else:
    max_record_id = 0  # If table doesn't exist, start from 1

# Generate an auto-incrementing record_id starting from max_record_id + 1
window_spec = Window.orderBy("ingestion_timestamp")
df_transformed = df_transformed.withColumn("record_id", row_number().over(window_spec) + lit(max_record_id))

# Ensure "record_id" is Integer
df_transformed = df_transformed.withColumn("record_id", expr("CAST(record_id AS INT)"))

# Replace all statuses except "Good Service" with "Delay"
df_transformed = df_transformed.withColumn(
    "status",
    when(col("service_status") != "Good Service", "Delay").otherwise(col("status"))
)

# Debugging: Ensure record_id and new columns are properly created before writing
df_transformed.select("record_id", "timedetails", "route", "delay_time").show()

# Ensure column order matches Hive table
expected_columns = ["record_id", "timedetails", "line", "status", "reason", "delay_time", "route", "ingestion_timestamp"]
df_final = df_transformed.select(*expected_columns)

# Ensure data is partitioned correctly (e.g., if you're using partitioned Hive tables)
df_final.write.format("hive").mode("append").insertInto("default.tfl_underground_result_")

# Step 6: Add explicit partitioning if necessary
# If your Hive table is partitioned by a column (e.g., "date"), make sure you are specifying the partition column when writing
# df_final.write.partitionBy("date_column").format("hive").mode("append").insertInto("default.tfl_underground_result_")

# Step 7: To avoid duplicate records, consider cleaning up your data before insertion
# Optionally, you can deduplicate records based on the ingestion timestamp or other criteria:
df_final = df_final.dropDuplicates(["timedetails", "line", "status", "reason", "delay_time", "route"])

# Final write to Hive table
df_final.write.format("hive").mode("append").saveAsTable("default.tfl_underground_result_")

# Stop Spark session at the end of the script
spark.stop()
