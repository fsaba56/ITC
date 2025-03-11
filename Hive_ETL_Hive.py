from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import monotonically_increasing_id

# Initialize Spark Session with Hive Support
spark = SparkSession.builder \
    .appName("Hive ETL Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

target_table = "default.tfl_underground_result"
# Step 1: Load data from the source Hive table
print("Step 1: Reading data from Hive table")
df_source = spark.sql("SELECT * FROM default.tfl_undergroundrecord")

# Step 2: Check if the table exists and get the last recordid
try:
    print("Step 1: Reading maxid from Hive table")
    last_recordid = spark.sql("SELECT MAX(recordid) FROM default.tfl_underground_result").collect()[0][0]
except Exception as e:
    last_recordid = None

# If table doesn't exist or it's the first load, set the starting recordid to 1
if last_recordid is None:
    last_recordid = 0

# Example Transformations
print("Step 2: Performing transformations...")

# Step 1: Generate new 'recordid' for the data from source
# Create a monotonically increasing ID, starting from last_recordid + 1
df_transformed = df_source.withColumn("recordid", monotonically_increasing_id() + last_recordid + 1)
   
# 2. Filtering records based on a condition (Example: removing NULL values)
df_transformed = df_transformed.filter(col("route").isNotNull())

# 3. Adding an "ingestion_timestamp" column
df_transformed = df_transformed.withColumn("ingestion_timestamp", current_timestamp())

# 4. Filter out rows where the timestamp column is a specific value (e.g., 'Timestamp')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'timedetails')

# 5. Filter out rows where the timestamp is NULL
df_transformed = df_transformed.filter(df_transformed.timestamp.isNotNull())

# Show transformed data
df_transformed.show()

# Write the transformed data back into another Hive table
print("Step 3: Writing transformed data to Hive table")
df_transformed.write.mode("append").format("hive").saveAsTable(target_table)

print("ETL Process Completed Successfully!")

# Stop Spark Session
spark.stop()
