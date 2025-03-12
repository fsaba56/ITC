from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.functions import monotonically_increasing_id

# Initialize Spark Session with Hive Support
spark = SparkSession.builder \
    .appName("Hive ETL Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
target_table = "default.tfl_underground_result"

# Step 1: Load data from the source Hive table
print("Step 1: Reading data from Hive table")
df_source = spark.sql("SELECT * FROM default.tfl_undergroundrecord")

# Step 2: Check if the table exists and get the last recordid
try:
    print("Step 1: Reading maxid from Hive table")
    last_recordid = spark.sql("SELECT MAX(record_id) FROM default.tfl_underground_result").collect()[0][0]
except Exception as e:
    last_recordid = None

# If table doesn't exist or it's the first load, set the starting recordid to 1
if last_recordid is None:
    last_recordid = 0

# Example Transformations
print("Step 2: Performing transformations...")

# Add Auto-Increment Column
df_source = df_source.withColumn("id", monotonically_increasing_id())
  
# 2. Filtering records based on a condition (Example: removing NULL values)
df_source = df_source.filter(col("route").isNotNull())

# 3. Adding an "ingestion_timestamp" column
df_source = df_source.withColumn("ingestion_timestamp", current_timestamp())

# 4. Filter out rows where the timestamp column is a specific value (e.g., 'Timestamp')
df_source = df_source.filter(df_source.timedetails != 'timedetails')
df_source = df_source.filter(df_source.timedetails != 'timedetails String')
df_source = df_source.filter(df_source.timedetails != 'status String')
df_source = df_source.filter(df_source.timedetails != 'route String')
df_source = df_source.filter(df_source.timedetails != 'reason String')
df_source = df_source.filter(df_source.timedetails != 'line String')
df_source = df_source.filter(df_source.timedetails != 'id int')
df_source = df_source.filter(df_source.timedetails != 'delay_time String')
df_source = df_source.filter(df_source.timedetails != 'STORED AS TEXTFILE')
df_source = df_source.filter(df_source.timedetails != 'ROW FORMAT DELIMITED')
df_source = df_source.filter(df_source.timedetails != 'LOCATION /tmp/big_datajan2025/TFL/TFL_UndergroundRecord')
df_source = df_source.filter(df_source.timedetails != 'LINES TERMINATED BY \n')
df_source = df_source.filter(df_source.timedetails != 'FIELD TERMINATED BY ')
df_source = df_source.filter(df_source.timedetails != 'CREATE EXTERNAL TABLE default.tfl_ugrFullScoop (')

# 5. Filter out rows where the timestamp is NULL
df_source = df_source.filter(df_source.timedetails.isNotNull())

# Show transformed data
df_source.show()

# Write the transformed data back into another Hive table
print("Step 3: Writing transformed data to Hive table")
#df_transformed.write.mode("overwrite").format("hive").saveAsTable(target_table)
# Use insertInto to append or overwrite the data
df_source.write.mode("overwrite").format("hive").insertInto(target_table)
print("ETL Process Completed Successfully!")

# Stop Spark Session
spark.stop()
