from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.window import Window

# Initialize Spark Session with Hive Support
spark = SparkSession.builder \
    .appName("Hive ETL Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
target_table = "default.tfl_underground_result"


spark.sql("TRUNCATE TABLE default.tfl_underground_result")

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

# Step 1: Use row_number to generate sequential record_id
windowSpec = Window.orderBy(F.lit(1))  # Use a constant to order the rows
df_transformed = df_source.withColumn("record_id", F.row_number().over(windowSpec) + last_recordid)

   
# 2. Filtering records based on a condition (Example: removing NULL values)
df_transformed = df_transformed.filter(col("route").isNotNull())

# 3. Adding an "ingestion_timestamp" column
df_transformed = df_transformed.withColumn("ingestion_timestamp", current_timestamp())

# 4. Filter out rows where the timestamp column is a specific value (e.g., 'Timestamp')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'timedetails')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'timedetails String')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'status String')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'route String')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'reason String')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'line String')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'id int')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'delay_time String')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'STORED AS TEXTFILE')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'ROW FORMAT DELIMITED')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'LOCATION /tmp/big_datajan2025/TFL/TFL_UndergroundRecord')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'LINES TERMINATED BY \n')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'FIELD TERMINATED BY ')
df_transformed = df_transformed.filter(df_transformed.timedetails != 'CREATE EXTERNAL TABLE default.tfl_ugrFullScoop (')

# 5. Filter out rows where the timestamp is NULL
df_transformed = df_transformed.filter(df_transformed.timedetails.isNotNull())

# Show transformed data
df_transformed.show()

# Write the transformed data back into another Hive table
print("Step 3: Writing transformed data to Hive table")
#df_transformed.write.mode("overwrite").format("hive").saveAsTable(target_table)
# Use insertInto to append or overwrite the data
df_transformed.write.mode("overwrite").format("hive").insertInto(target_table)
print("ETL Process Completed Successfully!")

# Stop Spark Session
spark.stop()
