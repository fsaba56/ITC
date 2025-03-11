from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import monotonically_increasing_id

# Initialize Spark Session with Hive Support
spark = SparkSession.builder \
    .appName("Hive ETL Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

# Define Hive Table Names
source_table = "default.tfl_undergroundrecord"
target_table = "default.tfl_underground_result"

print(f" Step 1: Reading data from Hive table: default.tfl_undergroundrecord")
df = spark.sql(f"SELECT * FROM {source_table}")

# Example Transformations
print("Step 2: Performing transformations...")

# Overwrite the existing 'id' column with monotonically increasing values
df_transformed = df.withColumn("recordid", monotonically_increasing_id())

# 2. Filtering records based on a condition (Example: removing NULL values)
df_transformed = df_transformed.filter(col("route").isNotNull())

# 3. Adding an "ingestion_timestamp" column
df_transformed = df_transformed.withColumn("ingestion_timestamp", current_timestamp())

# Show transformed data
df_transformed.show()

# Write the transformed data back into another Hive table
print(f"Step 3: Writing transformed data to Hive table: {target_table}")
df_transformed.write.mode("overwrite").format("hive").saveAsTable(target_table)

print("🎯 ETL Process Completed Successfully!")

# Stop Spark Session
spark.stop()
