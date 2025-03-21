from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("FileFormatExample").getOrCreate()

# Read CSV file
df = spark.read.option("header", True).csv("hdfs:///tmp/jenkins/data.csv")
#df = spark.read.option("header", True).csv(r"C:/Users/fsaba/OneDrive/Documents/Training_Documents/data.csv")

# Save as Parquet
df.write.mode("overwrite").parquet("data.parquet")

# Save as ORC
df.write.mode("overwrite").orc("data.orc")

# Save as JSON
df.write.mode("overwrite").json("data.json")

# Save as Delta (requires Delta Lake)
#df.write.format("delta").mode("overwrite").save("sample_data_delta")

# Save as TXT (tab-separated values)
df.write.mode("overwrite").option("sep", "\t").csv("data.txt")

# Verify Data
df.show()


# Read Parquet
df_parquet = spark.read.parquet("data.parquet")
print("This is parquet")
df_parquet.show()

# Read ORC
df_orc = spark.read.orc("data.orc")
print("This is orc")
df_orc.show()

# Read JSON
df_json = spark.read.json("data.json")
print("This is json")
df_json.show()

# Read Delta
#df_delta = spark.read.format("delta").load("sample_data_delta")
#df_delta.show()

# Read TXT (tab-separated)
df_txt = spark.read.option("sep", "\t").csv("data.txt", header=True)
df_txt.show()
