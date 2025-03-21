from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("FileFormatExample").getOrCreate()

# Read CSV file
df = spark.read.option("header", True).csv("hdfs:///tmp/jenkins/data.csv")

# Save as Parquet
df.write.mode("overwrite").parquet("data.parquet")

# Save as ORC
df.write.mode("overwrite").orc("data.orc")

# Save as JSON
df.write.mode("overwrite").json("data.json")

# Save as TXT (tab-separated values)
df.write.mode("overwrite").option("sep", "\t").csv("data.txt")

# Verify Data
print("Original CSV Data:")
df.show()

# Read Parquet
df_parquet = spark.read.parquet("data.parquet")
print("\n==== This is Parquet Format ====")
df_parquet.show()

# Read ORC
df_orc = spark.read.orc("data.orc")
print("\n==== This is ORC Format ====")
df_orc.show()

# Read JSON
df_json = spark.read.json("data.json")
print("\n==== This is JSON Format ====")
df_json.show()

# Read TXT (tab-separated)
df_txt = spark.read.option("sep", "\t").csv("data.txt", header=True)
print("\n==== This is TXT Format ====")
df_txt.show()
