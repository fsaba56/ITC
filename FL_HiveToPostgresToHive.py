from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("Minipro").enableHiveSupport().getOrCreate()

# PostgreSQL connection properties
url = "jdbc:postgresql://18.170.23.150/testdb?ssl=false"  # Replace with your PostgreSQL connection details
properties = {
    "user": "consultants",        # PostgreSQL username
    "password": "WelcomeItc@2022",    # PostgreSQL password
    "driver": "org.postgresql.Driver"
}

# Step 1: Read data from PostgreSQL into a Spark DataFrame
df = spark.read.format("jdbc").options(
    url=url,
    dbtable="tfl_underground_pyspark",  # Replace with your PostgreSQL table name
    user=properties["user"],
    password=properties["password"],
    driver=properties["driver"]
).load()

print("Data successfully read from PostgreSQL:")
df.printSchema()    

# Transformation 1: Convert 'Timestamp' column to timestamp data type
df_transformed = df.withColumn("Timestamp", to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm"))

# Transformation 2: Replace 'N/A' with None
df_transformed = df_transformed.na.replace("N/A", None)
df_transformed = df_transformed.na.replace("NaN", None)

# Write the transformed DataFrame to Hive table
df_transformed.write.mode("overwrite").saveAsTable("bigdata_sabaitc.tflpyspark")
print("Successfully loaded to Hive")
# spark-submit --master local[*] --jars /var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar src/full_load_postgresToHive.py

