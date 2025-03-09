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
df_postgres = spark.read.format("jdbc").options(
    url=url,
    dbtable="tfl_underground_pyspark",  # Replace with your PostgreSQL table name
    user=properties["user"],
    password=properties["password"],
    driver=properties["driver"]
).load()

# Step : Read data from Hive into a Spark DataFrame
df_hive = spark.sql("SELECT * FROM tfl_data_db.tfl_underground")

# Transformation 1: Convert 'Timestamp' column to timestamp data type
df_hive_transformed = df_hive.withColumn("Timestamp", to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm"))

# Transformation 2: Replace 'N/A' with None
df_hive_transformed = df_hive_transformed.na.replace("N/A", None)
df_hive_transformed = df_hive_transformed.na.replace("NaN", None)

# Step 3: Identify the new data (data not in PostgreSQL)
df_new_data = df_hive_transformed.subtract(df_postgres)

# Step 4: Append the new data to PostgreSQL
if df_new_data.count() > 0:
    df_new_data.write.format("jdbc").options(
        url=url,
        dbtable="tfl_underground_pyspark",  # PostgreSQL table name
        user=properties["user"],
        password=properties["password"],
        driver=properties["driver"]
    ).mode("append").save()
    df_new_data.printSchema() 
    print("New data successfully appended to PostgreSQL.")
else:
    print("No new data to append.")
   
# Write the transformed DataFrame to Hive table
#df_new_data.write.mode("overwrite").saveAsTable("bigdata_sabaitc.tflpyspark")
df_new_data.write.mode("append").saveAsTable("bigdata_sabaitc.tflpyspark")
print("Successfully loaded to Hive")
# spark-submit --master local[*] --jars /var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar src/full_load_postgresToHive.py

