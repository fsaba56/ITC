from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("Minipro").enableHiveSupport().getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.170.23.150:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "tfl_underground_pyspark").option("user", "postgres").option("password", "welcomeitc@2022").load()
df.printSchema()

# Transformation 1: Convert 'Timestamp' column to timestamp data type
df_transformed = df.withColumn("Timestamp", to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm"))

# Transformation 2: Replace 'N/A' with None
df_transformed = df_transformed.na.replace("N/A", None)

# Write the transformed DataFrame to Hive table
df_transformed.write.mode("overwrite").saveAsTable("bigdata_sabaitc.bigdata_sabaitc")
print("Successfully loaded to Hive")
# spark-submit --master local[*] --jars /var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar src/full_load_postgresToHive.py
