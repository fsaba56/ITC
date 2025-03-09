from pyspark.sql import *
from pyspark.sql.functions import *
import psycopg2

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

# Step 2: Read data from Hive into a Spark DataFrame
df_hive = spark.sql("SELECT * FROM tfl_data_db.tfl_underground")

# Transformation 1: Convert 'Timestamp' column to timestamp data type
df_hive_transformed = df_hive.withColumn("Timestamp", to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm"))

# Transformation 2: Replace 'N/A' with None
df_hive_transformed = df_hive_transformed.na.replace("N/A", None)
df_hive_transformed = df_hive_transformed.na.replace("NaN", None)

# Step 3: Identify the new data (data not in PostgreSQL)
df_new_data = df_hive_transformed.subtract(df_postgres)

# Step 4: Check if data exists in PostgreSQL
# Check PostgreSQL table size using psycopg2
conn = psycopg2.connect(
    host="18.170.23.150",
    database="testdb",
    user="consultants",
    password="WelcomeItc@2022"
)
cur = conn.cursor()

# Delete records where 'timestamp' is 'Timestamp' (as string) or NULL
delete_query = """
    DELETE FROM tfl_underground_pyspark WHERE timestamp = 'Timestamp';
    DELETE FROM tfl_underground_pyspark WHERE timestamp IS NULL;
"""
cur.execute(delete_query)
conn.commit()  # Commit the delete changes to the database

# Check if data exists in PostgreSQL
cur.execute("SELECT COUNT(*) FROM tfl_underground_pyspark")  # Check if data exists in PostgreSQL
result = cur.fetchone()

if result[0] > 0:
    print("Data exists in PostgreSQL. Appending new data...")
    # If data exists, append new data to PostgreSQL
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
else:
    print("No data in PostgreSQL. Inserting new data...")
    # If data doesn't exist, insert all the data
    df_hive_transformed.write.format("jdbc").options(
        url=url,
        dbtable="tfl_underground_pyspark",  # PostgreSQL table name
        user=properties["user"],
        password=properties["password"],
        driver=properties["driver"]
    ).mode("overwrite").save()
    print("Data successfully inserted into PostgreSQL.")

# Write the transformed DataFrame to Hive table
df_new_data.write.mode("append").saveAsTable("bigdata_sabaitc.tflpyspark")
print("Successfully loaded to Hive")

# Close PostgreSQL connection
cur.close()
conn.close()
