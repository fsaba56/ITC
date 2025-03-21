from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AQE Example") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "536870912") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.join.enabled", "true") \
    .getOrCreate()

# Load some data and perform a join operation
df1 = spark.read.csv("hdfs:///tmp/jenkins/large_data.csv", header=True)
df2 = spark.read.csv("hdfs:///tmp/jenkins/small_lookup.csv", header=True)

# Perform a join
result = df1.join(df2, "id")
result.show()
