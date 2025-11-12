from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("SimpleTest") \
    .getOrCreate()

# Create a simple DataFrame
data = [("John", 30), ("Alice", 25), ("Bob", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame
df.show()

# Stop SparkSession
spark.stop()