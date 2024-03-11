from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CheckNodeUsage") \
    .getOrCreate()

# Get the number of executors
num_executors = spark.sparkContext._conf.get("spark.executor.instances")

# Print a message indicating if both MacBooks are being used
if num_executors >= 2:
    print("Both MacBooks are being used in the Spark cluster.")
else:
    print("Only one MacBook is being used in the Spark cluster.")

# Stop the SparkSession
spark.stop()
