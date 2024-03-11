from pyspark.sql import SparkSession
import numpy as np

# Define the Schwefel function
def schwefel(x):
    n = len(x)
    return 418.9829 * n - np.sum(x * np.sin(np.sqrt(np.abs(x))))

# Define a function to evaluate Schwefel function for a given set of parameters
def evaluate_schwefel(params):
    return schwefel(params), params

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("PSO_Spark") \
        .getOrCreate()

    # Parallelize the parameter space across Spark workers
    param_space = spark.sparkContext.parallelize([np.random.uniform(-500, 500, 10) for _ in range(1000)])

    # Evaluate the Schwefel function for each set of parameters in parallel
    results = param_space.map(lambda params: evaluate_schwefel(params)).collect()

    # Find the minimum function value and corresponding parameters
    best_result = min(results, key=lambda x: x[0])

    # Print the results
    print("Global Minimum found at:", best_result[1])
    print("Function value at the minimum:", best_result[0])

    # Stop Spark session
    spark.stop()

    
