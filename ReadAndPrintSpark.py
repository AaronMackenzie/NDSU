import pyspark
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('ReadAndPrint').getOrCreate()

data=spark.read.text('/Users/aaronmackenzie/Desktop/a.txt')

data.show()

spark.stop()
