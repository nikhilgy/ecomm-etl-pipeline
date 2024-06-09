# src/spark_context.py
from pyspark.sql import SparkSession

# Initialize the Spark session and store it in a global variable
spark = SparkSession.builder \
    .appName("Ecomm-data-analysis") \
    .getOrCreate()
