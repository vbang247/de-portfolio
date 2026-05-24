from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("NYC Taxi Analysis") \
    .master("local[*]") \
    .getOrCreate()

print("Reading parquet file")
df = spark.read.parquet("taxi.parquet")

df.printSchema()

# Group by passenger count
print("\n=== Trips by Passenger Count ===")

# Filter — only completed trips over $20
print("\n=== High Value Trips ===")
high_value = df.filter((col('total_amount')>20) & (col('passenger_count') > 0))

print(f"High value trip count: {high_value.count()}")


spark.stop()
print("\nDone!")