from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Taxi Joins") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read cleaned taxi data
trips = spark.read.parquet("cleaned_taxi.parquet")

# Create a small zone lookup table
zone_data = [
    (1, "manhattan", "high_demand"),
    (2, "brooklyn", "medium_demand"),
    (3, "queens", "medium_demand"),
    (4, "bronx", "low_demand"),
    (5, "staten_island", "low_demand"),
]

zone_schema = ["zone_id", "zone_name", "demand_tier"]
zones_df = spark.createDataFrame(zone_data, zone_schema)

print("=== Zone Lookup Table ===")
zones_df.show()

# Join trips with zone lookup on passenger_count as proxy
# (in real data you'd join on zone_id)
print("\n=== Trips Summary ===")
trips.groupBy("trip_category", "is_rush_hour") \
    .agg(
        count("*").alias("total_trips"),
        avg(col("total_amount")).alias("avg_fare")
    ) \
    .orderBy("trip_category", "is_rush_hour") \
    .show()

spark.stop()
