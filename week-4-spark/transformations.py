from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("NYC Taxi Analysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("taxi.parquet")

df.printSchema()


print("Clean and Transform")

cleaned =df \
    .filter(col("passenger_count") > 0) \
    .filter(col("total_amount") > 0) \
    .filter(col("trip_distance") > 0) \
    .withColumn("amount_rounded", round(col("total_amount"), 2)) \
    .withColumn("trip_category",
        when(col("trip_distance") < 2, "short")
        .when(col("trip_distance") < 10, "medium")
        .otherwise("long")
    ) \
    .withColumn("pickup_hour", hour(col("pickup_at"))) \
    .withColumn("is_rush_hour",
        when(
            (col("pickup_hour") >= 7) & (col("pickup_hour") <= 9) |
            (col("pickup_hour") >= 17) & (col("pickup_hour") <= 19),
            True
        ).otherwise(False)
    )


print("Print schema")
cleaned.printSchema()

print("Trip category counts")
cleaned.groupBy("trip_category").count() \
    .orderBy(col("trip_category")) \
    .show()

cleaned.groupBy("is_rush_hour") \
    .agg(
        avg(col("total_amount")).alias("avg_fare")
    ).show()


cleaned.write.mode("overwrite").parquet("cleaned_taxi.parquet")

spark.stop()
print("Done!")