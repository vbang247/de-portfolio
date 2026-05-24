from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def create_spark_session():
    spark = SparkSession.builder \
        .appName("nyc_taxi_pipeline") \
        .master("local[*]") \
        .getOrCreate()
    return spark

def read_raw_data(spark, path):
    df = spark.read.parquet(path)
    print("Count of raw data:")
    print(df.count())
    return df

def validate_data(df):
    print("Validating data...")
    print("Count of null passenger_count:")
    print(df.filter( col("passenger_count").isNull()).count())
    print("Count of null total_amount:")
    print(df.filter( col("total_amount").isNull()).count())
    print("Minimum total_amount:")
    df.select(min("total_amount")).show()
    print("Maximum total_amount:")
    df.select(max("total_amount")).show()
    return df

def clean_data(df):
    cleaned = df \
        .filter(col("passenger_count") > 0) \
        .filter(col("total_amount") > 0) \
        .filter(col("trip_distance") > 0) \
        .dropDuplicates()
    return cleaned

def transform_data(df):
    window = Window.partitionBy("trip_category").orderBy(col("total_amount").desc())
    transformed = df \
        .withColumn("pickup_hour", hour(col("pickup_at"))) \
        .withColumn("trip_category",
            when(col("trip_distance") < 2, "short")
            .when(col("trip_distance") < 10, "medium")
            .otherwise("long")
            ) \
        .withColumn("is_rush_hour", 
            when(
                (col("pickup_hour") >= 7) & (col("pickup_hour") <= 9) |
                (col("pickup_hour") >= 17) & (col("pickup_hour") <= 19),
                True
            )
            .otherwise(False)
        ) \
        .withColumn("fare_rank", row_number().over(window))
    return transformed

def aggregate_data(df):
    aggregated = df.groupBy("trip_category").agg(
        count("*").alias("total_trips"),
        avg(col("total_amount")).alias("avg_fare"),
        avg(col("trip_distance")).alias("avg_distance")
    )
    return aggregated

def write_output(df, path):
    print("Output written to: ", path)
    df.coalesce(1).write.mode("overwrite").parquet(path)

def main():
    spark = create_spark_session()
    raw_df = read_raw_data(spark, "taxi.parquet")
    validated_df = validate_data(raw_df)
    cleaned_df = clean_data(validated_df)
    transformed_df = transform_data(cleaned_df)
    aggregated_df = aggregate_data(transformed_df)
    write_output(aggregated_df, "taxi_output")
    spark.stop()

if __name__ == "__main__":
    main()