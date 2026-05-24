# Data Engineering Portfolio — Varsha Bang

## Structure
- week1-dbt: dbt Core learning — models, tests, materializations
- week2-airflow: Airflow learning — DAGs, sensors, XComs, dbt 
integration
- week3-kafka: Kafka learning — topics, partitions, consumer groups, Kafka→DuckDB pipeline
- week4-spark: PySpark learning — transformations, joins, window functions, end-to-end pipeline

## Stack
dbt Core, DuckDB, Apache Airflow, Apache Kafka, Redpanda, Docker, Python, Apache Spark

## Week 1 — dbt Core Learning

### What I built
- Extended jaffle shop seeds with custom models
- order_summary: aggregates order history per customer
- order_status_summary: order counts grouped by status
- customer_segments: buckets customers into value tiers

## Week 2 — Airflow Core Learning

### What I built
- my_first_dag: basic ETL with XCom data passing
- branching_dag: conditional logic with BranchPythonOperator
- sensor_dag: FileSensor with Airflow Variables
- dbt_pipeline: Airflow triggering dbt seed → run → test
- etl_with_quality_gate: combined pipeline with all patterns

## Week 3 — Kafka Core Learning

### What I built
- producer.py: basic producer sending taxi trip events
- consumer.py: basic consumer reading from topic
- partitioned_producer.py: key-based routing to partitions
- consumer_group.py: consumer group with partition assignment
- pipeline_producer.py: simulated real-time taxi trip events
- pipeline_consumer.py: consumer landing events into DuckDB
- kafka_pipeline_dag: Airflow DAG orchestrating full pipeline

## Week 4 — PySpark Core Learning

### What I built
- parquet downloaded curl -L https://github.com/duckdb/duckdb-data/releases/download/v1.0/taxi_2019_04.parquet \
  -o taxi.parquet
- first_spark_job.py: basic stats on NYC Taxi dataset
- transformations.py: cleaning, withColumn, filtering
- joins.py: DataFrame joins + aggregations
- nyc_taxi_pipeline.py: end-to-end modular pipeline