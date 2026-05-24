from kafka import KafkaConsumer, KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import json
import random
import time
import duckdb


def produce():
    producer = KafkaProducer(
        bootstrap_servers='redpanda:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

    count = 0
    TRIP_STATUS = [ 'completed', 'completed', 'completed', 'cancelled']
    ZONES = ['manhattan', 'brooklyn', 'queens', 'bronx', 'staten_island']

    for i in range(1, 21):
        trip = {
            "trip_id": i,
            "status": random.choice(TRIP_STATUS),
            "zone": random.choice(ZONES),
            "passenger_count": random.randint(1, 5),
            "amount": round(random.uniform(10, 100), 2),
            "pickup_datetime": datetime.now().isoformat(),
        }
        producer.send(topic="taxi-events-raw", key=trip['zone'], value=trip)
        count += 1
        print(f"Sent trip {trip['trip_id']} to zone {trip['zone']}")
        time.sleep(0.5)
    producer.flush()
    print("All messages sent")

def consume():
    conn = duckdb.connect('pipeline.duckdb')
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS taxi_events_raw (
        trip_id INTEGER,
        status VARCHAR,
        zone VARCHAR,
        passenger_count INTEGER,
        amount DOUBLE,
        pickup_datetime TIMESTAMP,
        ingested_at TIMESTAMP DEFAULT current_timestamp
        )
        """
    )
    consumer = KafkaConsumer(
        "taxi-events-raw",
        bootstrap_servers='redpanda:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id="airflow-consumer",
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
    )

    for message in consumer:
        trip = message.value
        conn.sql(
            """
            INSERT INTO taxi_events_raw (trip_id, status, zone, passenger_count, amount, pickup_datetime)
            VALUES (?, ?, ?, ?, ?, ?)
            """, params=
            [
                trip['trip_id'],
                trip['status'],
                trip['zone'],
                trip['passenger_count'],
                trip['amount'],
                trip['pickup_datetime'],
            ]
        )
        print(f"Received trip {trip['trip_id']} from zone {trip['zone']}")
        time.sleep(0.5)
    consumer.close()
    print("All messages consumed")
    conn.close()

def verify():
    print("Verifying pipeline...")
    conn=duckdb.connect('pipeline.duckdb')
    print("Querying pipeline...")
    print(conn.sql(
        """
        select zone, 
        count(*) as total_trips, 
        avg(amount) as avg_fare, 
        sum(case when status = 'completed' then 1 else 0 end) as completed_trips 
        from taxi_events_raw 
        group by zone 
        order by total_trips desc
        """
    ).show())
    conn.close()

with DAG(
    dag_id="kafa_pipeline_dag",
    start_date=datetime(2026, 5, 17),
    schedule="@daily",
    catchup=False
)as dag:
    start = EmptyOperator(task_id="start_run")

    producer = PythonOperator(
        task_id="kafka_producer",
        python_callable=produce
    )

    consumer = PythonOperator(
        task_id="kafka_consumer",
        python_callable=consume
    )

    verify = PythonOperator(
        task_id="verify_pipeline",
        python_callable=verify
    )

    end = EmptyOperator(task_id="end_run")

    start >> producer >> consumer >> verify >> end