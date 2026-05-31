import os
import duckdb

def ingest_raw_data(raw_data_path: str, db_path: str):
    print("Connecting to DuckDB...")
    conn = duckdb.connect(db_path)
    print("Creating schema...")
    conn.sql(
        """
        CREATE SCHEMA IF NOT EXISTS bronze;
        """
 )
    print(f"Creating table using parquet file: {raw_data_path}")
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE bronze.taxi_trips_raw AS
        SELECT *,
        '{raw_data_path}' AS _source_file,
        current_timestamp AS _ingested_at,
        FROM read_parquet('{raw_data_path}')
        """
    )

    count = conn.execute("SELECT COUNT(*) from bronze.taxi_trips_raw").fetchone()[0]
    print(f"Ingested {count} rows from {raw_data_path}")

    print("Schema:")
    conn.sql("DESCRIBE bronze.taxi_trips_raw").show()

    print("Validating data...")
    conn.sql("SELECT * from bronze.taxi_trips_raw limit 10").show()

    print("Closing connection...")
    conn.close()

if __name__ == "__main__":
    RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", "data/raw/taxi_2019_04.parquet")
    DB_PATH = os.environ.get("DB_PATH", "data/taxi_lakehouse.duckdb")
    ingest_raw_data(RAW_DATA_PATH, DB_PATH)