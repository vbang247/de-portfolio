import duckdb
import os

def verify_data(db_path: str):
    print("Verifying bronze data")
    conn = duckdb.connect(db_path)
    conn.sql("SHOW TABLES from bronze").show()

    print("Getting row count")
    row_count = conn.execute("SELECT COUNT(*) from bronze.taxi_trips_raw").fetchone()[0]
    print(f"Row count: {row_count}")

    print("Null values check")

    conn.sql(

        """
        select COUNT(*) as total_rows,
        COUNT(pickup_at) as pick_at_not_null,
        COUNT(total_amount) as total_amount_not_null,
        COUNT(passenger_count) as passenger_count_not_null
        from bronze.taxi_trips_raw
        """
    ).show()
    print("Closing connection...")
    conn.close()

if __name__ == "__main__":
    DB_PATH = os.environ.get("DB_PATH", "data/taxi_lakehouse.duckdb")
    verify_data(DB_PATH)