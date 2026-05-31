{{  config(materialized='table') }}

with bronze as (
    select * from {{ source('bronze', 'taxi_trips_raw') }}
),
cleaned as (
    select 
    row_number() over() as trip_id,
    pickup_at,
    dropoff_at,
    passenger_count,
    trip_distance,
    total_amount,
    tip_amount,
    tolls_amount,
    date_diff('minute', pickup_at, dropoff_at) as trip_duration_minutes,
    CASE 
        WHEN trip_distance < 2 then 'short'
        when trip_distance < 10 then 'medium'
        else 'long'
    end as trip_category,
    hour(pickup_at) as pickup_hour,
    CASE 
        WHEN hour(pickup_at) between 7 and 9 OR hour(pickup_at) between 17 and 19 then true
        else false
    end as rush_hour,
    _source_file,
    _ingested_at
    from bronze
    where passenger_count > 0
    and total_amount > 0
    and trip_distance > 0
    and total_amount < 1000
    and date(pickup_at) < '2020-01-01'
    and date(pickup_at) > '2000-01-01'
)
select * from cleaned



