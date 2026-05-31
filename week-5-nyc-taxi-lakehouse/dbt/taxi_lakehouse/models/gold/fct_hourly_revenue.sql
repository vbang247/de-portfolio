{{  config(materialized='table') }}

with silver as (
    select * from {{ ref('stg_taxi_trips') }}
),

hourly as (
    select pickup_hour,
    rush_hour,
    count(*) as total_trips,
    round(sum(total_amount),2) as total_revenue,
    round(avg(total_amount), 2) as avg_fare,
    round(avg(trip_distance), 2) as avg_distance
    from silver 
    group by  pickup_hour, rush_hour

)

select * from hourly
order by pickup_hour