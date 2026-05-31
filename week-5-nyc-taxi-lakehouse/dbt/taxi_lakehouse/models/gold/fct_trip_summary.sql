{{ config(materialized='table') }}

with silver as (
    select * from {{ ref('stg_taxi_trips') }}
),

summary as (
    select
        trip_category,
        count(*)                            as total_trips,
        round(sum(total_amount), 2)         as total_revenue,
        round(avg(total_amount), 2)         as avg_fare,
        round(avg(trip_distance), 2)        as avg_distance,
        round(avg(trip_duration_minutes), 2) as avg_duration_minutes,
        round(sum(total_amount) * 100.0 /
            sum(sum(total_amount)) over (), 2) as revenue_pct
    from silver
    group by trip_category
)

select * from summary
order by total_revenue desc