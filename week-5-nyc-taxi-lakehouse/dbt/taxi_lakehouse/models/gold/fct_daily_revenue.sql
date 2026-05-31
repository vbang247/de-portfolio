{{ config(materialized='table') }}

with silver as (
    select * from {{ ref('stg_taxi_trips') }}
),

daily as (
    select
        cast(pickup_at as date)   as trip_date,
        count(*)                        as total_trips,
        round(sum(total_amount), 2)     as total_revenue,
        round(avg(total_amount), 2)     as avg_fare
    from silver
    group by cast(pickup_at as date)
)

select * from daily
order by trip_date