{{
    config(
        materialized='incremental',
        unique_key='user_id'
    )
}}

with orders as (
    select * from {{ ref('raw_orders') }}
    {% if is_incremental() %}
    where order_date > (select max(last_order_date) from {{ this }})
    {% endif %}
),

summary as (
    select user_id, 
    count(id) as order_count,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date
    from orders
    group by user_id
)
select * from summary