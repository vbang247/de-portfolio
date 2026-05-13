
{{ config(materialized='table') }}

with customer_orders as (
    select * from {{ ref('orders_summary') }}
),

segmented as (
    select user_id,
    order_count,
    total_amount,
    first_order_date,
    last_order_date,
    case
        when total_amount >= 50 then 'high_value'
        when total_amount >= 20 then 'medium_value'
    else 'low_value'
    end as segment
    from customer_orders
)

select * from segmented