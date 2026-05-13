{{ config(materialized='table') }} 

-- with orders as (
--     select * from {{ ref('raw_orders') }}
-- ),
with orders as (
    select * from {{ source('main', 'raw_orders') }}
),
payments as (
    select * from {{ source('main', 'raw_payments') }}
),

-- summary as (
--     select
--         user_id,
--         count(id) as order_count,
--         min(order_date) as first_order_date,
--         max(order_date) as last_order_date
--     from orders
--     group by user_id
-- )

-- summary as (
--     select * from orders
-- )

summary as (
    select 
    u.user_id,
    count(u.id) as order_count,
    min(u.order_date) as first_order_date,
    max(u.order_date) as last_order_date,
    sum(p.amount) as total_amount,
    if(sum(p.amount) > 0, 'completed', 'returned') as status
    from orders u
    left join payments p on u.id = p.order_id
    group by u.user_id
)

select * from summary