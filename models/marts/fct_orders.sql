{{ config(materialized='table') }}

select
    o.order_id,
    o.customer_id,
    o.ordered_at,
    o.status,
    o.order_total,
    o.currency,
    c.country,
    c.first_order_date
from {{ ref('stg_orders') }} as o
left join {{ ref('stg_customers') }} as c
    on o.customer_id = c.customer_id
