INSERT INTO filtered_orders
with aggregated_orders as (
    select 
        order_id,
        customer_id,
        sum(amount) as order_sum,
        max(order_ts) as order_date
    from app_orders
    group by order_id, customer_id
)
SELECT 
    order_id,
    customer_id,
    order_sum,
    order_date
FROM aggregated_orders
WHERE order_sum > 100;