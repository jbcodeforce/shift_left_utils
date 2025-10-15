INSERT INTO filtered_orders
SELECT 
    order_id,
    customer_id,
    order_sum,
    order_date
FROM orders
WHERE order_sum > 100;