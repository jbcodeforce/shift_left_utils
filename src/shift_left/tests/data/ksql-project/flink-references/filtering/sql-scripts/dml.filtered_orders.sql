INSERT INTO filtered_orders
SELECT
    *
FROM orders
WHERE `order_sum` > 100;
