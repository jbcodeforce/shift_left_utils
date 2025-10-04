INSERT INTO orders
SELECT 
    customer_id,
    SUM(order_amount) AS sum_order_amount
FROM (
    SELECT *
    FROM TABLE(TUMBLE(TABLE "daily-spend", DESCRIPTOR($rowtime), INTERVAL '24' HOUR))
    WHERE order_amount > 0
) AS windowed_data
GROUP BY customer_id, TUMBLE_START
;