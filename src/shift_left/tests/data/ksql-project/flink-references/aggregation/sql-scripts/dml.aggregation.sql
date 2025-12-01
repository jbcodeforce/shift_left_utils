INSERT INTO orders
SELECT
    window_start,
    window_end,
    customer_id,
    SUM(order_amount) as order_amount_sum
FROM TABLE(TUMBLE(TABLE daily_spend, DESCRIPTOR($rowtime), INTERVAL '86400' SECOND))
WHERE order_amount > 0
GROUP BY customer_id, window_start, window_end;
