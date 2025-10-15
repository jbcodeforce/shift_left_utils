INSERT INTO filtered_orders

SELECT 
   *
FROM app_orders
WHERE order_sum > 100;