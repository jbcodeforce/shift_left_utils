[]

INSERT INTO "filtered-orders" SELECT * FROM orders WHERE order_sum > 100;