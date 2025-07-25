[
  "CREATE TABLE IF NOT EXISTS `orders` ("
  ",  )"
] 

{CREATE TABLE if not exists filtered_orders (
 order_time bigint,
 order_id string,
 customer_id int,
 product_id int,
 quantity int,
 order_sum decimal(38,12),
 PRIMARY KEY(order_time) NOT ENFORCED
)