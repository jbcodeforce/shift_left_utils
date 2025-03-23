INSERT INTO fct_order
with cte_table as (
    SELECT
      order_id,
      product_id ,
      customer_id ,
      amount
    FROM int_table_2
)
SELECT  
    c.id,
    c.customer_name,
    c.account_name,
    c.balance - ct.amount as balance 
from cte_table ct
left join int_table_1 c on ct.customer_id = c.id;