INSERT INTO p1_fct_order
with cte_table as (
    SELECT
      order_id,
      product_id ,
      customer_id ,
      amount
    FROM int_p1_table_2
)
SELECT  
    coalesce(c.id,'N/A') as id,
    c.user_name,
    c.account_name,
    c.balance - ct.amount as balance
from cte_table ct
left join int_p1_table_1 c on ct.customer_id = c.id;