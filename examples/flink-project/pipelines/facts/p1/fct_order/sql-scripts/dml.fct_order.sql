INSERT INTO fct_order
with cte_table as (
    SELECT
      d,b,a
    FROM int_table_1
)
SELECT  
    a,b,c 
from cte_table
left join int_table_2;