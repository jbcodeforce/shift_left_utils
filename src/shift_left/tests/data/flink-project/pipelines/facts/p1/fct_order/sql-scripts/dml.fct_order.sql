INSERT INTO fct_order
with cte_table as (
    SELECT
      d,b,a
    FROM int_table_1
)
SELECT  
    cte.a,cte.b,cte.d  
from cte_table ct
left join int_table_2 i2 on ;