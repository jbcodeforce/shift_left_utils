INSERT INTO fct_order
SELECT  
    a,b,c 
from int_table_1
left join int_table_2;