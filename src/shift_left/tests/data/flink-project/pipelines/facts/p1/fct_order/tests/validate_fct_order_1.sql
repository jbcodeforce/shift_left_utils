with result_table as (
    select * from fct_order
    where id = 'user_id_1' and account_name = 'account of bob'
) 

SELECT CASE WHEN count(*)=1 THEN 'PASS' ELSE 'FAIL' END from result_table;
