with result_table as (
    select * from fct_order
    where id = 'user_id_5' and account_name = 'account of mathieu' and balance = 190
) 
SELECT CASE WHEN count(*)=1 THEN 'PASS' ELSE 'FAIL' END from result_table;