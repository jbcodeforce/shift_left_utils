with result as (
    select *, count(*) as row_num from fct_order 
    where id = 'user_id_5' and account_name = 'account of mathieu' and balance = 190
) 
select * from result where row_num = 1;