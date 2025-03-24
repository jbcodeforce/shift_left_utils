with result as (
    select *, count(*) as row_num from fct_order 
    where id = 'user_id_1' and account_name = 'account of bob'
) 
select * from result where row_num = 1;
