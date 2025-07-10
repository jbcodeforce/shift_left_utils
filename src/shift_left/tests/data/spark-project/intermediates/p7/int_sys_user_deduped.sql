with sys_user as ({{ dedup(ref('src_sys_user'), 'user_id') }})
    ,final as (
        select * from sys_user
            qualify row_number() over(partition by __db, upper(user_name) order by __ts_ms desc) = 1)

select * from final;