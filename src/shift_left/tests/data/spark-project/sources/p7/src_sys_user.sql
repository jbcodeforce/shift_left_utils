with sys_user as (select * from {{ source('p7','cdc.users_db.sys_user') }})

,final as (

    select 
        * 
    from sys_user
    {{limit_tenants()}}
    {{limit_ts_ms()}}

)

select * from final