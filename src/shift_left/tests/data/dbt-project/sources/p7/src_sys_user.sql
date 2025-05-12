with sys_user as (select * from {{ source('p7','sys_user') }})

,final as (

    select 
        * 
    from sys_user
    {{limit_tenants()}}
    {{limit_ts_ms()}}

)

select * from final