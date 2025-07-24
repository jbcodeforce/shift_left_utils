with portal_role as (select * from {{ source('p7','portal_role') }})

,final as (

    select 
        * 
    from portal_role
    {{limit_tenants()}}
    {{limit_ts_ms()}}

)

select * from final