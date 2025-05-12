with 
portal_role_member as (select * from {{ source('mc_qx','portal_role_member') }})

,final as (

    select 
        * 
    from portal_role_member
    {{limit_tenants()}}
    {{limit_ts_ms()}}

)

select * from final
