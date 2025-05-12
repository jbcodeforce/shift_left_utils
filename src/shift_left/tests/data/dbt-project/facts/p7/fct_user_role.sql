with 
  sys_user as (select * from {{ ref('int_sys_user_deduped') }} )
, portal_role_member as (select * from {{ ref('int_portal_role_member_deduped') }} )
, portal_role as (select * from {{ ref('int_portal_role_deduped') }} )
, pu_final as (
    select 
        {{dbt_utils.surrogate_key(['sys_user.user_id', 'sys_user.__db'])}} as user_sid
        , {{dbt_utils.surrogate_key(['sys_user.user_id', 'portal_role.role_id', 'sys_user.__db'])}} as user_role_sid
        , sys_user.user_id
        , portal_role.role_id as role_id
        , portal_role.role_name as role_name
        , portal_role.role_description as role_description
        , sys_user.__db as tenant_id
        , greatest(sys_user.__ts_ms, portal_role_member.__ts_ms, portal_role.__ts_ms) as __ts_ms
        , greatest(sys_user.dl_landed_at, portal_role_member.dl_landed_at, portal_role.dl_landed_at) as dl_landed_at
    from sys_user
        left join portal_role_member
        on
            portal_role_member.member_id = sys_user.user_id and
            portal_role_member.__db = sys_user.__db
        left join portal_role
        on
            portal_role.role_id = portal_role_member.role_id and
            portal_role.__db = portal_role_member.__db
)
, roles_wo_users as ( 
    select
        {{dbt_utils.surrogate_key(['null', 'portal_role.__db'])}} as user_sid
        , {{dbt_utils.surrogate_key(['null', 'portal_role.role_id', '__db'])}} as user_role_sid
        , null as user_id
        , portal_role.role_id as role_id
        , portal_role.role_name as role_name
        , portal_role.role_description as role_description
        , portal_role.__db as tenant_id
        , portal_role.__ts_ms as __ts_ms
        , portal_role.dl_landed_at as dl_landed_at
    from portal_role
    left anti join pu_final on
            portal_role.role_id = pu_final.role_id and
            portal_role.__db = pu_final.tenant_id
)
, final as (
    select * from pu_final
    union all
    select * from roles_wo_users
)

select * from final