insert into sl_c360_fct_user_per_group
select
    tenant_id,
    group_id,
    group_name,
    COUNT(*) as total_users,
    SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) as active_users,
    SUM(CASE WHEN is_active = false THEN 1 ELSE 0 END) as inactive_users
from sl_c360_dim_users
group by tenant_id, group_id, group_name, is_active
