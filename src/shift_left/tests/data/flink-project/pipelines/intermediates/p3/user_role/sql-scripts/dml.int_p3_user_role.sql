INSERT INTO int_p3_user_role
WITH
    users as (
        select user_id,
               tenant_id,
               role_id,
               status
        from src_p3_users 
        left join src_p3_tenants on src_p3_users.tenant_id = src_p3_tenants.id
    ),
    roles as (
        select role_id
               role_name,
               u.tenant_id,
               u.user_id,
               u.status
        from src_p3_roles
        left join users u on src_p3_roles.role_id = u.role_id
    )
SELECT * FROM roles;
