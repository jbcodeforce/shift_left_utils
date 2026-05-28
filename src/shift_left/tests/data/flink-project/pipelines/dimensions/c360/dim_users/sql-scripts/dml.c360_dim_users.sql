
INSERT INTO sl_c360_dim_users
with valid_users as (
  SELECT * FROM sl_c360_src_users
  WHERE user_id IS NOT NULL and group_id IS NOT NULL and tenant_id IS NOT NULL
)
  SELECT
    u.user_id,
    u.user_name,
    u.user_email,
    u.group_id,
    g.tenant_id,
    g.tenant_name,
    g.group_name,
    g.group_type,
    u.created_date,
    u.is_active
  FROM valid_users u
  LEFT JOIN sl_c360_dim_groups g
  ON  u.tenant_id = g.tenant_id and u.group_id = g.group_id
  WHERE g.tenant_id is not null and u.group_id is not null;

