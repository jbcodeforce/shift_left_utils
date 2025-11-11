EXECUTE STATEMENT SET
BEGIN
INSERT INTO c360_dim_users
with valid_users as (
  SELECT * FROM src_c360_users
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
  LEFT JOIN c360_dim_groups g
  ON  u.tenant_id = g.tenant_id and u.group_id = g.group_id;
INSERT INTO c360_dim_user_dql
SELECT
  user_id,
  user_name,
  user_email,
  group_id,
  tenant_id,
  tenant_name,
  group_name,
  group_type,
  created_date,
  is_active
FROM c360_dim_users where user_id is NULL or tenant_id is NULL or group_id is NULL;
END;
