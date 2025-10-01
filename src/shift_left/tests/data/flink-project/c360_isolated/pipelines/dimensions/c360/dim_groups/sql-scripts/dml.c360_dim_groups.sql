INSERT INTO c360_dim_groups
SELECT 
  group_id,
  tenant_id,
  group_name,
  group_type,
  tenant_name,
  created_date,
  is_active,
  updated_at
FROM src_c360_groups g
join src_common_tenant tenant
on g.tenant_id = tenant.tenant_id
