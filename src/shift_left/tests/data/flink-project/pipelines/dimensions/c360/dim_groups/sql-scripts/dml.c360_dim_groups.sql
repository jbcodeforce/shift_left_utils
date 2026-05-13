INSERT INTO sl_c360_dim_groups
SELECT
  group_id,
  tenant_id,
  group_name,
  group_type,
  tenant_name,
  created_date,
  is_active,
  updated_at
FROM sl_c360_src_groups g
join sl_cmn_src_tenants tenant
on g.tenant_id = sl_tenant.tenant_id
