INSERT INTO sl_c360_dim_groups
SELECT
  group_id,
  t.tenant_id,
  group_name,
  group_type,
  tenant_name,
  created_date,
  is_active,
  updated_at
FROM sl_c360_src_groups as g
JOIN sl_cmn_src_tenants as t
    ON g.tenant_id = t.tenant_id
WHERE t.tenant_id IS NOT NULL;
