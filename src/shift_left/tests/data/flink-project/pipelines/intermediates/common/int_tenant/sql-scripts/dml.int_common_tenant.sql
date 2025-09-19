INSERT INTO int_common_tenant
SELECT 
*
FROM src_common_tenant
WHERE op <> 'd'