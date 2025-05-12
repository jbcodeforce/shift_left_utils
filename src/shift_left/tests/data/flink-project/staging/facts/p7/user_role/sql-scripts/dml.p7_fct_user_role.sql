```sql
INSERT INTO user_role
WITH 
  sys_user AS (SELECT * FROM int_sys_user_deduped),
  portal_role_member AS (SELECT * FROM int_portal_role_member_deduped),
  portal_role AS (SELECT * FROM int_portal_role_deduped),
  pu_final AS (
    SELECT 
        MD5(CONCAT_WS('', `sys_user`.`user_id`, `sys_user`.`__db`)) AS user_sid,
        MD5(CONCAT_WS('', `sys_user`.`user_id`, `portal_role`.`role_id`, `sys_user`.`__db`)) AS user_role_sid,
        `sys_user`.`user_id`,
        `portal_role`.`role_id` AS role_id,
        `portal_role`.`role_name` AS role_name,
        `portal_role`.`role_description` AS role_description,
        `sys_user`.`__db` AS tenant_id,
        GREATEST(`sys_user`.`__ts_ms`, `portal_role_member`.`__ts_ms`, `portal_role`.`__ts_ms`) AS __ts_ms,
        GREATEST(`sys_user`.`dl_landed_at`, `portal_role_member`.`dl_landed_at`, `portal_role`.`dl_landed_at`) AS dl_landed_at
    FROM sys_user
    LEFT JOIN portal_role_member
      ON `portal_role_member`.`member_id` = `sys_user`.`user_id`
        AND `portal_role_member`.`__db` = `sys_user`.`__db`
    LEFT JOIN portal_role
      ON `portal_role`.`role_id` = `portal_role_member`.`role_id`
        AND `portal_role`.`__db` = `portal_role_member`.`__db`
  ),
  roles_wo_users AS ( 
    SELECT
        MD5(CONCAT_WS('', 'null', `portal_role`.`__db`)) AS user_sid,
        MD5(CONCAT_WS('', 'null', `portal_role`.`role_id`, '__db')) AS user_role_sid,
        NULL AS user_id,
        `portal_role`.`role_id` AS role_id,
        `portal_role`.`role_name` AS role_name,
        `portal_role`.`role_description` AS role_description,
        `portal_role`.`__db` AS tenant_id,
        `portal_role`.`__ts_ms` AS __ts_ms,
        `portal_role`.`dl_landed_at` AS dl_landed_at
    FROM portal_role
    LEFT ANTI JOIN pu_final 
      ON `portal_role`.`role_id` = pu_final.role_id
        AND `portal_role`.`__db` = pu_final.tenant_id
  ),
  final AS (
    SELECT * FROM pu_final
    UNION ALL
    SELECT * FROM roles_wo_users
)
SELECT * FROM final;
```