insert into src_c360_groups_ut
(`group_id`, `tenant_id`, `group_name`, `group_type`, `created_date`, `is_active`, `updated_at`)
values
('group_id_1', 'tenant_id_1', 'group_name_1', 'group_type_1', 'created_date_1', false, TIMESTAMP '2021-01-01 00:00:00'),
('group_id_2', 'tenant_id_2', 'group_name_2', 'group_type_2', 'created_date_2', true, TIMESTAMP '2021-01-01 00:00:00'),
('group_id_3', 'tenant_id_3', 'group_name_3', 'group_type_3', 'created_date_3', false, TIMESTAMP '2021-01-01 00:00:00');