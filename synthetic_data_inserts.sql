-- Synthetic Data INSERT Statements for AQEM Step Role Fact Table
-- =============================================================

-- 1. Insert synthetic rule configurations
INSERT INTO `src_aqem_recordconfiguration_rule_configuration` (id, node_id, tenant_id, rule_type, created_at, updated_at) VALUES
(1, 'node_001', 'tenant_001', 'NODE_PARTICIPANT_PERMISSION', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(2, 'node_002', 'tenant_001', 'EVENT_INITIATOR_PERMISSION', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(3, 'node_003', 'tenant_002', 'NODE_PARTICIPANT_PERMISSION', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(4, 'node_004', 'tenant_001', 'NODE_PARTICIPANT_PERMISSION', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(5, 'node_005', 'tenant_002', 'EVENT_INITIATOR_PERMISSION', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(6, 'node_006', 'tenant_001', 'OTHER_PERMISSION_TYPE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'); -- This should be filtered out

-- 2. Insert synthetic rule conditions
INSERT INTO `src_aqem_recordconfiguration_rule_condition` (id, rule_configuration_id, tenant_id, condition_name, created_at, updated_at) VALUES
(101, 1, 'tenant_001', 'role_assignment_condition', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(102, 2, 'tenant_001', 'site_assignment_condition', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(103, 3, 'tenant_002', 'department_assignment_condition', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(104, 4, 'tenant_001', 'role_assignment_condition', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(105, 5, 'tenant_002', 'site_assignment_condition', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(106, 6, 'tenant_001', 'ignored_condition', '2024-01-01 10:00:00', '2024-01-01 10:00:00'); -- This should be filtered out

-- 3. Insert synthetic rule operands (LEFT operands - define assignee types)
INSERT INTO `src_aqem_recordconfiguration_rule_operand` (id, rule_condition_id, tenant_id, operand_type, target_value, created_at, updated_at) VALUES
-- LEFT operands (define the assignee type)
(201, 101, 'tenant_001', 'LEFT', 'ROLE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(202, 102, 'tenant_001', 'LEFT', 'SITE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(203, 103, 'tenant_002', 'LEFT', 'DEPARTMENT', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(204, 104, 'tenant_001', 'LEFT', 'ROLE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(205, 105, 'tenant_002', 'LEFT', 'SITE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(206, 106, 'tenant_001', 'LEFT', 'INVALID_TYPE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'), -- This should be filtered out
-- RIGHT operands (contain the assignee IDs in array format)
(301, 101, 'tenant_001', 'RIGHT', '[admin_role,manager_role,user_role]', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(302, 102, 'tenant_001', 'RIGHT', '[site_001,site_002]', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(303, 103, 'tenant_002', 'RIGHT', '[dept_hr,dept_finance,dept_it]', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(304, 104, 'tenant_001', 'RIGHT', '[supervisor_role]', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(305, 105, 'tenant_002', 'RIGHT', '[site_003,site_004,site_005]', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
(306, 106, 'tenant_001', 'RIGHT', '[]', '2024-01-01 10:00:00', '2024-01-01 10:00:00'), -- Empty array - should be filtered out
(307, 101, 'tenant_001', 'RIGHT', '[executive_role]', '2024-01-01 10:00:00', '2024-01-01 10:00:00'); -- Additional RIGHT operand for same condition

-- 4. Insert synthetic nodes (filtered view)
INSERT INTO `int_aqem_recordconfiguration_node_filtered` (id, tenant_id, node_name, node_type, status, created_at, updated_at) VALUES
('node_001', 'tenant_001', 'Approval Step 1', 'APPROVAL', 'ACTIVE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
('node_002', 'tenant_001', 'Review Step 1', 'REVIEW', 'ACTIVE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
('node_003', 'tenant_002', 'Approval Step 2', 'APPROVAL', 'ACTIVE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
('node_004', 'tenant_001', 'Final Approval', 'APPROVAL', 'ACTIVE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
('node_005', 'tenant_002', 'Initial Review', 'REVIEW', 'ACTIVE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
('node_006', 'tenant_001', 'Ignored Node', 'OTHER', 'INACTIVE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'),
('node_007', 'tenant_001', 'Orphan Node', 'APPROVAL', 'ACTIVE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'), -- No rules defined for this node
('node_008', 'tenant_002', 'Another Orphan', 'REVIEW', 'ACTIVE', '2024-01-01 10:00:00', '2024-01-01 10:00:00'); -- No rules defined for this node

-- =============================================================
-- Expected Results Summary:
-- =============================================================
-- This synthetic data should generate the following step-role assignments:
--
-- node_001 (tenant_001) -> ROLE assignments: admin_role, manager_role, user_role, executive_role
-- node_002 (tenant_001) -> SITE assignments: site_001, site_002  
-- node_003 (tenant_002) -> DEPARTMENT assignments: dept_hr, dept_finance, dept_it
-- node_004 (tenant_001) -> ROLE assignments: supervisor_role
-- node_005 (tenant_002) -> SITE assignments: site_003, site_004, site_005
-- node_007 (tenant_001) -> No assignments (orphan node)
-- node_008 (tenant_002) -> No assignments (orphan node)
--
-- The query should produce approximately 13 rows in the final result:
-- - 4 rows for node_001 (4 role assignments)
-- - 2 rows for node_002 (2 site assignments) 
-- - 3 rows for node_003 (3 department assignments)
-- - 1 row for node_004 (1 role assignment)
-- - 3 rows for node_005 (3 site assignments)
-- - 2 rows for orphan nodes (with NULL assignee values)
--
-- Total: ~13 rows in aqem_fct_step_role 