-- Insert test groups into raw_groups table
-- These groups correspond to the group_id values used in the users table

INSERT INTO raw_groups (group_id, tenant_id, group_name, group_type, created_date, is_active) VALUES
-- Group 1: Administrator group
('admin', 'tenant_id_001', 'Administrators', 'system', '2023-01-01', true),

-- Group 2: Regular user group  
('user', 'tenant_id_001', 'Regular Users', 'standard', '2023-01-01', true),

-- Group 3: Manager group
('manager', 'tenant_id_001', 'Managers', 'leadership', '2023-01-05', true),

-- Group 4: Support group
('support', 'tenant_id_001', 'Support Team', 'operational', '2023-01-10', true),

-- Group 3: test duplicate
('manager', 'tenant_id_001', 'Managers', 'leadership', '2023-01-05', true),

-- Group 5: Developer group
('developer', 'tenant_id_001', 'Developers', 'technical', '2023-01-15', true),
('analyst', 'tenant_id_001', 'Analysts', 'technical', '2023-01-15', true),
('admin', 'tenant_id_002', 'Administrators', 'system', '2023-02-01', true);
