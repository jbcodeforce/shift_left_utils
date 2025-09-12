-- Insert test groups into raw_groups table
-- These groups correspond to the group_id values used in the users table

INSERT INTO raw_groups (group_id, group_name, group_type, created_date, is_active) VALUES
-- Group 1: Administrator group
('admin', 'Administrators', 'system', '2023-01-01', true),

-- Group 2: Regular user group  
('user', 'Regular Users', 'standard', '2023-01-01', true),

-- Group 3: Manager group
('manager', 'Managers', 'leadership', '2023-01-05', true),

-- Group 4: Support group
('support', 'Support Team', 'operational', '2023-01-10', true),

-- Group 3: test duplicate
('manager', 'Managers', 'leadership', '2023-01-05', true),

-- Group 5: Developer group
('developer', 'Developers', 'technical', '2023-01-15', true);
