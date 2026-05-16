-- Insert test users into raw_users table
-- This includes some duplicates to test deduplication logic

INSERT INTO sl_raw_users (user_id, user_name, user_email, group_id, tenant_id, created_date, is_active) VALUES
-- User 1: Initial record
('user_001', 'Alice Johnson', 'alice.johnson@example.com', 'admin', 'tenant_id_001', '2023-01-15', true),

-- User 2: Regular user
('user_002', 'Bob Smith', 'bob.smith@example.com', 'user', 'tenant_id_001', '2023-02-20', true),

-- User 3: Another admin
('user_003', 'Carol Williams', 'carol.williams@example.com', 'admin', 'tenant_id_001', '2023-03-10', false),

-- User 4: Inactive user
('user_004', 'David Brown', 'david.brown@example.com', 'user', 'tenant_id_001', '2023-04-05', false),

-- User 5: Manager role
('user_005', 'Emma Davis', 'emma.davis@example.com', 'manager', 'tenant_id_001', '2023-05-12', true),

-- User 1 DUPLICATE: Same user_id but newer created_date (this should be kept by dedup logic)
('user_001', 'Alice Johnson-Updated', 'alice.johnson.new@example.com', 'admin', 'tenant_id_001', '2023-06-01', true),

-- User 6: Guest user
('user_006', 'Frank Miller', 'frank.miller@example.com', 'guest', 'tenant_id_001', '2023-06-15', true),

-- User 7: Support role
('user_007', 'Grace Wilson', 'grace.wilson@example.com', 'support', 'tenant_id_001', '2023-07-20', true),

-- User 2 DUPLICATE: Same user_id but older created_date (should be filtered out by dedup logic)
('user_002', 'Bob Smith-Old', 'bob.smith.old@example.com', 'user', 'tenant_id_001', '2023-01-10', true),

-- User 8: Analyst role
('user_008', 'Henry Taylor', 'henry.taylor@example.com', 'analyst', 'tenant_id_001', '2023-08-25', true),

-- User 9: Developer role
('user_009', 'Ivy Anderson', 'ivy.anderson@example.com', 'developer', 'tenant_id_001', '2023-09-30', true),

-- User 10: QA role
('user_010', 'Jack Thomas', 'jack.thomas@example.com', 'qa', 'tenant_id_001', '2023-10-15', false),
('user_011', 'Katherine White', 'katherine.white@example.com', 'qa', 'tenant_id_002', '2024-01-15', true),
('user_012', 'Liam Brown', 'liam.brown@example.com', 'qa', 'tenant_id_003', '2024-02-20', false),
('user_013', 'Mia Green', 'mia.green@example.com', 'qa', 'tenant_id_001', '2024-03-25', true),
('user_014', 'Noah Hall', 'noah.hall@example.com', 'qa', 'tenant_id_002', '2024-04-30', false),
('user_015', 'Olivia Johnson', 'olivia.johnson@example.com', 'qa', 'tenant_id_003', '2024-05-05', true),
('user_016', 'Paul Smith', 'paul.smith@example.com', 'qa', 'tenant_id_001', '2024-06-10', false),
('user_017', 'Quincy Williams', 'quincy.williams@example.com', 'qa', 'tenant_id_002', '2024-07-15', true),
('user_018', 'Rose Davis', 'rose.davis@example.com', 'qa', 'tenant_id_003', '2024-08-20', false),
('user_019', 'Samuel Wilson', 'samuel.wilson@example.com', 'qa', 'tenant_id_001', '2024-09-25', true),
('user_020', 'Taylor Brown', 'taylor.brown@example.com', 'qa', 'tenant_id_002', '2024-10-30', false);
