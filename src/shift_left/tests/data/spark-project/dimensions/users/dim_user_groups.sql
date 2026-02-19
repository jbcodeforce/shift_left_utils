-- dim_user_groups: User Groups Dimension Table
-- This dimension table contains information about user groups and their attributes

CREATE TABLE IF NOT EXISTS dim_user_groups (
    group_id BIGINT NOT NULL,
    group_name STRING NOT NULL,
    group_type STRING NOT NULL,
    created_date DATE NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    description STRING,
    max_members INT,
    permissions ARRAY<STRING>,
    created_by STRING,
    updated_date TIMESTAMP,
    updated_by STRING
);

-- Create unique index on group_id for better performance
-- ALTER TABLE dim_user_groups ADD CONSTRAINT unique_group_id UNIQUE (group_id);

-- Sample data insertion for testing
INSERT INTO dim_user_groups (
    group_id,
    group_name,
    group_type,
    created_date,
    is_active,
    description,
    max_members,
    permissions,
    created_by,
    updated_date,
    updated_by
) VALUES
(1, 'Administrators', 'ADMIN', '2023-01-01', true, 'System administrators with full access', 10, array('READ', 'WRITE', 'DELETE', 'ADMIN'), 'system', current_timestamp(), 'system'),
(2, 'Power Users', 'POWER_USER', '2023-01-01', true, 'Users with elevated privileges', 50, array('READ', 'WRITE'), 'system', current_timestamp(), 'system'),
(3, 'Standard Users', 'STANDARD', '2023-01-01', true, 'Regular users with standard access', 1000, array('READ'), 'system', current_timestamp(), 'system'),
(4, 'Guest Users', 'GUEST', '2023-01-01', true, 'Temporary access for external users', 100, array('READ'), 'system', current_timestamp(), 'system'),
(5, 'Beta Testers', 'BETA', '2023-02-01', true, 'Users testing new features', 25, array('READ', 'WRITE'), 'system', current_timestamp(), 'system'),
(6, 'Deprecated Group', 'LEGACY', '2022-01-01', false, 'Old group no longer in use', 0, array(), 'system', current_timestamp(), 'system');
