-- raw_active_users: Source table for active user data
-- This table contains raw user data for users who have logged in recently

CREATE TABLE IF NOT EXISTS raw_active_users (
    user_id BIGINT NOT NULL,
    username STRING NOT NULL,
    email STRING NOT NULL,
    group_id BIGINT NOT NULL,
    last_login_date DATE NOT NULL,
    first_name STRING,
    last_name STRING,
    registration_date DATE,
    phone_number STRING,
    country_code STRING,
    timezone STRING,
    preferred_language STRING,
    account_status STRING,
    login_count BIGINT,
    session_duration_minutes INT,
    last_ip_address STRING,
    user_agent STRING,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
) 
USING DELTA
PARTITIONED BY (DATE_TRUNC('MONTH', last_login_date))
TBLPROPERTIES (
    'description' = 'Raw active users data from source systems',
    'quality.expectations.user_id.not_null' = 'true',
    'quality.expectations.username.not_null' = 'true',
    'quality.expectations.email.not_null' = 'true',
    'quality.expectations.email.format' = 'email',
    'quality.expectations.last_login_date.not_null' = 'true'
);

-- Sample data insertion for testing
INSERT INTO raw_active_users (
    user_id,
    username,
    email,
    group_id,
    last_login_date,
    first_name,
    last_name,
    registration_date,
    phone_number,
    country_code,
    timezone,
    preferred_language,
    account_status,
    login_count,
    session_duration_minutes,
    last_ip_address,
    user_agent,
    created_date,
    updated_date
) VALUES 
(1001, 'admin_user', 'admin@company.com', 1, CURRENT_DATE - INTERVAL 1 DAYS, 'John', 'Admin', '2022-01-15', '+1-555-0101', 'US', 'America/New_York', 'en-US', 'ACTIVE', 245, 180, '192.168.1.10', 'Mozilla/5.0', current_timestamp(), current_timestamp()),
(1002, 'jane.doe', 'jane.doe@company.com', 2, CURRENT_DATE - INTERVAL 2 DAYS, 'Jane', 'Doe', '2022-03-20', '+1-555-0102', 'US', 'America/Los_Angeles', 'en-US', 'ACTIVE', 128, 95, '192.168.1.20', 'Chrome/120.0', current_timestamp(), current_timestamp()),
(1003, 'power_user1', 'power.user@company.com', 2, CURRENT_DATE - INTERVAL 1 DAYS, 'Bob', 'Power', '2022-05-10', '+1-555-0103', 'US', 'America/Chicago', 'en-US', 'ACTIVE', 89, 120, '192.168.1.30', 'Firefox/119.0', current_timestamp(), current_timestamp()),
(1004, 'standard.user', 'user@company.com', 3, CURRENT_DATE - INTERVAL 5 DAYS, 'Alice', 'Standard', '2023-01-05', '+1-555-0104', 'US', 'America/Denver', 'en-US', 'ACTIVE', 45, 60, '192.168.1.40', 'Safari/17.0', current_timestamp(), current_timestamp()),
(1005, 'beta_tester', 'beta@company.com', 5, CURRENT_DATE - INTERVAL 3 DAYS, 'Charlie', 'Beta', '2023-02-01', '+1-555-0105', 'CA', 'America/Toronto', 'en-CA', 'ACTIVE', 67, 140, '192.168.1.50', 'Edge/119.0', current_timestamp(), current_timestamp()),
(1006, 'guest.temp', 'guest@external.com', 4, CURRENT_DATE - INTERVAL 7 DAYS, 'Diana', 'Guest', '2023-11-01', '+44-20-7946-0958', 'UK', 'Europe/London', 'en-GB', 'ACTIVE', 12, 45, '10.0.0.15', 'Chrome/120.0', current_timestamp(), current_timestamp()),
(1007, 'frequent.user', 'frequent@company.com', 3, CURRENT_DATE - INTERVAL 1 DAYS, 'Eva', 'Frequent', '2022-08-15', '+49-30-12345678', 'DE', 'Europe/Berlin', 'de-DE', 'ACTIVE', 312, 200, '10.0.0.25', 'Firefox/119.0', current_timestamp(), current_timestamp()),
(1008, 'mobile.user', 'mobile@company.com', 3, CURRENT_DATE - INTERVAL 10 DAYS, 'Frank', 'Mobile', '2023-06-20', '+81-3-1234-5678', 'JP', 'Asia/Tokyo', 'ja-JP', 'ACTIVE', 156, 75, '203.0.113.10', 'Mobile Safari', current_timestamp(), current_timestamp()),
(1009, 'weekend.user', 'weekend@company.com', 2, CURRENT_DATE - INTERVAL 15 DAYS, 'Grace', 'Weekend', '2023-04-10', '+61-2-9876-5432', 'AU', 'Australia/Sydney', 'en-AU', 'ACTIVE', 78, 90, '203.0.113.20', 'Chrome Mobile', current_timestamp(), current_timestamp()),
(1010, 'night.owl', 'night@company.com', 3, CURRENT_DATE - INTERVAL 25 DAYS, 'Henry', 'Night', '2022-12-01', '+33-1-23-45-67-89', 'FR', 'Europe/Paris', 'fr-FR', 'ACTIVE', 234, 165, '203.0.113.30', 'Opera/105.0', current_timestamp(), current_timestamp());