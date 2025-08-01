-- raw_inactive_users: Source table for inactive user data
-- This table contains raw user data for users who haven't logged in recently

CREATE TABLE IF NOT EXISTS raw_inactive_users (
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
    'description' = 'Raw inactive users data from source systems',
    'quality.expectations.user_id.not_null' = 'true',
    'quality.expectations.username.not_null' = 'true',
    'quality.expectations.email.not_null' = 'true',
    'quality.expectations.email.format' = 'email',
    'quality.expectations.last_login_date.not_null' = 'true'
);

-- Sample data insertion for testing
INSERT INTO raw_inactive_users (
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
(2001, 'old.user1', 'old1@company.com', 3, CURRENT_DATE - INTERVAL 45 DAYS, 'Mark', 'Inactive', '2021-06-15', '+1-555-0201', 'US', 'America/New_York', 'en-US', 'INACTIVE', 89, 120, '192.168.2.10', 'Chrome/115.0', current_timestamp(), current_timestamp()),
(2002, 'dormant.account', 'dormant@company.com', 3, CURRENT_DATE - INTERVAL 60 DAYS, 'Lisa', 'Dormant', '2021-09-20', '+1-555-0202', 'US', 'America/Los_Angeles', 'en-US', 'INACTIVE', 156, 95, '192.168.2.20', 'Firefox/110.0', current_timestamp(), current_timestamp()),
(2003, 'vacation.user', 'vacation@company.com', 2, CURRENT_DATE - INTERVAL 35 DAYS, 'Tom', 'Vacation', '2022-01-10', '+1-555-0203', 'US', 'America/Chicago', 'en-US', 'VACATION', 234, 140, '192.168.2.30', 'Safari/16.0', current_timestamp(), current_timestamp()),
(2004, 'former.employee', 'former@company.com', 3, CURRENT_DATE - INTERVAL 90 DAYS, 'Sarah', 'Former', '2020-03-05', '+1-555-0204', 'US', 'America/Denver', 'en-US', 'TERMINATED', 445, 180, '192.168.2.40', 'Edge/115.0', current_timestamp(), current_timestamp()),
(2005, 'temp.contractor', 'temp@external.com', 4, CURRENT_DATE - INTERVAL 120 DAYS, 'Mike', 'Temp', '2023-01-01', '+44-20-7946-0959', 'UK', 'Europe/London', 'en-GB', 'EXPIRED', 23, 60, '10.0.1.15', 'Chrome/118.0', current_timestamp(), current_timestamp()),
(2006, 'legacy.account', 'legacy@oldcompany.com', 6, CURRENT_DATE - INTERVAL 180 DAYS, 'Nancy', 'Legacy', '2019-12-15', '+49-30-12345679', 'DE', 'Europe/Berlin', 'de-DE', 'LEGACY', 678, 220, '10.0.1.25', 'IE/11.0', current_timestamp(), current_timestamp()),
(2007, 'project.ended', 'project@company.com', 2, CURRENT_DATE - INTERVAL 75 DAYS, 'Paul', 'Project', '2022-05-20', '+81-3-1234-5679', 'JP', 'Asia/Tokyo', 'ja-JP', 'PROJECT_ENDED', 112, 105, '203.0.114.10', 'Chrome/117.0', current_timestamp(), current_timestamp()),
(2008, 'seasonal.worker', 'seasonal@company.com', 3, CURRENT_DATE - INTERVAL 150 DAYS, 'Quinn', 'Seasonal', '2021-11-10', '+61-2-9876-5433', 'AU', 'Australia/Sydney', 'en-AU', 'SEASONAL', 345, 130, '203.0.114.20', 'Firefox/115.0', current_timestamp(), current_timestamp()),
(2009, 'intern.summer', 'intern@company.com', 3, CURRENT_DATE - INTERVAL 100 DAYS, 'Rachel', 'Intern', '2023-05-01', '+33-1-23-45-67-90', 'FR', 'Europe/Paris', 'fr-FR', 'INTERNSHIP_ENDED', 67, 85, '203.0.114.30', 'Safari/16.5', current_timestamp(), current_timestamp()),
(2010, 'consultant.old', 'consultant@external.com', 4, CURRENT_DATE - INTERVAL 200 DAYS, 'Steve', 'Consultant', '2020-08-01', '+39-06-1234-5678', 'IT', 'Europe/Rome', 'it-IT', 'CONTRACT_ENDED', 234, 160, '203.0.114.40', 'Opera/100.0', current_timestamp(), current_timestamp());