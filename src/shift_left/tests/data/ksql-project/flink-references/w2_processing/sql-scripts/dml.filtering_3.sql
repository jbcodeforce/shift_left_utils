INSERT INTO etf_hub_users_t
SELECT user_id, email_address, contact_name 
FROM (
    SELECT user_id,
           email_address,
           contact_name,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY $rowtime DESC) AS rn 
    FROM etf_hub_users
)
WHERE rn = 1;