INSERT INTO etf_hub_business_t
SELECT 
business_id,
dba_name,
user_id,
recipient_id,
email_address
FROM (
SELECT business_id, dba_name, user_id, recipient_id, email_address, $rowtime,
ROW_NUMBER() OVER (PARTITION BY business_id ORDER BY $rowtime DESC) as rn
FROM etf_hub_business)
WHERE rn = 1;