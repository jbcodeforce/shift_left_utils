CREATE TABLE IF NOT EXISTS etf_hub_users_t (
   `user_id` BIGINT, 
   `email_address` STRING,
   `contact_name` STRING,
   PRIMARY KEY(`user_id`) NOT ENFORCED 
)
WITH ('connector' = 'filesystem',
'path' = '/default_db/etf_hub_users_t',
'format' = 'avro')
DISTRIBUTED BY HASH(user_id) INTO 1 BUCKETS;