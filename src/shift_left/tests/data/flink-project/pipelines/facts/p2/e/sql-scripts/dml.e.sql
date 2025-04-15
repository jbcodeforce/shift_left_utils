INSERT INTO e
SELECT 
c.default_key,
'e-value' as e_field,
c.c_value
FROM c
