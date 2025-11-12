INSERT INTO p
SELECT
	z.default_key,
	'p-value' as p_value
FROM z
