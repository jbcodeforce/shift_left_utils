INSERT INTO d
SELECT 
y.default_key,
'd-value' as d_value,
count(*) as sum_value
FROM y  
join z on y.x_key = z.default_key

