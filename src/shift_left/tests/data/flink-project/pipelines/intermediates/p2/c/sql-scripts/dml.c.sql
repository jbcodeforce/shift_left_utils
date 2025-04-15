INSERT INTO c
SELECT 
b.b_value
concat_ws('-', b.b_value, 'c-value') as c_value
FROM b  
union all
select 
z.default_key,
z.z_value,
concat_ws('-', z.z_value, 'c-value') as c_value
from z