INSERT INTO z
SELECT
y.default_key,
concat(y.y_value,'-', x.x_value) as z_value

FROM y
join x on y.x_key = x.default_key
