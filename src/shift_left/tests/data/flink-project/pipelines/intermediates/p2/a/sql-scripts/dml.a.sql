INSERT INTO a
SELECT
a.default_key,
a.a_value,
x.x_value
FROM src_a as a
join src_x as x on a.x_key = x.default_key
