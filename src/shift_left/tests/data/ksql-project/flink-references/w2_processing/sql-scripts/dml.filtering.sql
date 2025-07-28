INSERT INTO form_w2_t
SELECT 
    w.form_w2_id,
    w.return_id as return_id,
    w.employee_id as employee_id,
    w.employee_ssn as employee_ssn
FROM (
    SELECT * FROM form_w2 FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP
) as w
GROUP BY w.form_w2_id;