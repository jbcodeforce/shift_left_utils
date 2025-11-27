INSERT INTO form_w2_t
SELECT 
    form_w2_id,
    return_id,
    employee_id,
    employee_ssn
FROM (
    SELECT form_w2_id, return_id, employee_id, employee_ssn,
           ROW_NUMBER() OVER (PARTITION BY form_w2_id ORDER BY $rowtime DESC) as rn
    FROM form_w2
) WHERE rn = 1;