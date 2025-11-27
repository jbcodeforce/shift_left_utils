INSERT INTO w2_returns2_t
SELECT 
    w2.return_id,
    w2.employee_id,
    w2.employee_ssn,
    returns.tax_year,
    returns.business_id,
    ROW_NUMBER() OVER (PARTITION BY w2.return_id ORDER BY w2.$rowtime DESC) as rn,
    STRUCT(
        taxyear = returns.tax_year
    ) as submissiondetails,
    STRUCT(
        business = MAP('businessId', returns.business_id),
        employee = STRUCT(
            employeeid = w2.employee_id,
            ssn = w2.employee_ssn
        ),
        states = ARRAY['a', 'b']
    ) as returndata
FROM form_w2_t w2
JOIN etf_returns_t returns ON w2.return_id = returns.return_id
WHERE rn = 1;