INSERT INTO w2_returns_t
SELECT 
    w2.return_id,
    w2.employee_id,
    w2.employee_ssn,
    returns.tax_year,
    returns.business_id,
    ROW(
        ROW(returns.tax_year),
        ROW(
            MAP('businessId', returns.business_id),
            ROW(w2.employee_id, w2.employee_ssn)
        )
    ) AS structured
FROM form_w2_t AS w2
JOIN etf_returns_t AS returns
ON w2.return_id = returns.return_id;