INSERT INTO W2_RETURNS_T
SELECT
    w2.*,
    returns.*,
    MAP_CONSTRUCTOR(
        'submissionDetails', 
        MAP_CONSTRUCTOR(
            'taxYear', returns.tax_year
        ),
        'returnData', 
        MAP_CONSTRUCTOR(
            'business', 
            MAP_CONSTRUCTOR(
                'businessId', CAST(returns.business_id AS STRING),
                'email', hub_biz.email_address
            ),
            'employee', 
            MAP_CONSTRUCTOR(
                'employeeId', CAST(w2.employee_id AS STRING),
                'ssn', w2.employee_ssn
            )
        )
    ) AS structured
FROM (
SELECT * FROM FORM_W2_T FOR SYSTEM_TIME AS OF ROWTIME() AND DEDUPLICATE ON employee_id
) w2
JOIN ETF_RETURNS_T returns ON w2.return_id = returns.return_id
LEFT JOIN ETF_HUB_BUSINESS_T hub_biz ON 
    PROCTIME() BETWEEN hub_biz.rowtime - INTERVAL '10' MINUTE AND FUTURE AND
    returns.business_id = hub_biz.business_id;