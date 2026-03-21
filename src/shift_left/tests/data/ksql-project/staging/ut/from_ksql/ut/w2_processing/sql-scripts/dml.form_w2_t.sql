INSERT INTO form_w2_t SELECT form_w2_id, return_id, employee_id, employee_ssn FROM (
    SELECT form_w2_id, return_id, employee_id, employee_ssn,
           ROW_NUMBER() OVER (PARTITION BY form_w2_id ORDER BY $rowtime DESC) AS rn
    FROM form_w2
) WHERE rn = 1;

INSERT INTO etf_returns_t SELECT return_id, tax_year, business_id, recipient_id, correction_type, filing_status_id, pdf_status FROM (
    SELECT return_id, tax_year, business_id, recipient_id, correction_type, filing_status_id, pdf_status,
           ROW_NUMBER() OVER (PARTITION BY return_id ORDER BY $rowtime DESC) AS rn
    FROM etf_returns
) WHERE rn = 1;

INSERT INTO etf_hub_business_t SELECT business_id, dba_name, user_id, recipient_id, email_address FROM (
    SELECT business_id, dba_name, user_id, recipient_id, email_address,
           ROW_NUMBER() OVER (PARTITION BY business_id ORDER BY $rowtime DESC) AS rn
    FROM etf_hub_business
) WHERE rn = 1;

INSERT INTO etf_hub_users_t SELECT user_id, email_address, contact_name FROM (
    SELECT user_id, email_address, contact_name,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY $rowtime DESC) AS rn
    FROM etf_hub_users
) WHERE rn = 1;

INSERT INTO etf_recipient_t SELECT recipient_id, email_address, recipient_telephone_no, fax_number FROM (
    SELECT recipient_id, email_address, recipient_telephone_no, fax_number,
           ROW_NUMBER() OVER (PARTITION BY recipient_id ORDER BY $rowtime DESC) AS rn
    FROM etf_recipient
) WHERE rn = 1;

INSERT INTO w2_returns_t
SELECT w2.form_w2_id, w2.return_id, w2.employee_id, w2.employee_ssn,
       returns.tax_year, returns.business_id, returns.recipient_id,
       returns.correction_type, returns.filing_status_id, returns.pdf_status,
       ROW<
           submissionDetails ROW<taxYear STRING>,
           returnData ROW<
               business MAP<STRING, STRING>,
               employee ROW<employeeId BIGINT, ssn STRING>
           >
       >(
           ROW('2021'),
           ROW(
               MAP('businessId', 'guid-101'),
               ROW(631, '555-666-8888')
           )
       )
FROM form_w2_t w2
JOIN etf_returns_t returns ON w2.return_id = returns.return_id;

INSERT INTO w2_returns2_t
SELECT w2.form_w2_id, w2.return_id, w2.employee_id, w2.employee_ssn,
       returns.tax_year, returns.business_id, returns.recipient_id,
       returns.correction_type, returns.filing_status_id, returns.pdf_status,
       ROW<
           submissionDetails ROW<taxYear STRING>,
           returnData ROW<
               business MAP<STRING, STRING>,
               employee ROW<employeeId BIGINT, ssn STRING>,
               states ARRAY<STRING>
           >
       >(
           ROW('2021'),
           ROW(
               MAP('businessId', 'guid-101'),
               ROW(631, '555-666-8888'),
               ARRAY['a', 'b']
           )
       )
FROM form_w2_t w2
JOIN etf_returns_t returns ON w2.return_id = returns.return_id;

INSERT INTO etf_hub_users (user_id, email_address, contact_name) VALUES ('guid-51', 'user1@user.com', 'User One');

INSERT INTO etf_recipient (recipient_id, email_address, recipient_telephone_no, fax_number) VALUES (81, 'recipient1@user.com', '111-222-3333', '111-222-4444');

INSERT INTO etf_hub_business (business_id, dba_name, user_id, recipient_id, email_address) VALUES ('guid-101', 'HUKO', 'guid-51', 81, 'email@huko.com');

INSERT INTO etf_returns (return_id, tax_year, business_id, recipient_id, correction_type, filing_status_id, pdf_status) VALUES (501, '2021', 'guid-101', 81, null, 'status-na', true);

INSERT INTO form_w2 (form_w2_id, return_id, employee_id, employee_ssn) VALUES (901, 501, 631, '555-666-8888');