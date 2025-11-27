INSERT INTO etf_returns_t
SELECT 
    return_id,
    tax_year,
    business_id,
    recipient_id,
    correction_type,
    filing_status_id,
    pdf_status
FROM (
    SELECT return_id, tax_year, business_id, recipient_id, correction_type, filing_status_id, pdf_status,
           ROW_NUMBER() OVER (PARTITION BY return_id ORDER BY $rowtime DESC) as rn
    FROM etf_returns
) WHERE rn = 1;