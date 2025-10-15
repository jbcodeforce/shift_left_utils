-- Test data for filtering logic
-- Testing aggregation and filtering where sum(amount) > 100
-- Expected results:
--   PASS (sum > 100): ORD001 ($150.00), ORD004 ($250.50), ORD006 ($101.25), ORD007 ($100.01)
--   FAIL (sum <= 100): ORD002 ($90.00), ORD003 ($100.00), ORD005 ($25.00), ORD008 ($0.00)

INSERT INTO app_orders VALUES 
    -- Test Case 1: Order that should PASS (sum = 150.00)
    ('ORD001', 'CUST001', 'PROD001', 75.00, TIMESTAMP '2024-01-01 10:00:00'),
    ('ORD001', 'CUST001', 'PROD002', 75.00, TIMESTAMP '2024-01-01 10:15:00'),
    
    -- Test Case 2: Order that should FAIL (sum = 90.00)
    ('ORD002', 'CUST002', 'PROD003', 50.00, TIMESTAMP '2024-01-01 11:00:00'),
    ('ORD002', 'CUST002', 'PROD004', 40.00, TIMESTAMP '2024-01-01 11:30:00'),
    
    -- Test Case 3: Edge case - exactly 100 should FAIL (not > 100)
    ('ORD003', 'CUST003', 'PROD005', 60.00, TIMESTAMP '2024-01-01 12:00:00'),
    ('ORD003', 'CUST003', 'PROD006', 40.00, TIMESTAMP '2024-01-01 12:15:00'),
    
    -- Test Case 4: Large order that should PASS (sum = 250.50)
    ('ORD004', 'CUST004', 'PROD007', 250.50, TIMESTAMP '2024-01-01 13:00:00'),
    
    -- Test Case 5: Small order that should FAIL (sum = 25.00)
    ('ORD005', 'CUST005', 'PROD008', 25.00, TIMESTAMP '2024-01-01 14:00:00'),
    
    -- Test Case 6: Multiple items that should PASS (sum = 101.25)
    ('ORD006', 'CUST006', 'PROD009', 30.50, TIMESTAMP '2024-01-01 15:00:00'),
    ('ORD006', 'CUST006', 'PROD010', 35.75, TIMESTAMP '2024-01-01 15:10:00'),
    ('ORD006', 'CUST006', 'PROD011', 35.00, TIMESTAMP '2024-01-01 15:20:00'),
    
    -- Test Case 7: Decimal precision test - should PASS (sum = 100.01)
    ('ORD007', 'CUST007', 'PROD012', 50.00, TIMESTAMP '2024-01-01 16:00:00'),
    ('ORD007', 'CUST007', 'PROD013', 50.01, TIMESTAMP '2024-01-01 16:05:00'),
    
    -- Test Case 8: Zero amount order - should FAIL (sum = 0.00)
    ('ORD008', 'CUST008', 'PROD014', 0.00, TIMESTAMP '2024-01-01 17:00:00');

