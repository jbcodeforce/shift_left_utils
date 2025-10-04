-- Insert statements for daily_spend table to trigger 24-hour tumbling window
-- Data is spread across multiple 24-hour windows to trigger window firing

INSERT INTO daily_spend (customer_id, order_amount, category, transaction_id, tx_timestamp)
VALUES 
    -- Day 1: First 24-hour window (2024-01-01 00:00:00 to 2024-01-01 23:59:59)
    ('C001', 150.00, 'Electronics', 'TXN001', TIMESTAMP '2024-01-01 08:30:00.000'),
    ('C001', 75.50, 'Books', 'TXN002', TIMESTAMP '2024-01-01 10:15:00.000'),
    ('C002', 220.00, 'Clothing', 'TXN003', TIMESTAMP '2024-01-01 12:45:00.000'),
    ('C001', 45.25, 'Food', 'TXN004', TIMESTAMP '2024-01-01 16:20:00.000'),
    ('C003', 300.00, 'Electronics', 'TXN005', TIMESTAMP '2024-01-01 18:30:00.000'),
    ('C002', 85.75, 'Home', 'TXN006', TIMESTAMP '2024-01-01 20:10:00.000'),
    
    -- Day 2: Second 24-hour window (2024-01-02 00:00:00 to 2024-01-02 23:59:59)
    -- These inserts will trigger the first window to close and emit results
    ('C001', 120.00, 'Electronics', 'TXN007', TIMESTAMP '2024-01-02 09:00:00.000'),
    ('C002', 55.00, 'Books', 'TXN008', TIMESTAMP '2024-01-02 11:30:00.000'),
    ('C003', 180.50, 'Clothing', 'TXN009', TIMESTAMP '2024-01-02 14:15:00.000'),
    ('C004', 95.00, 'Sports', 'TXN010', TIMESTAMP '2024-01-02 17:45:00.000'),
    ('C001', 200.00, 'Electronics', 'TXN011', TIMESTAMP '2024-01-02 19:20:00.000'),
    
    -- Day 3: Third 24-hour window (2024-01-03 00:00:00 onwards)
    -- These inserts will trigger the second window to close and emit results
    ('C002', 130.00, 'Home', 'TXN012', TIMESTAMP '2024-01-03 08:00:00.000'),
    ('C003', 75.00, 'Food', 'TXN013', TIMESTAMP '2024-01-03 10:30:00.000'),
    ('C004', 250.00, 'Electronics', 'TXN014', TIMESTAMP '2024-01-03 13:45:00.000'),
    ('C001', 90.00, 'Books', 'TXN015', TIMESTAMP '2024-01-03 16:00:00.000');

-- Expected aggregated results per 24-hour window:
-- Window 1 (2024-01-01): C001: 270.75, C002: 305.75, C003: 300.00
-- Window 2 (2024-01-02): C001: 320.00, C002: 55.00, C003: 180.50, C004: 95.00
-- Window 3 (2024-01-03): C001: 90.00, C002: 130.00, C003: 75.00, C004: 250.00
