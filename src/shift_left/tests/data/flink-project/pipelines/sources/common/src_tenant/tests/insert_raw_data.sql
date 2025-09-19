execute statement SET
begin
insert into raw_tenants (key, source, op, ts_ms, after, before) values (
     cast('tenant_id_001' as  bytes),  -- key
    ROW(
        'v1',                          -- version
        'app_1',                      -- name
        1001,
        'db1',
        'tenants',
        true),
    'c',
    to_timestamp('2024-01-01T10:00:00.123','yyyy-MM-dd''T''HH:mm:ss.SSS'),
    ROW('tenant_id_001', 'tenant_A', 'tenant of the company A', 'Active', '2024-01-01T10:00:00.123', '2024-01-01T10:00:00.123'),  -- after
    CAST(NULL AS ROW<tenant_id STRING,
            tenant_name STRING,
            tenant_description STRING,
            tenant_status STRING,
            tenant_created_at STRING,
            tenant_updated_at STRING>)  -- before
);
insert into raw_tenants (key, source, op, ts_ms,  after, before) values (
     cast('tenant_id_002' as  bytes),  -- key
        ROW(
        'v1',                          -- version
        'app_1',                      -- name
        1002,
        'db1',
        'tenants',
        false),
    'c',
    to_timestamp('2024-01-01T10:00:00.123','yyyy-MM-dd''T''HH:mm:ss.SSS'),
    ROW('tenant_id_002', 'tenant_B', 'tenant of the company B', 'Active', '2024-02-01T10:00:00.123', '2024-02-01T10:00:00.123'),  -- data
    CAST(NULL AS ROW<tenant_id STRING,
            tenant_name STRING,
            tenant_description STRING,
            tenant_status STRING,
            tenant_created_at STRING,
            tenant_updated_at STRING>)  
);
insert into raw_tenants (key, source, op, ts_ms,  after, before) values (
     cast('tenant_id_003' as  bytes),  -- key
        ROW(
        'v1',                          -- version
        'app_1',                      -- name
        1003,
        'db1',
        'tenants',
        false),
    'c',
    to_timestamp('2024-01-01T10:00:00.123','yyyy-MM-dd''T''HH:mm:ss.SSS'),
    ROW('tenant_id_003', 'tenant_C', 'tenant of the company C', 'Active', '2024-03-01T10:00:00.123', '2024-03-01T10:00:00.123'),  -- data
    CAST(NULL AS ROW<tenant_id STRING,
            tenant_name STRING,
            tenant_description STRING,
            tenant_status STRING,
            tenant_created_at STRING,
            tenant_updated_at STRING>)  -- before
);
insert into raw_tenants (key, source, op, ts_ms, after, before) values (
     cast('tenant_id_002' as  bytes),  -- key
           ROW(
        'v1',                          -- version
        'app_1',                      -- name
        1004,
        'db1',
        'tenants',
        false),
    'u',
    to_timestamp('2024-03-02T10:00:00.123','yyyy-MM-dd''T''HH:mm:ss.SSS'),
    ROW('tenant_id_002', 'tenant_Bic', 'tenant of the company Bic', 'Active', '2024-02-01T10:00:00.123', '2024-03-02T10:00:00.123'),  -- data
    ROW('tenant_id_002', 'tenant_B', 'tenant of the company B', 'Active', '2024-02-01T10:00:00.123', '2024-02-01T10:00:00.123')  -- beforeData
);
-- duplicate update
insert into raw_tenants (key, source, op, ts_ms, after, before) values (
     cast('tenant_id_002' as  bytes),  -- key
           ROW(
        'v1',                          -- version
        'app_1',                      -- name
        1005,  -- demonstrate duplicate update
        'db1',
        'tenants',
        false),
    'u',
    to_timestamp('2024-03-02T10:00:00.123','yyyy-MM-dd''T''HH:mm:ss.SSS'),
    ROW('tenant_id_002', 'tenant_Bic', 'tenant of the company Bic', 'Active', '2024-02-01T10:00:00.123', '2024-03-02T10:00:00.123'),  -- data
    ROW('tenant_id_002', 'tenant_B', 'tenant of the company B', 'Active', '2024-02-01T10:00:00.123', '2024-02-01T10:00:00.123')  -- beforeData
);
insert into raw_tenants (key, source, op, ts_ms, after, before) values (
     cast('tenant_id_001' as  bytes),  -- key
    ROW(
        'v1',                          -- version
        'app_1',                      -- name
        1006,
        'db1',
        'tenants',
        true),
    'd',
    to_timestamp('2024-04-01T12:00:00.123','yyyy-MM-dd''T''HH:mm:ss.SSS'),
    CAST(NULL AS ROW<tenant_id STRING,
            tenant_name STRING,
            tenant_description STRING,
            tenant_status STRING,
            tenant_created_at STRING,
            tenant_updated_at STRING>),  -- after
     ROW('tenant_id_001', 'tenant_A', 'tenant of the company A', 'Active', '2024-01-01T10:00:00.123', '2024-01-01T10:00:00.123') -- beore
   
);

END