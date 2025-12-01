# W2 Processing - ksqlDB to Flink SQL Migration

This document contains the complete migration of `w2_processing.ksql` from ksqlDB to Apache Flink SQL.

## Source Streams Migration

### 1. FORM_W2

**ksqlDB (lines 23-34):**
```sql
CREATE OR REPLACE STREAM FORM_W2 (
  form_w2_id int KEY,
  return_id bigint,
  employee_id bigint,
  employee_ssn varchar
) WITH (
  KAFKA_TOPIC = 'form_w2',
  PARTITIONS=3,
  REPLICAS=3,
  KEY_FORMAT='avro',
  VALUE_FORMAT='avro'
);
```

**Flink DDL:**
```sql
CREATE TABLE IF NOT EXISTS form_w2 (
  form_w2_id INT,
  return_id BIGINT,
  employee_id BIGINT,
  employee_ssn STRING,
  PRIMARY KEY(form_w2_id) NOT ENFORCED
) DISTRIBUTED BY HASH(form_w2_id) INTO 3 BUCKETS WITH (
  'changelog.mode' = 'append',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
```

---

### 2. ETF_RETURNS

**ksqlDB (lines 47-61):**
```sql
CREATE OR REPLACE STREAM ETF_RETURNS (
  return_id bigint KEY,
  tax_year varchar,
  business_id varchar,
  recipient_id int,
  correction_type varchar,
  filing_status_id varchar,
  pdf_status boolean
) WITH (
  KAFKA_TOPIC = 'etf_returns',
  PARTITIONS=3,
  REPLICAS=3,
  KEY_FORMAT='avro',
  VALUE_FORMAT='avro'
);
```

**Flink DDL:**
```sql
CREATE TABLE IF NOT EXISTS etf_returns (
  return_id BIGINT,
  tax_year STRING,
  business_id STRING,
  recipient_id INT,
  correction_type STRING,
  filing_status_id STRING,
  pdf_status BOOLEAN,
  PRIMARY KEY(return_id) NOT ENFORCED
) DISTRIBUTED BY HASH(return_id) INTO 3 BUCKETS WITH (
  'changelog.mode' = 'append',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
```

---

### 3. ETF_HUB_BUSINESS

**ksqlDB (lines 70-82):**
```sql
CREATE OR REPLACE STREAM ETF_HUB_BUSINESS (
  business_id varchar KEY,
  dba_name varchar,
  user_id varchar,
  recipient_id int,
  email_address varchar
) WITH (
  KAFKA_TOPIC = 'etf_hub_business',
  PARTITIONS=3,
  REPLICAS=3,
  KEY_FORMAT='avro',
  VALUE_FORMAT='avro'
);
```

**Flink DDL:**
```sql
CREATE TABLE IF NOT EXISTS etf_hub_business (
  business_id STRING,
  dba_name STRING,
  user_id STRING,
  recipient_id INT,
  email_address STRING,
  PRIMARY KEY(business_id) NOT ENFORCED
) DISTRIBUTED BY HASH(business_id) INTO 3 BUCKETS WITH (
  'changelog.mode' = 'append',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
```

---

### 4. ETF_HUB_USERS

**ksqlDB (lines 90-100):**
```sql
CREATE OR REPLACE STREAM ETF_HUB_USERS (
  user_id varchar KEY,
  email_address varchar,
  contact_name varchar
) WITH (
  KAFKA_TOPIC = 'etf_hub_users',
  PARTITIONS=3,
  REPLICAS=3,
  KEY_FORMAT='avro',
  VALUE_FORMAT='avro'
);
```

**Flink DDL:**
```sql
CREATE TABLE IF NOT EXISTS etf_hub_users (
  user_id STRING,
  email_address STRING,
  contact_name STRING,
  PRIMARY KEY(user_id) NOT ENFORCED
) DISTRIBUTED BY HASH(user_id) INTO 3 BUCKETS WITH (
  'changelog.mode' = 'append',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
```

---

### 5. ETF_RECIPIENT

**ksqlDB (lines 108-119):**
```sql
CREATE OR REPLACE STREAM ETF_RECIPIENT (
  recipient_id int KEY,
  email_address varchar,
  recipient_telephone_no varchar,
  fax_number varchar
) WITH (
  KAFKA_TOPIC = 'etf_recipient',
  PARTITIONS=3,
  REPLICAS=3,
  KEY_FORMAT='avro',
  VALUE_FORMAT='avro'
);
```

**Flink DDL:**
```sql
CREATE TABLE IF NOT EXISTS etf_recipient (
  recipient_id INT,
  email_address STRING,
  recipient_telephone_no STRING,
  fax_number STRING,
  PRIMARY KEY(recipient_id) NOT ENFORCED
) DISTRIBUTED BY HASH(recipient_id) INTO 3 BUCKETS WITH (
  'changelog.mode' = 'append',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
```

---

## Derived Tables with LATEST_BY_OFFSET Pattern

### 6. FORM_W2_T

**ksqlDB (lines 127-140):**
```sql
CREATE OR REPLACE TABLE FORM_W2_T WITH (
  PARTITIONS=3,
  REPLICAS=3,
  KEY_FORMAT='avro',
  VALUE_FORMAT='avro'
) AS
  SELECT
    form_w2_id,
    LATEST_BY_OFFSET(return_id) AS return_id,
    LATEST_BY_OFFSET(employee_id) AS employee_id,
    LATEST_BY_OFFSET(employee_ssn) AS employee_ssn
  FROM FORM_W2
  GROUP BY form_w2_id
EMIT CHANGES;
```

**Flink DDL:**
```sql
CREATE TABLE IF NOT EXISTS form_w2_t (
  form_w2_id INT,
  return_id BIGINT,
  employee_id BIGINT,
  employee_ssn STRING,
  PRIMARY KEY(form_w2_id) NOT ENFORCED
) DISTRIBUTED BY HASH(form_w2_id) INTO 3 BUCKETS WITH (
  'changelog.mode' = 'retract',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
```

**Flink DML:**
```sql
INSERT INTO form_w2_t
SELECT
  form_w2_id,
  return_id,
  employee_id,
  employee_ssn
FROM (
  SELECT
    form_w2_id,
    return_id,
    employee_id,
    employee_ssn,
    ROW_NUMBER() OVER (PARTITION BY form_w2_id ORDER BY $rowtime DESC) as rn
  FROM form_w2
) WHERE rn = 1;
```

---

### 7. ETF_RETURNS_T

**ksqlDB (lines 143-159):**
```sql
CREATE OR REPLACE TABLE ETF_RETURNS_T WITH (
  PARTITIONS=3,
  REPLICAS=3,
  KEY_FORMAT='avro',
  VALUE_FORMAT='avro'
) AS
  SELECT
    return_id,
    LATEST_BY_OFFSET(tax_year) AS tax_year,
    LATEST_BY_OFFSET(business_id) AS business_id,
    LATEST_BY_OFFSET(recipient_id) AS recipient_id,
    LATEST_BY_OFFSET(correction_type) AS correction_type,
    LATEST_BY_OFFSET(filing_status_id) AS filing_status_id,
    LATEST_BY_OFFSET(pdf_status) AS pdf_status
  FROM ETF_RETURNS
  GROUP BY return_id
EMIT CHANGES;
```

**Flink DDL:**
```sql
CREATE TABLE IF NOT EXISTS etf_returns_t (
  return_id BIGINT,
  tax_year STRING,
  business_id STRING,
  recipient_id INT,
  correction_type STRING,
  filing_status_id STRING,
  pdf_status BOOLEAN,
  PRIMARY KEY(return_id) NOT ENFORCED
) DISTRIBUTED BY HASH(return_id) INTO 3 BUCKETS WITH (
  'changelog.mode' = 'retract',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
```

**Flink DML:**
```sql
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
  SELECT
    return_id,
    tax_year,
    business_id,
    recipient_id,
    correction_type,
    filing_status_id,
    pdf_status,
    ROW_NUMBER() OVER (PARTITION BY return_id ORDER BY $rowtime DESC) as rn
  FROM etf_returns
) WHERE rn = 1;
```

---

### 8. ETF_HUB_BUSINESS_T

**ksqlDB (lines 162-176):**
```sql
CREATE OR REPLACE TABLE ETF_HUB_BUSINESS_T WITH (
  PARTITIONS=3,
  REPLICAS=3,
  KEY_FORMAT='avro',
  VALUE_FORMAT='avro'
) AS
  SELECT
    business_id,
    LATEST_BY_OFFSET(dba_name) AS dba_name,
    LATEST_BY_OFFSET(user_id) AS user_id,
    LATEST_BY_OFFSET(recipient_id) AS recipient_id,
    LATEST_BY_OFFSET(email_address) AS email_address
  FROM ETF_HUB_BUSINESS
  GROUP BY business_id
EMIT CHANGES;
```

**Flink DDL:**
```sql
CREATE TABLE IF NOT EXISTS etf_hub_business_t (
  business_id STRING,
  dba_name STRING,
  user_id STRING,
  recipient_id INT,
  email_address STRING,
  PRIMARY KEY(business_id) NOT ENFORCED
) DISTRIBUTED BY HASH(business_id) INTO 3 BUCKETS WITH (
  'changelog.mode' = 'retract',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
```

**Flink DML:**
```sql
INSERT INTO etf_hub_business_t
SELECT
  business_id,
  dba_name,
  user_id,
  recipient_id,
  email_address
FROM (
  SELECT
    business_id,
    dba_name,
    user_id,
    recipient_id,
    email_address,
    ROW_NUMBER() OVER (PARTITION BY business_id ORDER BY $rowtime DESC) as rn
  FROM etf_hub_business
) WHERE rn = 1;
```

---

### 9. ETF_HUB_USERS_T

**ksqlDB (lines 178-190):**
```sql
CREATE OR REPLACE TABLE ETF_HUB_USERS_T WITH (
  PARTITIONS=3,
  REPLICAS=3,
  KEY_FORMAT='avro',
  VALUE_FORMAT='avro'
) AS
  SELECT
    user_id,
    LATEST_BY_OFFSET(email_address) AS email_address,
    LATEST_BY_OFFSET(contact_name) AS contact_name
  FROM ETF_HUB_USERS
  GROUP BY user_id
EMIT CHANGES;
```

**Flink DDL:**
```sql
CREATE TABLE IF NOT EXISTS etf_hub_users_t (
  user_id STRING,
  email_address STRING,
  contact_name STRING,
  PRIMARY KEY(user_id) NOT ENFORCED
) DISTRIBUTED BY HASH(user_id) INTO 3 BUCKETS WITH (
  'changelog.mode' = 'retract',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
```

**Flink DML:**
```sql
INSERT INTO etf_hub_users_t
SELECT
  user_id,
  email_address,
  contact_name
FROM (
  SELECT
    user_id,
    email_address,
    contact_name,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY $rowtime DESC) as rn
  FROM etf_hub_users
) WHERE rn = 1;
```

---

### 10. ETF_RECIPIENT_T

**ksqlDB (lines 192-205):**
```sql
CREATE OR REPLACE TABLE ETF_RECIPIENT_T WITH (
  PARTITIONS=3,
  REPLICAS=3,
  KEY_FORMAT='avro',
  VALUE_FORMAT='avro'
) AS
  SELECT
    recipient_id,
    LATEST_BY_OFFSET(email_address) AS email_address,
    LATEST_BY_OFFSET(recipient_telephone_no) AS recipient_telephone_no,
    LATEST_BY_OFFSET(fax_number) AS fax_number
  FROM ETF_RECIPIENT
  GROUP BY recipient_id
EMIT CHANGES;
```

**Flink DDL:**
```sql
CREATE TABLE IF NOT EXISTS etf_recipient_t (
  recipient_id INT,
  email_address STRING,
  recipient_telephone_no STRING,
  fax_number STRING,
  PRIMARY KEY(recipient_id) NOT ENFORCED
) DISTRIBUTED BY HASH(recipient_id) INTO 3 BUCKETS WITH (
  'changelog.mode' = 'retract',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
```

**Flink DML:**
```sql
INSERT INTO etf_recipient_t
SELECT
  recipient_id,
  email_address,
  recipient_telephone_no,
  fax_number
FROM (
  SELECT
    recipient_id,
    email_address,
    recipient_telephone_no,
    fax_number,
    ROW_NUMBER() OVER (PARTITION BY recipient_id ORDER BY $rowtime DESC) as rn
  FROM etf_recipient
) WHERE rn = 1;
```

---

## Complex Join Tables with STRUCT/MAP

### 11. W2_RETURNS_T

**ksqlDB (lines 280-306):**
```sql
CREATE OR REPLACE TABLE W2_RETURNS_T WITH (
  PARTITIONS=3,
  REPLICAS=3,
  KEY_FORMAT='avro',
  VALUE_FORMAT='avro'
) AS
  SELECT
    w2.*,
    returns.*,
    STRUCT(
      `submissionDetails` := STRUCT(
        `taxYear` := returns.tax_year
      ),
      `returnData` := STRUCT(
        `business` := MAP(
          'businessId' := returns.business_id
        ),
        `employee` := STRUCT(
          `employeeId` := w2.employee_id,
          `ssn` := w2.employee_ssn
        )
      )
    ) AS structured
FROM FORM_W2_T w2
JOIN ETF_RETURNS_T returns
  ON w2.return_id = returns.return_id
EMIT CHANGES;
```

**Flink DDL:**
```sql
CREATE TABLE IF NOT EXISTS w2_returns_t (
  form_w2_id INT,
  return_id BIGINT,
  employee_id BIGINT,
  employee_ssn STRING,
  tax_year STRING,
  business_id STRING,
  recipient_id INT,
  correction_type STRING,
  filing_status_id STRING,
  pdf_status BOOLEAN,
  structured ROW<
    submissionDetails ROW<taxYear STRING>,
    returnData ROW<
      business MAP<STRING, STRING>,
      employee ROW<employeeId BIGINT, ssn STRING>
    >
  >,
  PRIMARY KEY(form_w2_id) NOT ENFORCED
) DISTRIBUTED BY HASH(form_w2_id) INTO 3 BUCKETS WITH (
  'changelog.mode' = 'retract',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
```

**Flink DML:**
```sql
INSERT INTO w2_returns_t
SELECT
  w2.form_w2_id,
  w2.return_id,
  w2.employee_id,
  w2.employee_ssn,
  returns.tax_year,
  returns.business_id,
  returns.recipient_id,
  returns.correction_type,
  returns.filing_status_id,
  returns.pdf_status,
  ROW(
    ROW(returns.tax_year),
    ROW(
      MAP['businessId', returns.business_id],
      ROW(w2.employee_id, w2.employee_ssn)
    )
  ) AS structured
FROM form_w2_t w2
JOIN etf_returns_t returns FOR SYSTEM_TIME AS OF w2.$rowtime
  ON w2.return_id = returns.return_id;
```

---

### 12. W2_RETURNS2_T

**ksqlDB (lines 309-339):**
```sql
CREATE OR REPLACE TABLE W2_RETURNS2_T WITH (
  PARTITIONS=3,
  REPLICAS=3,
  KEY_FORMAT='json_sr',
  VALUE_FORMAT='json_sr'
) AS
  SELECT
    w2.*,
    returns.*,
    STRUCT(
      `submissionDetails` := STRUCT(
        `taxYear` := returns.tax_year
      ),
      `returnData` := STRUCT(
        `business` := MAP(
          'businessId' := returns.business_id
        ),
        `employee` := STRUCT(
          `employeeId` := w2.employee_id,
          `ssn` := w2.employee_ssn
        ),
        `states` := Array[
          'a',
          'b'
        ]
      )
    ) AS structured
FROM FORM_W2_T w2
JOIN ETF_RETURNS_T returns
  ON w2.return_id = returns.return_id
EMIT CHANGES;
```

**Flink DDL:**
```sql
CREATE TABLE IF NOT EXISTS w2_returns2_t (
  form_w2_id INT,
  return_id BIGINT,
  employee_id BIGINT,
  employee_ssn STRING,
  tax_year STRING,
  business_id STRING,
  recipient_id INT,
  correction_type STRING,
  filing_status_id STRING,
  pdf_status BOOLEAN,
  structured ROW<
    submissionDetails ROW<taxYear STRING>,
    returnData ROW<
      business MAP<STRING, STRING>,
      employee ROW<employeeId BIGINT, ssn STRING>,
      states ARRAY<STRING>
    >
  >,
  PRIMARY KEY(form_w2_id) NOT ENFORCED
) DISTRIBUTED BY HASH(form_w2_id) INTO 3 BUCKETS WITH (
  'changelog.mode' = 'retract',
  'key.format' = 'json-registry',
  'value.format' = 'json-registry',
  'key.json-registry.schema-context' = '.flink-dev',
  'value.json-registry.schema-context' = '.flink-dev',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
```

**Flink DML:**
```sql
INSERT INTO w2_returns2_t
SELECT
  w2.form_w2_id,
  w2.return_id,
  w2.employee_id,
  w2.employee_ssn,
  returns.tax_year,
  returns.business_id,
  returns.recipient_id,
  returns.correction_type,
  returns.filing_status_id,
  returns.pdf_status,
  ROW(
    ROW(returns.tax_year),
    ROW(
      MAP['businessId', returns.business_id],
      ROW(w2.employee_id, w2.employee_ssn),
      ARRAY['a', 'b']
    )
  ) AS structured
FROM form_w2_t w2
JOIN etf_returns_t returns FOR SYSTEM_TIME AS OF w2.$rowtime
  ON w2.return_id = returns.return_id;
```

---

## Data Insertion Statements

**ksqlDB (lines 211-219):**
```sql
INSERT INTO etf_hub_users (user_id, email_address, contact_name) VALUES ('guid-51', 'user1@user.com', 'User One');
INSERT INTO etf_recipient (recipient_id, email_address, recipient_telephone_no, fax_number) VALUES (81, 'recipient1@user.com', '111-222-3333', '111-222-4444');
INSERT INTO etf_hub_business (business_id, dba_name, user_id, recipient_id, email_address) VALUES ('guid-101', 'HUKO', 'guid-51', 81, 'email@huko.com');
INSERT INTO etf_returns (return_id, tax_year, business_id, recipient_id, correction_type, filing_status_id, pdf_status) VALUES (501, '2021', 'guid-101', 81, null, 'status-na', true);
INSERT INTO FORM_W2 (form_w2_id, return_id, employee_id, employee_ssn) VALUES (901, 501, 631, '555-666-8888');
```

**Flink SQL (same syntax):**
```sql
INSERT INTO etf_hub_users (user_id, email_address, contact_name) VALUES ('guid-51', 'user1@user.com', 'User One');
INSERT INTO etf_recipient (recipient_id, email_address, recipient_telephone_no, fax_number) VALUES (81, 'recipient1@user.com', '111-222-3333', '111-222-4444');
INSERT INTO etf_hub_business (business_id, dba_name, user_id, recipient_id, email_address) VALUES ('guid-101', 'HUKO', 'guid-51', 81, 'email@huko.com');
INSERT INTO etf_returns (return_id, tax_year, business_id, recipient_id, correction_type, filing_status_id, pdf_status) VALUES (501, '2021', 'guid-101', 81, null, 'status-na', true);
INSERT INTO form_w2 (form_w2_id, return_id, employee_id, employee_ssn) VALUES (901, 501, 631, '555-666-8888');
```

---

## Key Migration Notes

1. **LATEST_BY_OFFSET Pattern**: All uses of `LATEST_BY_OFFSET` with `GROUP BY` have been converted to `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY $rowtime DESC)` with `WHERE rn = 1`.

2. **Changelog Mode**: 
   - Source streams use `'changelog.mode' = 'append'`
   - Derived tables with aggregations use `'changelog.mode' = 'retract'`

3. **STRUCT Syntax**: ksqlDB's `:=` syntax for STRUCT field assignment becomes standard field ordering in Flink's `ROW()` constructor.

4. **MAP Syntax**: ksqlDB's `MAP('key' := 'value')` becomes Flink's `MAP['key', 'value']`.

5. **Join Semantics**: Stream-table joins require `FOR SYSTEM_TIME AS OF` for temporal semantics.

6. **Data Types**:
   - `VARCHAR` → `STRING`
   - `BIGINT` → `BIGINT`
   - `INT` → `INT`
   - `BOOLEAN` → `BOOLEAN`

7. **Table Names**: Converted to lowercase per Flink conventions.

8. **Ad-hoc SELECT Queries**: The various SELECT queries (lines 223-458) are exploratory queries and don't create tables, so they can be run as-is in Flink SQL with adjustments for temporal joins where needed.

---

## Deployment Order

Deploy tables in dependency order:

1. Source streams (form_w2, etf_returns, etf_hub_business, etf_hub_users, etf_recipient)
2. Derived tables with LATEST_BY_OFFSET (form_w2_t, etf_returns_t, etf_hub_business_t, etf_hub_users_t, etf_recipient_t)
3. Join tables (w2_returns_t, w2_returns2_t)
4. Insert test data


