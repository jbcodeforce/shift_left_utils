CREATE TABLE IF NOT EXISTS int_aqem_tag_tag_dummy_ut (
    id                 STRING NOT NULL,
    tenant_id          STRING NOT NULL,
    status             STRING,
    name               STRING,
    `type`             STRING,
    created_by         STRING,
    created_date       BIGINT,
    last_modified_by   STRING,
    PRIMARY KEY(id) NOT ENFORCED
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS;