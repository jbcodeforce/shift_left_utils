INSERT INTO int_tag

WITH tag_src AS (
    SELECT
        id,
        tenant_id,
        status,
        name,
        `type`,
        created_by,
        created_date,
        last_modified_by,
        last_modified_date
    FROM src_tag
    WHERE type <> 'TESTDATA'
),
tenants AS (
        SELECT t.id, t.__db AS tenant_id
        FROM tenant_dimension AS t
),
other_tags AS (
    SELECT
        mc_tags.id,
        tenants.tenant_id,
        status,
        name,
        type,
        created_by,
        created_date,
        last_modified_by,
        last_modified_date
    FROM src_tag AS mc_tags
    CROSS JOIN tenants
    WHERE type = 'TESTDATA'
)

SELECT
    id,
    tenant_id,
    status,
    name,
    type,
    created_by,
    created_date,
    last_modified_by,
    last_modified_date
FROM tag_src
UNION ALL
SELECT
    id,
    tenant_id,
    status,
    name,
    type,
    created_by,
    created_date,
    last_modified_by,
    last_modified_date
FROM other_tags;