INSERT INTO KPI_CONFIG_TABLE
SELECT 
    `dbTable`,
    kpiName,
    kpiStatus,
    networkService,
    elementType,
    interfaceName
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY `dbTable` ORDER BY $rowtime DESC) AS rn
    FROM BASIC_TABLE_STREAM
)
WHERE rn = 1;