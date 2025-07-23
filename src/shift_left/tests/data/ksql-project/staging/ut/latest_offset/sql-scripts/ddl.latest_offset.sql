INSERT INTO KPI_CONFIG_TABLE
SELECT 
  `dbTable`,
  kpiName,
  kpiStatus,
  networkService,
  elementType,
  interfaceName,
  sourceName
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY `dbTable` ORDER BY PROCTIME()) AS rn
  FROM BASIC_TABLE_STREAM
) t
WHERE rn = 1;