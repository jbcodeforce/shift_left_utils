INSERT INTO kpi_config_table SELECT dbtable, dbtable0, kpiname, kpistatus, networkservice, elementtype, interfacename FROM (
    SELECT dbtable, dbtable0, kpiname, kpistatus, networkservice, elementtype, interfacename,
           ROW_NUMBER() OVER (PARTITION BY dbtable ORDER BY $rowtime DESC) as rn
    FROM basic_table_stream
) WHERE rn = 1;