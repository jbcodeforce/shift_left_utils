# AI based testing

## KSQL to Flink SQL migration

KSQL source files live under `tests/data/ksql-project/sources/`. The following table lists the KSQL files used in migration tests and the target table name.

| KSQL file | Table name | Test / description | status |
|-----------|------------|---------------------|---|
| aggregation.ksql | orders | Aggregation tutorial; outputs orders, daily_spend | ✅  |
| ddl-basic-table.ksql | (basic table/stream) | Basic CREATE TABLE/STREAM | |
| ddl-bigger-file.ksql | equipment | Multi-stream DDL (EQUIPMENT_STAGE_STREAM, ERRORS_TX, DISCARDS_RX) | |
| ddl-filtering.ksql | filtering | Filtering CREATE TABLE; reference filtered_orders | |
| ddl-geo.ksql | geolocation | Geolocation DDL | |
| ddl-g.ksql | table_struct | Struct/JSON handling | |
| ddl-kpi-config-table.ksql | KPI_CONFIG_TABLE | KPI config with latest offset | |
| ddl-measure_alert.ksql | (measurement alert) | Measurement KPI alert | |
| ddl-struct.ksql | (struct types) | Struct and nested types | |
| filtering.ksql | filtering | First-KSQL migration smoke test | |
| geospacial.ksql | (geospatial) | Geospatial operations | |
| merge_tutorial.ksql | all_songs | Confluent merge tutorial | |
| movements.ksql | (movements) | Movements stream | |
| splitter.ksql | splitter | Split stream by field | |
| splitting.ksql | (splitting) | Splitting tutorial variant | |
| w2_processing.ksql | w2_processing | W2 processing multi-statement | |

Source path is relative to `tests/data/ksql-project/sources/`. Staging output goes to `tests/data/flink-project/staging/ut/fromsql` (path set per test). Reference Flink SQL for some cases is under `tests/data/ksql-project/flink-references/`.

