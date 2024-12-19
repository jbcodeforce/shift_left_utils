

## Summary of commands

This section is quick summary of the full content of this readme.

* Get the parent hierarchy for a fact or dimension table:

```shs
python find_pipeline.py -f ../../de-datawarehouse-main/models/facts/qx/fct_training_jobcode_unit.sql
```

* Generate from dbt source tables the matching Flink SQL DDL and Dedup DMLs into a temporary folder to finalize manually to the final pipelines folder:

```sh
python process_src_tables.py -f ../../de-datawarehouse-main/models/sources -o to_process_staging/sources
```

* Generate Flink SQL from one dbt file to the staging temporary folder

```sh
python process_src_tables.py -f ../../de-datawarehouse-main/models/facts/qx/fct_training_jobcode_unit.sql -o to_process_staging/qx
```

* Once the Source DDL is executed successful, generate test data, 5 records, to send to the matching topic. It is 

```sh
# -o is for the output file to get the json array of json objects to be send to the topic, -t is for table name, and -n is for the number of records to create
python generate_data_for_table.py -o data.json -t tdc_sys_user_raw -n 5 
```

* Send the test data to the target topic

```sh
# -t for the topic name and -s source for prtal 
python kafka_avro_producer.py -t portal_role_raw -s ../pipelines/sources/portal_role
```

## Source and target folder structure

The dbt project contains all the SQL statements for migration, located in the models folder. The objective of the tools is to process these files and replicate the same organizational structure for Flink SQL statements, which includes sources, intermediates, staging, dimensions, and facts. Additionally, the tools aim to automate parts of the migration process


The target structure will look like in the following example:

```sh
pipelines
├── dimensions
│   └── qx
│       ├── dim_trainee
│       │   ├── Makefile
│       │   ├── sql-scripts
│       │   │   ├── ddl.dim_trainee.sql
│       │   │   └── dml.dim_trainee.sql
│       │   └── tests
│       └── dim_training_data
│           ├── Makefile
│           ├── sql-scripts
│           │   ├── ddl.dim_training_data.sql
│           │   └── dml.dim_training_data.sql
│           └── tests
├── facts
│   └── qx
│       ├── fct_training_data
│       │   ├── Makefile
│       │   ├── sql-scripts
│       │   │   ├── ddl.fct_training_data.sql
│       │   │   └── dml.fct_training_data.sql
│       │   └── tests
│       └── fct_user_role
│           ├── Makefile
│           ├── connector_config
│           ├── sql-scripts
│           │   ├── ddl.fct_user_role.sql
│           │   └── dml.fct_user_role.sql
│           └── tests
├── intermediates
│   ├── int_training_data_unformatted
│   │   ├── Makefile
│   │   ├── sql-scripts
│   │   │   ├── ddl.int_training_data_unformatted.sql
│   │   │   └── dml.int_training_data_unformatted.sql
│   │   └── tests
│   └── qx
├── sources
│   ├── exam_data
│   │   ├── Makefile
│   │   ├── dedups
│   │   │   └── dml.exam_data.sql
│   │   └── tests
│   │       └── ddl.exam_data.sql
│   ├── mc_version
│   │   ├── Makefile
│   │   ├── dedups
│   │   │   └── dml.mc_version.sql
│   │   └── tests
│   │       └── ddl.mc_version.sql
│   ├── portal_role
│   │   ├── Makefile
│   │   ├── dedups
│   │   │   └── dml.portal_role.sql
│   │   └── tests
│   │       ├── data.json
│   │       └── ddl.portal_role.sql
│   ├── portal_role_member
│   │   ├── Makefile
│   │   ├── dedups
│   │   │   └── dml.portal_role_member.sql
│   │   └── tests
│   │       └── ddl.portal_role_member.sql
│   ├── tdc_sys_user
│   │   ├── Makefile
│   │   ├── dedups
│   │   │   └── dml.tdc_sys_user.sql
│   │   └── tests
│   │       ├── data.json
│   │       ├── data.tdc_sys_user.sql
│   │       └── ddl.tdc_sys_user.sql

```

## 1- From a dbt file get all the parent tables

The tool is `find_pipeline.py` which needs only one argument, the sql file name of the fact or dimension table:

```sh
# under utils folder
python find_pipeline.py -f ../../de-datawarehouse-main/models/facts/qx/fct_user_role.sql
```

Should return something like:

('src_tdc_sys_user', None)
('int_portal_role_deduped', '../../de-datawarehouse-main/models/intermediates/dedups/int_portal_role_deduped.sql')
('int_tdc_sys_user_deduped', '../../de-datawarehouse-main/models/intermediates/dedups/int_tdc_sys_user_deduped.sql')
('int_portal_role_member_deduped', '../../de-datawarehouse-main/models/intermediates/dedups/int_portal_role_member_deduped.sql')
('src_portal_role_member', None)
('src_portal_role', None)

Source tables have no parent. 

## Create one Flink sql from one dbt sql file

This tool is based on local Ollama server with the `qwen2.5-coder:32b` LLM and some agents working with different prompts. This tool does not need to be used for migration, it is more for prompt tuning. There is a more complete process in the following section that uses function of this python module withing other tools that help automating the migration. 

```sh
python flink_sql_code_agent_lg.py -n dbt-file.sql
```

## Process a hierarchy from one single sink sql file

The `process_src_tables.py` is really a set of integrated functions to process a lot of source dbt files and help to migrate more efficiently. It uses 
the LLM LangGraph workflow as defined in `flink_sql_code_agent_lg.py`

The command arguments are:

| arg | meaning |
| --- | --- |
| -f  or --folder_path | name of the folder or file name including the tables to migrate. This Could be a unique .sql file only |
| -o  or --pipeline_folder_path | name of the folder output of the pipelines. Mandatory |
| -t or --table_name | name of the table to process - as dependencies are derived from previous, it is useful to give the table name as parameter and search for the file then process the sql file. This is to follow the process table by table |
| -ld |  default= False, For the given file or folder, list for each table their table dependancies |
| -pd' | default= False, For the given file, process also its dependencies so a full graph is created NOT SUPPORTED" |


**Attention** `-pd` is for taking a table and process all its parent up to all sources: it can be long and give a lot of file to process. It is not yet fully implemented.

The following example will create a DML based on the same logic as the one in the sql script, and will add corresponding DDL to create the sink table:

```sh
python process_src_tables.py -f ../../de-datawarehouse-main/models/facts/qx/fct_user_role.sql -o to_process_staging/qx
```

The tool also lists the dependencies in term of parent tables, up to the source files. It reuses the same functions of the `find_pipeline.py` module:

```sh
Process the table: fct_user_role
Dependencies found:
  - depends on : int_tdc_sys_user_deduped
  - depends on : int_portal_role_member_deduped
  - depends on : int_portal_role_deduped
```

For a given table, the tool creates one folder with the table name, a Makefile to help managing the Flink Statements with confluent cli, a `sql-scripts` folder for Flink ddl and dml statements. A `tests` folder to add `data.json` (using another tool) to do some basic testing.

Example of created folders:

```sh
facts
└──qx
    └── fct_user_role
        ├── Makefile
        ├── sql-scripts
        │   ├── ddl.fct_user_role.sql
        │   └── dml.fct_user_role.sql
        └── tests
```

As part of the process, developers need to validate the generated DDL and update the PRIMARY key to refect the expected key. This information is hidden in lot of files in the dbt, and it is not yet fully automated for the migration.

Normally the DML is not executable until all dependent tables are created.

With the Makefile the following commands should work for any table: you need to be logged to confluent cloud using `confluent login` command.

```sh
make create_flink_ddl
```

Sometime we may need to delete the created topics and schemas, for that the makefile target is:

```sh
make delete_data
```

Each Flink Statements are named, so it may be relevant to delete a created statement with:

```sh
make delete_flink_statements
```

The `process_src_tables.py` creates or updates two tracking files under `process_out/` folder:

1. `tables_done.txt`  which lists the files processed so far by the different runs. It also manually updated to track the testing.
1. `tables_to_process.txt`: as new dependencies tables are found, they are added to this file so we can track the things to be done

`table_done.txt` can be manually updated to add project management information like if the DDL (True or False), DML (True or False) were successfuly completed and running on CCloud, the source target file (URI_SRC) and the new Flink SQL matching file (URI_TGT).

```sh
table_name, Type of dbt, DDL, DML, URI_SRC, URI_TGT
fct_user_role,fact,T,T,facts/qx/fct_user_role.sql,pipelines/facts/qx/fct_user_role
```

## Process from a table name

To continue the example above, suppose the DDL was successfully created, to be able to get the dml, we need to process the parent tables as defined by the dependencies graph. `fct_user_role` depends on : `int_tdc_sys_user_deduped, int_portal_role_member_deduped, int_portal_role_deduped`

We can launch the same tool to process the tables one by one given their name, the tool will search in the source dbt file for the table reference:

```sh
python process_src_tables.py  -t int_tdc_sys_user_deduped  -o ../to_process_staging/qx
```

Process the table: int_tdc_sys_user_deduped
Dependencies found:
  - depends on : src_tdc_sys_user

```
intermediates
└── qx
    └── int_tdc_sys_user_deduped
        ├── Makefile
        ├── sql-scripts
        │   ├── ddl.int_tdc_sys_user_deduped.sql
        │   └── dml.int_tdc_sys_user_deduped.sql
        └── tests
```

! This is when it becomes a little bit tricky, as analysis should lead to refectoring.  Intermediate tables, name starting with `int_`, include some transformation and filtering logic or but also some deduplication.  It was decided to do the deduplication as early as possible. close to the raw data. So when migrating source table, the tool prepare a dml to do the dedup, which means partitioning on the `__db` field and the primary key of the table. It is also leveraging the Flink table changelog format as `upsert` to keet the last record for a given key.

The source table is coming from a topic and the `src_tdc_sys_user.sql` as the following source structure:

```sql
with 
tdc_sys_user as (select * from {{ source('mc_qx','tdc_sys_user') }})

,final as (

    select 
        * 
    from tdc_sys_user
    {{limit_tenants()}}
    {{limit_ts_ms()}}

)

select * from final
```

The tool has identified the source of `int_tdc_sys_user_deduped` is `src_tdc_sys_user` and this sql file uses `tdc_sys_user` as the source table. This source table structure is defined in Databrick, so using Databricks console, we can update the generated DDL to complete the column definitions. 

The conditions in previous example, use the `limit_tenants` and `limit_ts_ms` which may not be relevant when running in Flink as this logic is used to filter out records in the batch processing as the order of records is not guarantee. In Kafka topic the records are in append mode so ordered over time and each records with the same key will be in the same topic/partition. 

Doing the following command creates the source ddl and dml statements to do the dedups for this source table. The logic of the tool is to look at the table name pattern so `src_` is a source table. We could find a better heuristic in the future.

```sh
python process_src_tables.py -t src_tdc_sys_user -o to_process_staging/qx
```

To complete the dedup logic it is import to understand the dbt dedup macro and the intermediate sql to see if there is something important to add and definitively to get the goof column names used for partitioning.

## Create test data from schema registry 

Once the source DDL is completed in Flink, the Topic is created in Kafka and the value and key schemas are published to the schema registry. In future development those topics and schemas are created in the MC platform.

```sh
python generate_data_for_table.py -o data.json -t tdc_sys_user_raw -n 5 
```

Send the test data to the target topic

```sh
# -t for the topic name and -s source for prtal 
python kafka_avro_producer.py -t portal_role_raw -s ../pipelines/sources/portal_role
```