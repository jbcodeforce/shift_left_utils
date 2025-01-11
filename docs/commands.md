# Command Summary


This section presents a quick summary of the tools used for migration. Be sure to have set environment variables: SRC_FOLDER, STAGING and CONFIG_FILE:

```sh
export SRC_FOLDER=../../your-src-dbt-folder/models
export STAGING=../../flink-project/staging
export CONFIG_FILE=../../flink-project/config.yaml
```

* Get the parent hierarchy for a fact or dimension table. The output will report the dependent tables up to the sources

```sh
python find_pipeline.py -f $SRC_FOLDER/facts/fct_training_doc.sql
```


[See this section for details](./migration.md#1---discover-the-current-pipeline)

* For all tables inthe `sources` folder, create the matching Flink SQL DDL and Dedup DMLs into a temporary folder. Once generated the deduplication logic can be finalized manually to the final pipelines folder. This approach may not be needed if you use the process parent hierarchy option.

```sh
python process_src_tables.py -f $SRC_FOLDER/sources -o $STAGING/sources
```

* Generate Flink SQLs from one Fact or Dimension table using a recurcive processing up to the source tables. The SQL created statements are saved into the staging temporary folder

```sh
python process_src_tables.py -f $SRC_FOLDER/facts/qx/fct_training_doc.sql -o $STAGING/qx -pd
```


* Once the Source DDL is executed successfully, generate test data, with 5 records, and send them to the matching topic. 

```sh
# -o is for the output file to get the json array of json objects to be send to the topic, -t is for table name, and -n is for the number of records to create
python generate_data_for_table.py -o data.json -t sys_user_raw -n 5 
```

* Send the test data to the target topic

```sh
# -t for the topic name and -s source for prtal 
python kafka_avro_producer.py -t portal_role_raw -s ../pipelines/sources/portal_role
```
