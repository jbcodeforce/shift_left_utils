# Command Summary


This section is quick summary of the full content of this readme. Be sure to have set environment variables: SRC_FOLDER and STAGING

```sh
export SRC_FOLDER=../../your-src-dbt-folder/models
export STAGING=../../flink-project/staging
```

* Get the parent hierarchy for a fact or dimension table:

```sh
python find_pipeline.py -f $SRC_FOLDER/facts/qx/fct_training_doc.sql
```

    [See this section for details](./migration.md#1---discover-the-current-pipeline)

* Generate from dbt source tables the matching Flink SQL DDL and Dedup DMLs into a temporary folder to finalize manually to the final pipelines folder:

```sh
python process_src_tables.py -f $SRC_FOLDER/sources -o $STAGING/sources
```

* Generate Flink SQL from one dbt file to the staging temporary folder

```sh
python process_src_tables.py -f $SRC_FOLDER/facts/qx/fct_training_doc.sql -o $STAGING
```

* Once the Source DDL is executed successful, generate test data, 5 records, to send to the matching topic. It is 

```sh
# -o is for the output file to get the json array of json objects to be send to the topic, -t is for table name, and -n is for the number of records to create
python generate_data_for_table.py -o data.json -t sys_user_raw -n 5 
```

* Send the test data to the target topic

```sh
# -t for the topic name and -s source for prtal 
python kafka_avro_producer.py -t portal_role_raw -s ../pipelines/sources/portal_role
```
