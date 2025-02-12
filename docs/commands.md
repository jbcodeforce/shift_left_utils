# Command Summary


This section presents a quick summary of the tools used for migration. Be sure to have set environment variables: SRC_FOLDER, STAGING and CONFIG_FILE:

```sh
export SRC_FOLDER=../../your-src-dbt-folder/models
export STAGING=../../flink-project/staging
export CONFIG_FILE=../../flink-project/config.yaml
```

## Get parent pipeline

To get the parent hierarchy for a fact or dimension table, use the `find_pipeline.py` tool. The output will report the dependent tables up to the sources

```sh
python find_pipeline.py -f $SRC_FOLDER/facts/fct_training_doc.sql
```


[See this section for details](./migration.md#1---discover-the-current-pipeline)

## Get the table using another table

```sh
python find_table_user.py -t table_name -r root_folder_to_search_in
# example
python find_table_user.py -t users -r $STAGING
```

## Process a fact or dimenstion table


* Generate Flink SQLs from one Fact or Dimension table using a recurcive processing up to the source tables. The SQL created statements are saved into the staging temporary folder

```sh
python process_src_tables.py -f $SRC_FOLDER/facts/fct_users.sql -o $STAGING/app -pd
```

## Process all the source tables

* For all tables in the `sources` folder, create the matching Flink SQL DDL and  DMLs into a temporary folder. Once generated the deduplication logic can be finalized manually to the final pipelines folder. This approach may not be needed if you use the process parent hierarchy option.

```sh
python process_src_tables.py -f $SRC_FOLDER/sources -o $STAGING/sources
```

## Create a sink table structure

The goal of this tool is to create a folder structure to start migrating SQL manually:

```sh
python  create_sink_structure.py -t fct_user -o $STAGING/fct_user
```


## Update the source sql to do some update

The typical update will be the source table name to be used as it may change per environment. So this tool can help do a global change. Another use case is to limit the data to use for developement to a spesific set of record, so a where statement can be done.

```sh
python change_src_sql.py -s <source_folder> -w "where condition as string"
```

## Others - Need to be updated
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
