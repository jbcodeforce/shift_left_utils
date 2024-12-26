# Shift Left tools and practices

## Introduction

Shift Left means taking bach-processing jobs and try to refactor them to real-time processing using a product such as Apache Flink. In batch processing a lot of projects use SQL and dbt (Data build tool) to define the logic of the data pipeline. In real-time processing, Apache Kafka is a de-facto middleware to persist immutable records, and for SQL, Python and Java based real-time processing, Apache Flink is also the preferred platform.

To organize batch processing and data pipelines to Datawarehouse, some compagnies are using the [Kimball guidelines](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/) and best practices.

Two important concepts of this practice:

* **Dimensions** provide the “who, what, where, when, why, and how” context surrounding a business process event. Dimension tables contain the descriptive attributes used by BI applications for ﬁltering and grouping the facts. 
* **Facts** are the measurements that result from a business process event and are almost always numeric. The design of a fact table is entirely based on a physical activity, and not by the reports to produce from those facts. A fact table always contains foreign keys for each of its associated dimensions, as well as optional degenerate dimension keys and date/time stamps

## Context

The target environment will be Apache Flink running within the Confluent Cloud as a managed service. The source of the batch processing is defined within a dbt (Data build tool) project.

## Command Summary


This section is quick summary of the full content of this readme.

* Get the parent hierarchy for a fact or dimension table:

```sh
python find_pipeline.py -f ../dbt-src/models/facts/qx/fct_training_jobcode_unit.sql
```

* Generate from dbt source tables the matching Flink SQL DDL and Dedup DMLs into a temporary folder to finalize manually to the final pipelines folder:

```sh
python process_src_tables.py -f ../dbt-src/models/sources -o ../staging/sourcesl
```

* Generate Flink SQL from one dbt file to the staging temporary folder

```sh
python process_src_tables.py -f ../dbt-src/models/facts/qx/fct_training_jobcode_unit.sql -o ../staging/qx
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

## A migration path
