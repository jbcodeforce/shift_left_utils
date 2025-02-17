# Shift Left tools and practices

???- info "Versions"
    Created 12/2024
    
## Introduction

Shift Left means taking bach-processing jobs and try to refactor them to real-time processing using a product such as Apache Flink. In batch processing a lot of projects use SQL and dbt (Data build tool) to define the logic of the data pipeline. In real-time processing, Apache Kafka is a de-facto middleware to persist immutable records, and for SQL, Python and Java based real-time processing, Apache Flink is also the preferred platform.

To organize batch processing and data pipelines to Datawarehouse, some compagnies are using the [Kimball guidelines](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/) and best practices.

Working on this kind of refactoring projects is taking some time and is challenging. AI Agentic solution should help data engineers to shift their data pipelines to the real-time processing by offering flexible SQL translation tools.

This repository is a tentative to develop and share some of those tools and practices for running such projects.

As of now the utilities are oriented to use Confluent Cloud for Kafka and for Flink, but running local Flink and Kafka should be easy to support.

To avoid calling remote LLM, the current repository uses Ollama, running locally, or potentially in a remote server on-premises or inside a private network.

Two important concepts of this practice:

* **Dimensions** provide the “who, what, where, when, why, and how” context surrounding a business process event. Dimension tables contain the descriptive attributes used by BI applications for ﬁltering and grouping the facts. 
* **Facts** are the measurements that result from a business process event and are almost always numeric. The design of a fact table is entirely based on a physical activity, and not by the reports to produce from those facts. A fact table always contains foreign keys for each of its associated dimensions, as well as optional degenerate dimension keys and date/time stamps

## Context

The target environment will be Apache Flink running within the Confluent Cloud as a managed service. The source of the batch processing is defined within a dbt (Data build tool) project and the refactored SQL are produced under the `pipelines` folder.

The following diagram illustrates the development environment whicj, mainly, uses 2 containers:

![](./images/environment.drawio.png)

The shift left utils docker image groups a set of Python tools, Python 3.13.1 and the needed libraries to integrate with Ollama, like using LLM client API with Langgraph. The [ollama image](https://hub.docker.com/r/ollama/ollama) is used to run **qwen2.5-coder:32b** LLM model locally on the developer's computer.

Follow the [setup instructions to get started with the migration project](./setup.md).

If the developer laptop does not have enough capacity, there is an option to run Ollama on an EC2 server.

## A migration path

Any batch pipelines that create tables or files in a Lakehouse platform can be refactored using a Flink pipeline, as illustrated in the following figure:

![](./images/generic_src_to_sink_flow.drawio.png)

The diagram above illustrates the target architecture for each pipeline after migration. This architecture employs a sink configured as a PostgreSQL database, which will support business intelligence dashboards. The Flink tables are mapped to Kafka topics, and Kafka connectors are utilized to transfer data from these topics to the PostgreSQL database.

From the perspective of a Confluent Cloud Flink pipeline, the last topic serves as the sink.

To facilitate the refactoring, the approach begins with the sink table and works backward to identify the sources. Once the sources are determined, the process may involve implementing a set of deduplication statements and intermediate steps to apply business logic or data transformations.

The dbt project contains all the SQL statements necessary for migration, located in the models folder. The goal of the tools is to process these files and replicate the same organizational structure for Flink SQL statements, which includes sources, intermediates, staging, dimensions, and facts. Additionally, the tools aim to automate parts of the migration process.


The target structure will look like in the following example:

```sh
pipelines
├── dimensions
│   └── {application_name}
│       └── {dimension_name}
│           ├── Makefile
│           ├── sql-scripts
│           │   ├── ddl.{dim_name}.sql
│           │   └── dml.{dim_trainee}.sql
│           └── tests
├── facts
│   └── {application_name}
│       └─── {fact_name}
│           ├── Makefile
│           ├── sql-scripts
│           │   ├── ddl.{fact_name}.sql
│           │   └── dml.{fact_name}.sql
│           └── tests
├── intermediates
│   └── {application_name}
│       └─── {fact_name}
│          ├── Makefile
│          ├── sql-scripts
│          │   ├── ddl.{intermediate_name}.sql
│          │   └── dml.{intermediate_name}.sql
│          └── tests
├── sources
│   └── {application_name}
│       ├── {src_name}
│           ├── Makefile
│           ├── dedups
│           │   └── dml.{src_name}.sql
│           └── tests
│               └── ddl.{src_name}.sql

```

## Source Topic management

In some ETL or ELT pipelines, Kafka topics may be the source of the pipeline. A classical example is when a change data capture mechanism is deployed and get records from existing SQL database to Kafka Topics. One topic per table. The architecture looks like in the following diagram:

![](./images/elt_pipe.drawio.png)

When migrating to real-time processing, using Confluent Cloud and Flink compute pool, the source topics may be reused and the Flink Statement will do mostly the same processing as the ELT. Confluent Cloud for Flink creates tables from topic and get the schema from the schema registry.

The name of the table matches the name of the topic and the table schema maps to the topic-value schema. 

Adopt the naming convention from the change data capture, like Debezium for the topic. Any naming convention based on the environment like dev, staging, or production, will impact any DML statements. In Confluent Cloud the environment groups one to many Kafka clusters and multiple Flink compute pools. The topic name can be the same between Kafka cluster so the same DML statement can be used in different conpute pool.