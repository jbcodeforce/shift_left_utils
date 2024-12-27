# Shift Left tools and practices

???- info "Versions"
    Created 12/2024
    
## Introduction

Shift Left means taking bach-processing jobs and try to refactor them to real-time processing using a product such as Apache Flink. In batch processing a lot of projects use SQL and dbt (Data build tool) to define the logic of the data pipeline. In real-time processing, Apache Kafka is a de-facto middleware to persist immutable records, and for SQL, Python and Java based real-time processing, Apache Flink is also the preferred platform.

To organize batch processing and data pipelines to Datawarehouse, some compagnies are using the [Kimball guidelines](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/) and best practices.

Two important concepts of this practice:

* **Dimensions** provide the “who, what, where, when, why, and how” context surrounding a business process event. Dimension tables contain the descriptive attributes used by BI applications for ﬁltering and grouping the facts. 
* **Facts** are the measurements that result from a business process event and are almost always numeric. The design of a fact table is entirely based on a physical activity, and not by the reports to produce from those facts. A fact table always contains foreign keys for each of its associated dimensions, as well as optional degenerate dimension keys and date/time stamps

## Context

The target environment will be Apache Flink running within the Confluent Cloud as a managed service. The source of the batch processing is defined within a dbt (Data build tool) project and the refactored SQL are produced under the `pipelines` folder.

The following diagram illustrates the development environment:

![](./images/environment.drawio.png)

The shift left utils docker images groups a set of Python tools, Python 3.13.1 and the needed libraries, like using LLM client API with Langgraph. The [ollama image](https://hub.docker.com/r/ollama/ollama) is used to run **qwen2.5-coder:32b** LLM model locally on the developer's computer.

Follow the [setup instructions to get started with the migration project](./setup.md).

If the developer laptop does not have enough capacity, there is an option to run Ollama on an EC2 server.

## A migration path

Any batch pipelines creating tables or files into a Lakehouse platform, may be refactored with Flink pipeline using an approach illustrated by the following figure:

![](./images/generic_src_to_sink_flow.drawio.png)

This is the target architecture for each pipeline after migration.