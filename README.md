# [Shift left Tools and AI utilities](https://jbcodeforce.github.io/shift_left_utils/)

This repository includes a set of tools to help refactoring SQL batch processing to real-time prodessing using Apache Flink and to manage a Flink project on Confluent Cloud. 

Here are the list of important features supported:

* Support taking SQL / dbt sql scripts and migrate them with LLM to speed up a migration project to Apache Flink SQL. It supports understanding the static relationship between source SQL tables
* Support defining code structure to manage Flink statements for DDL, DML, Makefile (wrapper on top of Confluent Cli), testing and metadata creation about the relationship from one Flink statement with other Flink statements.
* Build table inventory for a project with metadata to help automate CI/CD work or supporting the shift_left tool itself
* Create metadata about a pipeline for a given Flink Statement: those metadata includes, name, type of Flink statement (stateless, stateful), what are the direct ancestors of the flink statement, what are the children, users of the current Flink statement.
* Build an execution plan for each pipeline to understand what needs to be started and redeployed to avoid brute force deployment. Execution plan is a topological sorted graph which helps to start Flink statements that are needed before other statements
* Deploy execution plan, with constraints on forcing restart of ancestors (or not), update children or not.
* Support grouping Flink statements per directory (reflecting the medallion structure of the project: sources, intermediates, dimensions, facts and views), or as a product (data as a product) as an orthogonal view of the medallion view), or as a list of table / Flink statements or a unique table/Flink statement.
* Create Confluent Cloud Flink compute pool via REST API when needed during the execution plan deployment. This help to support less SRE involvement and dynamic definition of pool to flink assignment. 
* Select compute pool from existing running Flink statement to reuse resources.
* Support reports of running statements using the execution plan semantic
* Verify naming convention is respected and other best practices as topic configuration, schema parameters.
* Support aggregating report on statements, like running Flink explain  and reporting deployment errors for all statements within a Folder or for a product.
* Support adding custom table worker to do SQL content update during deployment: this is needed to support multi-tenancy into the same Kafka Cluster and Schema Registry, or apply some table changes names. Some default transformations are available:
* Unit tests creation from DML with test definition metadata to be able to run unit tests with mock data on Confluent Cloud, by using REST API to deploy statement.

[Read the documentation in book format](https://jbcodeforce.github.io/shift_left_utils/).

