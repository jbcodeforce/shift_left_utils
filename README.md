# [Shift Left Utils](https://jbcodeforce.github.io/shift_left_utils/)

A comprehensive toolkit for migrating SQL batch processing to real-time Apache Flink on Confluent Cloud, with AI-powered code translation and advanced pipeline management. It also address KsqlDB to Apache Flink SQL migration.

## What it does

**Project** (`shift_left project`): Create and manage Flink project layout (kimball or data-product: sources, intermediates, dimensions, facts, views). Validate `config.yaml`, list Kafka topics and compute pools. Track git changes for blue-green deployment (`list-modified-files`). Report cross-product table usage, tables with one child, and isolate a data product. Init, run, or delete integration tests. Assess and delete unused tables; housekeep failed or completed statements.

**Table** (`shift_left table`): Add table folder structure (sql-scripts, Makefile) with `init`. Build table inventory from the pipeline path. Migrate KSQL, Spark SQL, or DBT to Flink SQL via LLM-based agents (`migrate --source-type ksql|spark|dbt`) with optional Confluent validation. Validate naming conventions, search source dependencies, update Makefiles or SQL in bulk. Get Flink execution plan explanations (`explain`). Unit test harness: init templates with synthetic data, run or delete unit tests on Confluent Cloud.

**Pipeline** (`shift_left pipeline`): Build pipeline metadata from DDL/DML (parent-child graph) and table inventory. Build execution plans for deployment order. Deploy by table name, product name, directory, or table list file (e.g. from `list-modified-files`) to Confluent Cloud, with optional DML-only or parallel deployment. Report pipeline hierarchy, healthcheck, and running statements. Compute field-level lineage. Undeploy from a sink; analyze compute pool usage. 


## Cursor AI Integration

This project includes **MCP (Model Context Protocol)** integration for Cursor AI! Use shift_left commands naturally in Cursor conversations. See **[Quick Start Guide](docs/mcp/index.md)** to get started in 5 minutes.


## Project status

* This project is still under tuning and development.
* It is used for different engagements extensively.
* Stress test cammpaigns for parallel deployment and result accuracy were performed on Data Pipeline reposityory with 400+ sql statements

## Development Branches

- **`main`**: Stable production-ready releases
- **`develop`**: Active development branch with latest features and improvements

## Documentation

**[Complete Documentation](https://jbcodeforce.github.io/shift_left_utils/)** - Comprehensive guides, tutorials, and API reference

**[Blue-Green Deployment](https://jbcodeforce.github.io/shift_left_utils/blue_green_deploy.md)** - Git-based change tracking and deployment strategies

**[AI based migration](https://jbcodeforce.github.io/shift_left_utils/coding/llm_based_translation)**


## Support my work

Love it? Give it a ⭐️ by clicking below:

<a href="https://github.com/jbcodeforce/shift_left_utils/stargazers"><img src="https://img.shields.io/github/stars/jbcodeforce/shift_left_utils?style=social" style="margin-left:0;box-shadow:none;border-radius:0;height:24px"></a>
