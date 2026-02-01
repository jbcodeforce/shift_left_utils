# shift_left

CLI to manage Flink projects and migrate batch SQL pipelines to real-time Apache Flink on Confluent Cloud. Supports KSQL, Spark SQL, and dbt as sources.

**Documentation:** [https://jbcodeforce.github.io/shift_left_utils/](https://jbcodeforce.github.io/shift_left_utils/)

## Install

```bash
pip install shift_left
```

## Major features


- **Project management**: Initialize Flink projects (Kimball or data-product layout), validate config, list Kafka topics and Flink compute pools, and report cross-product or single-child tables.
- **Pipeline deployment**: Build table inventory and pipeline metadata, deploy DDL/DML with execution planning. Optional parallel deploy, dml-only, and blue-green via modified-files tracking.
- **Table lifecycle**: Create table scaffolding, run AI migration per table (with optional recursion), and manage unit tests (init, run, delete) against Confluent Cloud.
- **SQL migration**: Migrate KSQL, Spark SQL, and dbt to Flink SQL using LLM-based agents (OpenAI-compatible, including local Ollama). Handles table detection, DDL creation, and translation with validation.
- **Unit test harness**: Develop synthetic test data from SQL DML semantic, with test suite and test case metadata to help Data Engineers to unit test their SQL on Confluent Cloud.
- **Blue-green and operations**: List modified files by branch for safe deployments, integrate with your CI/CD and Confluent Cloud APIs.

## Quick start

```bash
# Initialize a Flink project
shift_left project init my-flink-project /path/to/project --project-type data_product

# Validate Confluent Cloud config
shift_left project validate-config

# Migrate a table from Spark SQL to Flink SQL (one time work)
shift_left table migrate my_table /path/to/script.sql /path/to/staging --source-type spark --recursive

# Build pipeline execution plan
shift_left table build-inventory
shift_left pipeline build-all-metadata
shift_left pipeline build-execution-plan  /path/to/inventor --table-name my_table --may-start-de

# Deploy pipelines from inventory
shift_left pipeline deploy /path/to/inventory --table-name my_table
```

## Documentation

Full guides, tutorials, CLI reference, and MCP server setup:

**[https://jbcodeforce.github.io/shift_left_utils/](https://jbcodeforce.github.io/shift_left_utils/)**

- User guide: environment setup, recipes, pipeline management, blue-green deployment
- Tutorials: setup, project management, migration with AI
- CLI reference and MCP (Model Context Protocol) integration for Cursor and other tools

## Requirements

- Python 3.12
- Confluent Cloud (Kafka and Flink). For AI migration: OpenAI-compatible API or local Ollama.

## License

Apache-2.0
