# Shift Left Utils

A comprehensive toolkit for migrating SQL batch processing to real-time Apache Flink on Confluent Cloud, with AI-powered code translation and advanced pipeline management. It also addresses KsqlDB to Apache Flink SQL migration.

## Installation

```bash
pip install shift_left
```

## Features

**SQL Migration & Translation**: Automatically migrate KSQL, Spark SQL, and DBT code to Flink SQL using LLM-based agents with validation and refinement capabilities.

**Pipeline Management**: Build, validate, and deploy Flink SQL pipelines with dependency management, execution planning, and blue-green deployment strategies.

**Project Structure**: Scaffold and manage Flink projects following medallion architecture (sources, intermediates, dimensions, facts, views) with comprehensive metadata and testing frameworks.

**Test Harness**: Develop unit test SQL templates with synthetic data to unit test Flink SQL statements.

## Quick Start

```bash
# Initialize a new Flink project
shift_left project init my_project ./my_project --project-type kimball

# Build table inventory
shift_left table build-inventory ./pipelines

# Deploy a pipeline
shift_left pipeline deploy ./pipelines --table-name my_table
```

## CLI Commands

### Project Management
- `shift_left project init` - Initialize a new Flink project
- `shift_left project validate-config` - Validate configuration file
- `shift_left project list-topics` - List Kafka topics
- `shift_left project list-compute-pools` - List Flink compute pools

### Table Management
- `shift_left table init` - Create table structure
- `shift_left table build-inventory` - Build table inventory
- `shift_left table migrate` - Migrate SQL with AI assistance

### Pipeline Deployment
- `shift_left pipeline deploy` - Deploy Flink pipelines
- `shift_left pipeline build-metadata` - Build pipeline metadata

### Testing
- `shift_left table init-unit-tests` - Create test file for unit tests
- `shift_left table run-unit-tests` - Run the test suite
- `shift_left table delete-unit-tests` - Remove unit test artifacts

## Documentation

- [Complete Documentation](https://jbcodeforce.github.io/shift_left_utils/)
- [Quick Start & Commands](https://jbcodeforce.github.io/shift_left_utils/command/)
- [Blue-Green Deployment](https://jbcodeforce.github.io/shift_left_utils/blue_green_deploy/)
- [AI-based Migration](https://jbcodeforce.github.io/shift_left_utils/coding/llm_based_translation/)

## Requirements

- Python 3.12+
- Confluent Cloud account (for deployment features)

## License

Apache License 2.0

## Links

- [GitHub Repository](https://github.com/jbcodeforce/shift_left_utils)
- [Issue Tracker](https://github.com/jbcodeforce/shift_left_utils/issues)






