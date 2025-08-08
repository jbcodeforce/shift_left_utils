# [Shift Left Utils](https://jbcodeforce.github.io/shift_left_utils/)

A comprehensive toolkit for migrating SQL batch processing to real-time Apache Flink on Confluent Cloud, with AI-powered code translation and advanced pipeline management. It also address KsqlDB to Apache Flink SQL migration.

## What it does

**SQL Migration & Translation**: Automatically migrate KSQL, Spark SQL, and DBT code to Flink SQL using LLM-based agents with validation and refinement capabilities.

**Pipeline Management**: Build, validate, and deploy Flink SQL pipelines with dependency management, execution planning, and blue-green deployment strategies.

**Project Structure**: Scaffold and manage Flink projects following medallion architecture (sources → intermediates → dimensions → facts → views) with comprehensive metadata and testing frameworks.

**Test Harness**: Develop unit test SQL template with synthetic data to unit test a Flink SQL statement. 

## Development Branches

- **`main`**: Stable production-ready releases
- **`develop`**: Active development branch with latest features and improvements

## Documentation

📖 **[Complete Documentation](https://jbcodeforce.github.io/shift_left_utils/)** - Comprehensive guides, tutorials, and API reference

📋 **[Quick Start & Commands](docs/command.md)** - CLI reference and usage examples

🚀 **[Blue-Green Deployment](docs/blue_green_deploy.md)** - Git-based change tracking and deployment strategies

