# CHANGELOG

<!-- version list -->
v0.1.29
* Enhanced KSQL to Flink SQL migration with improved LLM-based translation (Issue #23)
* Added table detection functionality for KSQL statements to handle multiple CREATE TABLE scenarios
* Added comprehensive test coverage for KSQL migration functionality with real-world examples
* Added Spark SQL code agent and migration examples for Spark to Flink SQL translation
* Fixed naming conflict in validate_config CLI command that was causing recursive function calls
* Added comprehensive tests for configuration validation including valid/invalid scenarios, placeholder detection, and data type validation
* Enhanced configuration validation error messages and handling
* Improved test data structure and organization with better separation of concerns
* Enhanced documentation for LLM-based translation methodology
* Cleaned up legacy utility files and improved project structure
* Updated dependencies and project configuration
* FIX issue #31: _ut postfix is now in config.yaml or defaulted to "_ut"
* Improve the pipelines.definitions.json creation to address table not found cases that were strange


v0.1.28
* Support getting the name of the table from DDL if not found in DML
* Improve may-start-descendants processing and cross-product deployment constraint
* Documentation update: Blue/green deployment
* Add version to cli: shift_left --version
* Clean reports consistency
* Add more unit tests to validate execution-plan for Flink statements

v0.1.27
* dbt to Flink statement migration
* project management
* table management, build table inventory, assess if a Flink statement is stateless or stateful
* pipeline management: build all metadata for all the Flink statement. build metadata for a specific table. For each table, builds a static metadata to understand what a Flink statement is consuming and who is using the output of a flink statement.
* pipeline management: support build execution plan for table, data product, or kimball hierarchy level
* pipeline deployment by specific table, data product, or kimball hierarchy level. sequential mode only
* support flexible restart of ancestors or descendants
* address unit testing of Flink statement for confluent cloud
* support different report of running statements with the view per pipelines, product, or hierarchy level.
* report and manage a Flink project with best practices
* support dynamic update of Flink SQL content to drive the deployment into different environments or within a multiple tenants Kafka Cluster. The code can be extended by users to adapt to their deployment strategy
* documentation per user profile, recipe oriented.

