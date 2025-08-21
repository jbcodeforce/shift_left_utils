# CHANGELOG

<!-- version list -->
v0.1.33
* Add --pool-creation boolean to pipeline deploy to set to true when the compute pool can be created during build execution plan, so one statement is allocated per compute pool. The flag is set to False for pipeline build-execution-plan.
* Add region to the statement name when deployed with Makefile
* Undeploy pipeline is improved: delete ddl as part of undeploy, verify DML is not just RUNNING, but also Failed, completed, pending. Unknown status will not try to delete statement.
* Add delete compute pool per product name. The command is `shift_left project --delete_al-compute-pools  product_1`. The product _1 string could be a string in the compute pool.
* Add `--max-thread` to the deploy command when coupled with --parallel. When set to more than 1, the parallel is set to true automatically.
* Add complexity to pipeline definition, by parsing SQL to count joins, left joins, outer joins, right joins.
* Improve selecting autonomous nodes for parallel processing
* Update doc on blue/green deployment with different scenarios.


v0.1.32
* Fix in pipeline_definition creation to improve SQL parser to get CREATE TABLE when searching for table name.
* Remove recursivity in one search for parent in pipeline_mgr, add more unit tests.

v0.1.31
* Add --no-ack to the pipeline undeploy command
* Fix issue in SQL parser to avoid getting some inner join and by passing FROM in function like SUBSTRING or TRIM
* Support region in statement name.
* revisit some unit tests for core to be fully isolated

v0.1.30
* Separate the SQL content processing in an extension python file, with class to extend TableWorker. Documentation for recipe is modified to explain how to develop the own class.
* Improve unit test harness framework to generate or not csv data, ensure test data match column type. Improve user feedbacks. Change command names to be: `init-unit-tests`, `run-unit-tests` and `delete-unit-tests`. 
* Add in config.yaml the `post_fix_unit_test: _ut` to adapt post_fix on table names to avoid conflict with other Data Engineers working on different unit tests.
* Enhance ksqlDB to Flink SQL migration.

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

