# CHANGELOG

* Support version flag in deploy command with table-list to deploy to change the table name in the SQL content during deployment. This is to support chirurgical blue/green deployment.

### v0.1.45
* Add list of statement per compute pool id, as part of project commands and in the deployment manager service
* Add support to exclude-table-list from a deployment or build execution plan. The table will not be restarted or re run
* Enhance the project list-modified-files



<!-- version list -->
### v0.1.45
* Add list of statement per compute pool id, as part of project commands and in the deployment manager service
* Add support to exclude-table-list from a deployment or build execution plan. The table will not be restarted or re run
* Enhance the project list-modified-files
* Support version flag in deploy command with table-list to deploy to change the table name in the SQL content during deployment. This is to support chirurgical blue/green deployment.

### v0.1.44
* Support post_fix to run and validate unit tests so they can be run in parallel with CI/CD workflow. New arguments to the unit test function
* Add new command for creating a healthcheck report of a given product pipeline.

### v0.1.43
* fix processing statement result for unit test, by changing the scan.bounded.mode to latest-offset, so validation scripts will complete, and the results will be bounded
* Add a run-all flag for running validation script:  `table validate-unit-tests <table_name> --run-all ` so all the validation SQLs will be executed.

### v0.1.42 - 2025-10-15
- **MCP (Model Context Protocol) Integration for Cursor AI. (alpha version)**
  - Added `shift_left.mcp` module for seamless Cursor AI integration
  - All CLI commands now available as MCP tools
  - Natural language interface for shift_left commands in Cursor
  - Comprehensive documentation in `docs/mcp/`
  - Test suite for MCP server validation
  - Uses `uv` for dependency management

- Add `project list-tables-with-one-child` command to report tables that have zero to one only child table in the pipeline hierarchy
- Add `project isolate-data-product` command to take all tables of the given product and copy to another folder, taking into account the complete execution graph. This function is for integrated backup.
- Add `project list-modified-files` to get the list of modified tables using git logs to build of list of tables to deploy in a context of a PR or between two dates,
- Improve `project init-integration-tests` command. 

### v0.1.41 - 2025-10-02
- bab3428 fix issue in deployment mgr for failing statement. when no test case name is specified run all and default to empty string. Fix spark ai migration ddl and dml as list of strings
- 017f36f update to spark ai

### v0.1.40
- Enhance sql diff with a hash based diff
- Work on spark agent, and unit tests
- Fix issue related to DDL could return RUNNING now. 
- Improve table reference extraction for table with unnest string in the table name, improve unit tests execution. 
- Improve SLQ parser for is disctinct from construct
- Add a command at the project level to isolate a data product: copying all Flink statements to a separate folder by keeping the interdependencies with cross product parents
- Migrate unit test with AI to use local qwen3:30b
- Update to list of modified file report, and get a better sql_content diff algorithm

### v0.1.39
- integration tests creation
- enhancement to the sql parser to better identify CTEs
- New project command to get the list of tables used by cross products
- New project command to get the list of modified table since a given data, leveraging git logs, and then compare to the running statements to build of list of tables to deploy, in the context of a 'surgical' blue/green deployment.

### v0.1.38
- Enhance validation.sql for unit tests
- Fix an issue for dev src sql content modification
- remove dependency on tk

### v0.1.37
* Shift Left utility now supports environment variables for sensitive configuration values. Environment variables take precedence over config.yaml values, allowing you to:
    - Keep sensitive data out of configuration files
    - Use different credentials for different environments
    - Securely manage secrets in CI/CD pipelines
    - Follow security best practices
* The config.yaml is not reduced to the minimum parameters and default values are in the code.
* Test harness now supports `app.post_fix_unit_test` to update table name while deploying. the _ut is during the creation of the unit tests, and should be committed to git as this. The run-unit-tests or run-validation-tests will take care of the `app.post_fix_unit_test`
* A new command at the project level to do some statement housekeeping: `shilft_left project housekeep-statements` will clean completed and failed statements.

### v0.1.36
* Improve node_map builder for better performance: keep node visited and currently built node_map to avoid reprocessing nodes already visited.
* Add compute pool analyzer.

### v0.1.35
* Improve prepare error management when statement is not completed- add failed status and use other api to get statement status
* Add masking data from config in trace - disable `typer` local variables reporting in case of exceptions
* Add unit tests for masking, error reporting

### v0.1.34
* Add an intermediate step in test harness to run the validation script by its own, using: `shift_left table run-validation-tests <table-name> <test_name>`
* Add nb-test-cases to the `shift_left table init-unit-tests` command to control the number of test case to create instead of the default of 2.
* Fix undeploy at the table level to only deploy descendants and the selected table.
* Improve table name retrieval in the test manager to better hansle backstick
* Implement AI agents for unit test data tuning: consistency between insert .sqls, complaince to the column type and generation of the validation script. (This last one need far more work)


### v0.1.33
* Add --pool-creation boolean to pipeline deploy to set to true when the compute pool can be created during build execution plan, so one statement is allocated per compute pool. The flag is set to False for pipeline build-execution-plan.
* Add region to the statement name when deployed with Makefile
* Undeploy pipeline is improved: delete ddl as part of undeploy, verify DML is not just RUNNING, but also Failed, completed, pending. Unknown status will not try to delete statement.
* Add delete compute pool per product name. The command is `shift_left project --delete_al-compute-pools  product_1`. The product _1 string could be a string in the compute pool.
* Add `--max-thread` to the deploy command when coupled with --parallel. When set to more than 1, the parallel is set to true automatically.
* Add complexity to pipeline definition, by parsing SQL to count joins, left joins, outer joins, right joins.
* Improve selecting autonomous nodes for parallel processing
* Update doc on blue/green deployment with different scenarios.


### v0.1.32
* Fix in pipeline_definition creation to improve SQL parser to get CREATE TABLE when searching for table name.
* Remove recursivity in one search for parent in pipeline_mgr, add more unit tests.

### v0.1.31
* Add --no-ack to the pipeline undeploy command
* Fix issue in SQL parser to avoid getting some inner join and by passing FROM in function like SUBSTRING or TRIM
* Support region in statement name.
* revisit some unit tests for core to be fully isolated

### v0.1.30
* Separate the SQL content processing in an extension python file, with class to extend TableWorker. Documentation for recipe is modified to explain how to develop the own class.
* Improve unit test harness framework to generate or not csv data, ensure test data match column type. Improve user feedbacks. Change command names to be: `init-unit-tests`, `run-unit-tests` and `delete-unit-tests`. 
* Add in config.yaml the `post_fix_unit_test: _ut` to adapt post_fix on table names to avoid conflict with other Data Engineers working on different unit tests.
* Enhance ksqlDB to Flink SQL migration.

### v0.1.29
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


### v0.1.28
* Support getting the name of the table from DDL if not found in DML
* Improve may-start-descendants processing and cross-product deployment constraint
* Documentation update: Blue/green deployment
* Add version to cli: shift_left --version
* Clean reports consistency
* Add more unit tests to validate execution-plan for Flink statements

### v0.1.27
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

