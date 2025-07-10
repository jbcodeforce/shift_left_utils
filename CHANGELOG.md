# CHANGELOG

<!-- version list -->
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
