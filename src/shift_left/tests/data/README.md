# Test data for Unit and Integration tests

The flink-rpojecct/pipelines folder includes the definition of Flink statement relationships to validate the shift_left tool capabilities, both for unit tests and integration tests.


## Flink project

flink-project folder is where all the Flink examples are defined. It was created with `shift_left project init` and tables added over time with `shift_left table init`

There are only 4 groups for each data products: Sources, intermediates, facts and dimensions.

The p1 data product contains all the flink statements to demonstrate the relationship as presented in this  figure:

![](./docs/flink_pipeline_for_test.drawio.png)

The users data product is simpler and here for unit tests the test manager.

### User data product

THe quick test deploy a user dimension that joins user per group and count the number of user per group as a fact.

* Set a config file like:
    ```yaml
    kafka:
  bootstrap.servers: pkc-......confluent.cloud:9092
  cluster_id: lkc-...
  reject_topics_prefixes: ["clone","dim_","src_"]
  src_topic_prefix: cdc
  cluster_type: dev
    confluent_cloud:
    environment_id: env-nknqp3
    region: us-west-2
    provider: aws
    organization_id: 49ce....44
  flink:
    compute_pool_id: lfcp-xvrvmz
    compule_pool_ids: [lfcp-xvrvmz,  lfcp-d3n9zz]
    catalog_name: j9r-env
    database_name: j9r-kafka
    max_cfu: 17
  app:
    logging: INFO
    accepted_common_products: ['common', 'seeds']
    post_fix_unit_test: _ut
    sql_content_modifier: shift_left.core.utils.table_worker.ReplaceEnvInSqlContent
    translator_to_flink_sql_agent: shift_left.core.utils.translator_to_flink_sql.DbtTranslatorToFlinkSqlAgent
    dml_naming_convention_modifier: shift_left.core.utils.naming_convention.DmlNameModifier
    compute_pool_naming_convention_modifier: shift_left.core.utils.naming_convention.ComputePoolNameModifier
    ```
    
* Build table inventory
* Build table medatada
* Deploy in one command: shift_left pipeline deploy --product-name users

## Spark project

This is a simple project to includes spark SQL statements to be used for batch processing. The use case is around a case management application from which the batch jobs are gathering data for users, workflows, tasks, enterprises and then compute some analytic aggregates and metrics.

The p7 folder includes the 'data_product' p7 with a set of table to create information about users.