# Test data for Unit and Integration tests

The pipelines folder includes the definition of Flink statements relationships to validate the shift_left tool capabilities, both for unit tests and integration tests.

## Spark project

This is a simple project to includes spark SQL statements to be used for batch processing. The use case is around a case management application from which the batch jobs are gathering data for users, workflows, tasks, enterprises and then compute some analytic aggregates and metrics.

The p7 folder includes the 'data_product' p7 with a set of table to create information about users.

## Flink project

flink-project folder is where all the Flink examples are defined. It was created with `shift_left project init`.

There are only 4 grouping for each data products: Sources, intermediates, facts and dimensions.

The p1 data product contains all the flink statements to demonstrate the relationship as presented in this  figure:

![](../../../../../../docs/images/flink_pipeline_for_test.drawio.png)

The p3 product represents 