
# Blue/Green Deployment Testing

The goals of the process presented in this note are to:

1. Reduce the impact on continuously running Flink statements: do not redeploy them when not necessary
1. Reduce the cost of dual environments (Kafka clusters, connectors), and running parallel logic
1. Simplify the overall deployment of Flink statements, and authorize quicker deployment time
1. Avoid redeploying stateful processing with big state to construct when not necessary

## Context

In a classical blue-green deployment, for ETL jobs, the CI/CD process updates everything and once the batch is done, the consumer of the data products, switches to new content. The following figure illustrates this approach at a high level:

<figure markdown="span">
![](./images/bg_current.drawio.png)
<caption>**Figure 1**: Blue-Green for batch processing</caption>
</figure>
*Blue is the **production**, Green is the new logic to deploy*

The processing includes:

* reloading the data from the CDC output topics, 
* change S3 Sink Connector to write to new bucket folders
* re-run the batch processing to create the bronze, silver and gold records for consumption by the query engine to serve data to the business intelligence dashboard. 

When the blue data set is ready the query engine switch to an other location.

While in real-time processing the concept of blue-green deployment should only be limited to the Flink pipeline impacted, as presented in [the pipeline management chapter](./pipeline_mgr.md).

The following figure illustrates the Flink statements processing data across source, intermediate, and fact tables. 

<figure markdown="span">
![](./images/bg_2_1.drawio.png)
<caption>**Figure 2**: Real-time processing with Apache Flink within a Data Stream Plarform</caption>
</figure>

On the left side, Raw data originates from Change Data Capture of a transactional database or from event-driven microservices utilizing the [transactional outbox pattern](https://jbcodeforce.github.io/eda-studies/patterns/#transactional-outbox). Given the volume of data injected into these raw topics and the need to retain historical data for extended periods, these topics should be rarely re-created.

*To simplify the diagram above the sink Kafka connectors to the object storage buckets and Iceberg or Delta Lake format are not presented, but it is assumed that those connectors support upsert semantic.* 

On right side, Iceberg or Delta Lake tables, stored in Apache Parquet format, are directly queried by the query engine. Each pipeline writes records in table format to object storage, such as an S3 bucket, with data partitioned within folders.

### Git flow process

As Flink SQLs are managed in a git repository, the process starts by the identification of the files modified from a certain time on a given branch.

As an example, the code release goal is to modify only the purple statements and redeploy them as part of a blue-green deployment. 

The general strategy for query evolution involves replacing the existing statement and its corresponding tables with a new statement and new tables. A straightforward approach is to use a release branch for a short time period, modify the purple Flink statements, and then deploy those statements to the development, staging environments. Once validated, these statements can be merged into the `main` branch where production deploymment may be done. 

The gitflow process may look like:

* **main branch**: This branch always reflects the production-ready, stable code. Only thoroughly tested and finalized code is merged into `main`. Commits are tagged in `main` with version numbers for easy tracking of releases.
* **develop branch**: This branch serves as the integration point for all new features and ongoing development. **Feature branches** are created from the `develop` branch and merged back into it after completion and PR review.
* Creating a **Release Branch**: When a set of features in develop is deemed ready for release, a new release branch is created from `develop`. This branch allows for final testing, bug fixes, and release-specific tasks without interrupting the ongoing development work in develop.
* **Finalizing the Release**: Only bug fixes and necessary adjustments are made on the `release` branch. New feature development is strictly avoided.
* **Merging and Tagging**: Once the release branch is stable and ready for production deployment, it's merged into two places:

    * `main`: The release branch is merged into main, effectively updating the production-ready code with the new release.
    * `develop`: The release branch is also merged back into develop to ensure that any bug fixes or changes made during the release preparation are incorporated into the ongoing development work.

* **Tagging**: After merging into main, the merge commit is tagged with a version number (e.g., v1.0.0) to mark the specific release point in the repository's history.
* **Cleanup**: After the release is finalized and merged, the release branch can be safely deleted

<figure markdown="span">
![](./images/bg_2_2_branch.drawio.png)
<caption>**Figure 3**:GitFlow branching for Flink Statement updates</caption>
</figure>

An alternate approach is to work directly to the `main` branch:

<figure markdown="span">
![](./images/bg_2_2_main_branch.drawio.png)
<caption>**Figure 3-bis**: Branching from main, for Flink Statement updates</caption>
</figure>

### Flink pipelines deployment

To illustrate the needs, we will start by this flink pipeline topology, running in production:

<figure markdown="span">
![](./images/bg_2_3_0.drawio.png){ width=800 }
<caption>**Figure 4**: Current Flink Statements in production</caption>
</figure>

The process needs to get the list of changed flink statements from a given tag or date on a given git branch. The shift left tool can get the list of statements modified from a date:
    
```sh
# At the project folder level do:
shift_left project list-modified-files --project-path . --file-filter sql --since 2025-09-10 main
```

The above command may list that the tables: `int 3`, `fact 3` were modified and `view 1` was added. Looking at the impact of those changes, the tool needs to redeploy the following tables with a new version:

<figure markdown="span">
![](./images/bg_2_3.drawio.png){ width=800 }
<caption>**Figure 5**: Flink logic update and impacted statements</caption>
</figure>

The above figure illustrates those new tables:

| Blue Table name | Green Table name | Statement name | <div style="width:600px">Triggered Change</div> | 
| --------------- | ---------------- |----------------|----------------- |
|    int_3        |     int_3_v2     |  dml.int_3     | User modified content, tool adds _v2 |
|    fact_3       |     fact_3_v2    |  dml.fact_3    | User modified content, tool adds _v2 |
|    fact_2       |     fact_2_v2    |  dml.fact_2    | tool adds _v2 for output as it modified input(s). fact_2 was not modified in the git, this is a side effect of the relationship|
|                 |     view32   |  dml.view32   | User created this new content - no extension |

Also as a side effect the sink connectors configuration need to be modified to go to _v2 topics and even add one new connector because of the new table.

The command creates two files under the $HOME/.shift_left folder: 

| File name | Type |<div style="width:600px">Content</div> |
| --- | --- | --- |
| modified_flink_files.txt | json | contains a filelist with element like: <code>{"table_name": "p1_dim_c2",</br>"file_modified_url": "...pipelines/dimensions/p1/dim_c2/sql-scripts/ddl.dim_c2.sql",</br>"same_sql_content": false,"running": false }</code> |
| modified_flink_files_short.txt | txt | list of table name only |

With this, it will be possible to assess the execution plan with:

```sql
shift_left pipeline build-execution-plan --table-list-file-name  ~/.shift_left/modified_flink_files_short.txt
```


The DDL Flink statements need to have a new table name with the next version postfix (e.g. int_3_v2). 

```sql
--- DDL intermediate table
create table int_3_v2 (
    --- all columns, new columns, ...
)
```
The DML with the `insert into` table name also needs to be modified.

```sql
-- DML intermediate table
insert into int_3_v2 
select 
...
from src_a ...
join src_b ... 
join src_c  ...
```

Any children of the modified statement needs to take into account the new table name of it input tables. For example the fact table needs to use the new versioned intermediate table:

```sql
--- DML Fact table
insert into fact_3_v2
select 
...
from int_3_v2  -- ATTENTION 
join int_1
```

For the 'view' creation, the Flink statement may be impacted as one of its source table is modified. So the same renaming logic applies.

During the tuning on the impacted statements, the pipeline dependencies can help assessing which statements to change. (e.g. `shift_left pipeline build-execution-plan --table-name <flink-intermediate> --may-start-descendants`).

The list of impacted table can be specified in a text file and specified as parameter to the deployment:

```sh
shift_left pipeline deploy --table-list-file-name statement_list.txt --may-start-descendants
```

## Different deployment scenarios

To demonstrate blue/green deployment we will take the [flink_project_demos git repo](https://github.com/jbcodeforce/flink_project_demos/tree/main/customer_360/c360_flink_processing) as a source of a real-time processing using a Kimball structure.

???- info "Access to the the flink demos repository"
    Clone the [https://github.com/jbcodeforce/flink_project_demos](https://github.com/jbcodeforce/flink_project_demos/tree/main/customer_360/c360_flink_processing).
    Set environment variables like:
    ```sh
    export FLINK_PROJECT=$HOME/Code/flink_project_demos/customer_360/c360_flink_processing
    export PIPELINES=$FLINK_PROJECT/pipelines
    ```

The current pipeline is presented in this figure:

![](./images/bg_c360_flink_pipeline_graph.drawio.png)

### Version migration rules

* For table with non existant `_v[0-9]+` as postfix, then use the default postfix of `_v2`
* For table with existing `_v[0-9]+`, then extract the numerical value, and add one: `_v2` -> `_v3`, `_v9` -> `_v10`
* Statement name does not need to have this name update. 

### Sink tables

Some Facts and Views have no child, in this case the version management is to modify the DDL and DML to change the current table with a new version. No need to walk to descendants

### Intermediate tables

For any table with descendants, need to modify DDL and DML of current, and for each direct descedant modify DML for new input version of current table, and then create new version for DDL and DML. Continue recursively until a table has no descendant.

The list of table modified will increase because of those changes.

### Source schema evolution 

For source schema evolution, it is assumed the modifications are schema compatible with Full Transitive semantic. 

<figure markdown="span">
![](./images/bg_2_4.drawio.png)
<caption>**Figure 6**: Transactional data change: schema evolution</caption>
</figure>

The CDC raw topic will contain records with both old and new schema definitions. The initial Flink statement, responsible for creating the source topic, is affected as it must now process new columns. This statement, which may handle deduplication, filtering, primary key redefinition, and field encryption, is designed to process both the previous and new schema versions. When Flink Statements are executed they load the last version of a schema and keep it in their state. Restarting the first level of Flink statements (to create src_), will get the last schema version, the new version. As it may reprocess records earliest offset, it will generate new records with default value or new values. This is a new version too.

So it may make sense to take into account schema modification in the registry as part of the CI/CD process to be able to modify the list of modified file, as Flink statements to create those src_ may not be aware to the change done into the transactional table schema.

## A blue/green deployment process

### Pre-deployment activities

* Get the list of Flink statements modified in `main` branch since a given date:
	```sh
	shift_left project list-modified-files --project-path . --file-filter sql --since 2025-09-10 main
	```
	This will create `$HOME/.shift_left/modified_flink_files.txt` and `$HOME/.shift_left/modified_flink_files.json`
* **Create git release branch** (`git checkout -b v1.0.1`)
* Potentially modify the `modified_flink_files_short.txt` to include table SREs know they have to change.
* Modify each Flink statement for the modified tables so the DDLs and `insert into` of the DLMs use the new version postfix, taking into consideration their descendants.
	```sh
	shift_left project update-table-version $HOME/.shift_left/modified_flink_files.json --default_version _v2
	```

* From the list of tables impacted, review execution plan
	```sh
	 shift_left pipeline build-execution-plan --table-list-file-name .shift_left/modified_flink_files_short.txt --version _v2
	```
* Verify resource (compute pool and CFU usage) availability
* Deploy to the `stage` environment (use a specific config.yaml file): an environment with existing Flink statements are already running
    ```sh
	export CONFIG_FILE=staging_config.yaml
    shift_left pipeline deploy --table-list-file-name .shift_left/modified_flink_files_short.txt
    ```

###  Data Quality Validation

The tools and practices need to address:

* **Schema Compatibility Checks:** Validate that new table versions maintain backward compatibility
* **Data Lineage Validation:** Ensure data flows correctly through the new pipeline versions
* **Record Count Validation:** Compare record counts between blue/green versions
* **Data Freshness Checks:** Validate that data processing latency hasn't increased

### Integration tests

Generate comprehensive test data for blue-green validation

* Send a sample of synthetic test data to source topics, validate they reach sink tables
* Verify no duplicate records are created from the new deployment in the output tables
* Validate all source records are processed
* Compare aggregations between blue/green versions
* Ensure event time processing remains consistent

### Test rollback procedures
