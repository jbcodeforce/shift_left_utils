# SQL translation methodology

The current implementation from dbt / Spark SQL to Flink SQL is prompt based. There are a better approach to improve the quality of the translation.

The core idea of the current implementation is to leverage LLM to understand the SQL semantic and translate to the target Flink SQL. Using the `qwen2.5-coder` model, it was assumed there are Flink SQL source code used to train the model, which seems to be a valid assumption. 

## Current approach

LLM alone might not be sufficuent to address complex translations and as part of an automated process. Agents should help as we can specialized them for a certain step of the process. Feedback loop and retries can be done. The current workflow is the basiest one:

1. Translate the given sql content
1. Verify the syntax
1. Generate the DDL derived from the DML.

What needs to be added is validation agents that execute syntatic validation and automatic deployment, using a feedback loop when the validation fails to inject error message to the translator agent.

## Future work

For any language translation, we need to start with a corpus of code source. It can be done programmatically from the source language, then for each generated code implement the semantically equivalent Flink SQL counterparts. 

The goal of this corpus creation is to identify common ksqlDB or Spark SQL constructs (joins, aggregations, window functions, UDFs, etc.), then manually translate a smaller, diverse set of queries to establish translation rules. Then using these rules, we can generate permutations and variations of queries. It is crucial to test the generated Flink SQL against a test dataset to ensure semantic equivalence.

Build a query pair to represent the source to target set if example as corpus. For each query pair, include the relevant table schemas. This is vital for the LLM to understand data types, column names, and relationships. It is not recommended to have different prompts for different part of a SQL, as the LLM strength comes from the entire context. But still there will be problem for sql scripts that have a lot of line of code, as a 200 lines script will reach thosand of tokens. 

To improve result accuracy, it is possible to use Supervised Fine-tuning techniques:

* Fine-tune the chosen LLM on the parallel corpus generated bove. The goal is for the LLM to learn the translation patterns and nuances between ksqlDB or Spark SQL and Flink SQL.
* Prompt Engineering: Experiment with different prompt structures during fine-tuning and inference. A good prompt will guide the LLM effectively. The current implementation leverage this type of prompts: e.g., "Translate the following Spark SQL query to Flink SQL, considering the provided schema. Ensure semantic equivalence and valid Flink syntax."

To assess the evaluation it is recommended to add a step to the agentic workflow to validate the syntax of the generated Flink SQL. 

The better validation, is to assess semantic equivalence by assessing if the Flink SQL query produces the same results as the or ksqlDB, Spark SQL query on a given dataset.

For validation it may be relevant to have a knowledge base of common translation error. When the Validation Agent reports an error, the Refinement Agent attempts to correct the Flink SQL. It might feed the error message back to the LLM with instructions to fix it. The knowledgebBase should be populated with human-curated rules for common translation pitfalls.

It may be important to explain why translation was done a certain way to better tune prompts. For complex queries or failures, a human review ("human in the loop") and correction mechanism will be essential, with the system learning from these corrections.

### Limitations

LLMs won't magically translate custom UDFs. This will likely require manual intervention or a separate mapping mechanism. The system should identify and flag untranslatable UDFs.

Flink excels at stateful stream processing. Spark SQL's batch orientation means translating stateful Spark operations (if they exist) to their Flink streaming counterparts would be highly complex and likely require significant human oversight or custom rules.


### Spark SQL to Flink SQL

While **Spark SQL** is primarily designed for batch processing, it also supports streaming. As part of shifting the processing more to real time, we consider migrating batch processing. Both SQL systems support standard SQL, but there are differences in their extensions and functions. Most basic SQL syntax (SELECT, FROM, WHERE, JOIN) is similar between Spark and Flink.

Flink SQL has more advanced windowing capabilities. For example:
    ```sql
     -- Spark SQL (using DataFrame API)
     val windowedDF = df.withWatermark("timestamp", "1 minute")
       .groupBy(window(col("timestamp"), "5 minutes"))
       .count()

     -- Flink SQL
     SELECT COUNT(*) FROM users
     GROUP BY TUMBLE(timestamp, INTERVAL '5' MINUTE);
    ```

### ksqlDB to Flink SQL

The prompts are defined in the [src/shift_left/core/utils/prompts/ksql_fsql folder](https://github.com/jbcodeforce/shift_left_utils/blob/main/src/shift_left/src/shift_left/core/utils/prompts)

Be sure to have a Python virtual environment created and the `uv` package manager (see [setup instructions](https://jbcodeforce.github.io/shift_left_utils/contributing/#environment-set-up) for developers). Use a config file with all the necessary setting and the environment variable CONFIG_FILE to point to this file.

kSQLdb has some SQL constructs but this is not a ANSI SQL engine. It is highly integrated with Kafka and uses keyword to define such integration. The migration and prompts need to support migration examples outside of the classical select and create table.

For kSQL the approach is to use 3 agents with different prompt and the Confluent Cloud for Flink REST API to deploy the translated SQL for validation.

| Agent | Scope| Prompt |
| --- | --- | --- |
| Translator | Take ksql content and do a first raw translation to Flink SQL | core/utils/prompts/ksql_fsql/translator.txt |
| Validation | Validate import Flink constructs | core/utils/prompts/ksql_fsql/mandatory_validation.txt |
| Refinement | Fix potential Flink statement deployment error | core/utils/prompts/ksql_fsql/refinement.txt |

See the translator class [src/shift_left/core/utils/ksql_code_agent.py](https://github.com/jbcodeforce/shift_left_utils/blob/main/src/shift_left/src/shift_left/core/utils/ksql_code_agent.py).

The code uses the multi-agent with human in the loop to validate the SQL on Confluent Cloud for Flink. The flow looks like:

![](./images/ai_agent_new_flow.drawio.png)

#### Test Data Set

The [tests/data/ksql-project](https://github.com/jbcodeforce/shift_left_utils/blob/main/src/shift_left/src/shift_left/tests/data/ksql-project/sources) folder includes a set of ksql scripts to be used as benchmark for migration. The matching Flink SQL is in the `flink-references` folder.

#### Running unit testing

To execute the migration use the following command:

```sh
uv run pytest -s tests/ai/test_ksql_migration.py
```

The migrated Flink SQL statements are saved under `src/shift_left/tests/data/ksql-project/staging/ut` folder using the table structure defined in shift_left tool.

#### Demonstration

* Set environment variables:
  ```sh
  export CONFIG_FILE=tests/config-ccloud.yaml
  export PIPELINES=tests/data/ksql-project/staging/ut
  export STAGING=tests/data/ksql-project/staging/ut
  export SRC_FOLDER=tests/data/ksql-project/sources
  ```

* Migrate the basic table with:
  ```sh
  shift_left table migrate basic_table_stream $SRC_FOLDER/ddl-basic-table.ksql $STAGING --source-type ksql
  ```

* Create synthetic data for this basic table:
  ```sh
  ```