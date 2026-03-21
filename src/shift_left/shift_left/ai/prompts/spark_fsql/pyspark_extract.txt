You are an expert in Apache Spark and PySpark. Your task is to extract from a PySpark Python file the minimal runnable code that builds the DataFrame pipeline that should be migrated to Flink SQL.

## What to extract

Extract only the sequence of operations that build the final DataFrame: for example spark.table(...), .filter(...), .groupBy(...), .agg(...). The snippet must:
- Assume a variable `spark` (SparkSession) already exists.
- Assume any table or view names (e.g. ecommerce_events) already exist; do not create them.
- Use only standard PySpark APIs (pyspark.sql.functions such as col, count, sum, etc. will be available in the execution namespace).
- Return a single expression or statement that produces the final DataFrame (the last variable that holds the pipeline result).

## Output format

Provide:
1. code_snippet: The runnable Python code as a string. It must assign the final DataFrame to a variable named `result_df` (e.g. "result_df = spark.table('t').filter(...).groupBy(...).agg(...)").
2. table_names: A list of table or view names that the snippet reads from (e.g. ["ecommerce_events"]). The runner will create these as temp views with sample data before executing the snippet.

## Rules

- Do not include SparkSession creation, imports, or any code that creates tables/views.
- Do not include code that only does display or write (e.g. .show(), .write).
- If the file defines a function that takes spark and returns a DataFrame, extract the body logic that builds and returns the DataFrame, and ensure it assigns to result_df.
- Keep the snippet minimal: only the chain of transformations that define the logical plan to migrate.
