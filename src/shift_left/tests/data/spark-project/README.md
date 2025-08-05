# Spark SQL Script Validation

This directory contains Spark SQL test scripts and a validation tool to ensure they are syntactically correct and executable.

## Prerequisites

Install PySpark:
```bash
pip install -r requirements.txt
or uv 
```

## Directory Structure

```
spark-project/
├── sources/                    # Spark SQL test scripts
│   ├── src_product_analytics.sql
│   ├── src_customer_journey.sql
│   ├── src_event_processing.sql
│   ├── src_sales_pivot.sql
│   ├── src_streaming_aggregations.sql
│   ├── src_set_operations.sql
│   ├── src_temporal_analytics.sql
│   └── src_advanced_transformations.sql
├── validate_spark_scripts.py   # Validation script
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## Spark SQL Test Scripts

The test scripts cover various Spark SQL features:

1. **`src_product_analytics.sql`** - Window functions, time-based aggregations
2. **`src_customer_journey.sql`** - Complex CTEs, multiple joins, segmentation
3. **`src_event_processing.sql`** - Array/struct operations, JSON parsing
4. **`src_sales_pivot.sql`** - Pivot operations, growth calculations
5. **`src_streaming_aggregations.sql`** - Time windows, real-time analytics
6. **`src_set_operations.sql`** - Set operations (UNION, EXCEPT), subqueries
7. **`src_temporal_analytics.sql`** - Time series analysis, seasonality
8. **`src_advanced_transformations.sql`** - UDF-like operations, ML features

## Running the Validation

### Validate All Scripts

```bash
cd shift_left_utils/src/shift_left/tests/data/spark-project
python validate_spark_scripts.py
```

### Expected Output

```
🚀 Starting Spark SQL Script Validation
==================================================
Creating sample data...
✓ Sample data created successfully

Found 8 SQL scripts to validate:
  - src_product_analytics.sql
  - src_customer_journey.sql
  - ...

🔍 Validating: src_product_analytics.sql
  Executing: src_product_analytics.sql
    ✓ Query executed successfully, returned 5 rows

...

==================================================
📊 VALIDATION SUMMARY
==================================================
Total Scripts: 8
✅ Successful: 8
❌ Failed: 0
Success Rate: 100.0%

✅ SUCCESSFUL SCRIPTS:
  - src_product_analytics.sql: 5 rows
  - src_customer_journey.sql: 3 rows
  ...

🎉 All scripts validated successfully!
```

## What the Validator Does

1. **Sets up a local Spark session** with optimized configurations
2. **Creates sample data** for all tables referenced in the SQL scripts:
   - `raw_product_events`
   - `web_events`, `customer_profiles`, `purchases`
   - `raw_events`
   - `sales_data`
   - `streaming_events`
   - `user_activities`, `feature_usage`
   - `user_events`
   - `raw_transactions`

3. **Executes each SQL script** as a Spark job
4. **Reports results** including:
   - Success/failure status
   - Row counts returned
   - Detailed error messages for failures
   - Overall success rate

## Troubleshooting

### PySpark Not Installed
```
ERROR: PySpark not installed. Please run: pip install pyspark
```
**Solution:** Install PySpark using `pip install pyspark`

### Java Not Found
```
JAVA_HOME is not set
```
**Solution:** Install Java 8+ and set JAVA_HOME environment variable

### Memory Issues
If you encounter memory errors, you can adjust Spark configurations by modifying the `_create_spark_session` method in `validate_spark_scripts.py`:

```python
.config("spark.driver.memory", "2g") \
.config("spark.executor.memory", "2g") \
```

## Integration with Migration Testing

These validated Spark SQL scripts are used by the migration tool tests in:
- `shift_left_utils/src/shift_left/tests/ai/test_spark_migration.py`

The validation ensures that the source SQL is correct before testing the Spark-to-Flink migration process.

## Adding New Test Scripts

1. Create a new `.sql` file in the `sources/` directory with prefix `src_`
2. Ensure it references existing sample tables or add new sample data in `validate_spark_scripts.py`
3. Run the validator to ensure it works
4. Add a corresponding test method in `test_spark_migration.py` 