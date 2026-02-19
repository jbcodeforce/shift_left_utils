# PySpark examples for Catalyst plan export

- **filter.py**: Pipeline that reads `ecommerce_events`, filters by `event_name = 'purchase'`, and aggregates by `user_id` with `COUNT(*)` as `purchase_count`.
- **run_and_export_plan.py**: Runs the same pipeline with in-memory data (temp view), exports the Catalyst logical plan to JSON, and optionally runs the FlinkSQLGenerator visitor to produce Flink SQL.

## Run and export plan

From this directory:

```bash
uv run python run_and_export_plan.py --output catalyst_plan.json
```

To also generate Flink SQL, the `shift_left` package must be on `PYTHONPATH` or installed (e.g. from `src/shift_left`: `uv pip install -e .`). Then:

```bash
uv run python run_and_export_plan.py --flink
```

The generated Flink SQL matches the plan: `SELECT user_id, COUNT(1) AS purchase_count FROM default.ecommerce_events WHERE event_name = 'purchase' GROUP BY user_id;`

