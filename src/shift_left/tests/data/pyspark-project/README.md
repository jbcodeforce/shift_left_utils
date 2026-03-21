# PySpark examples for Catalyst plan export

Scripts in this directory exercise the automatic migration path: run a PySpark pipeline, export the Catalyst logical plan to JSON, and optionally generate Flink SQL via FlinkSQLGenerator (or LLM extract).

## Pipeline scripts

Each script defines `setup_views(spark)` and `run(spark)` returning a DataFrame. Run directly (e.g. `uv run python filter.py`) or via `run_and_export_plan.py --from-file <script>.py`.

- **filter.py**: Reads `ecommerce_events`, filters by `event_name = 'purchase'`, aggregates by `user_id` with `COUNT(*)` as `purchase_count`.
- **joins_.py**: Joins `orders` and `customers` on `customer_id`, then aggregates `total_amount` and `order_count` per customer (tests Join in Catalyst plan).
- **multi_agg.py**: Reads `sales` (region, amount), aggregates by region with count, sum, min, max, avg (tests multiple aggregate expressions).
- **window_agg.py**: Reads `events`, adds per-user running sum over `event_ts` using a window (tests Window in Catalyst plan).
- **union_and_filter.py**: Unions `stream_a` and `stream_b`, filters to `action = 'click'`, counts by `user_id` (tests Union in Catalyst plan).
- **select_expr.py**: Reads `orders`, filters non-cancelled, adds `is_completed` via `when`/`otherwise`, then aggregates (tests Project with conditional expressions).
- **chain_filter_agg.py**: Filters `transactions` to category A, aggregates by `user_id`, then filters to rows with `tx_count >= 2` (tests chained Filter and Aggregate).

## Run and export plan

From this directory:

```bash
uv run python run_and_export_plan.py --output catalyst_plan.json
```

Use a specific pipeline (without LLM):

```bash
uv run python run_and_export_plan.py --from-file filter.py --output catalyst_plan.json
uv run python run_and_export_plan.py --from-file joins_.py --output plan_joins.json
uv run python run_and_export_plan.py --from-file multi_agg.py --flink
```

To also generate Flink SQL, the `shift_left` package must be on `PYTHONPATH` or installed (e.g. from `src/shift_left`: `uv pip install -e .`). Then:

```bash
uv run python run_and_export_plan.py --flink
```

For LLM-based extraction of a migratable snippet from a file (assigns to `result_df`):

```bash
uv run python run_and_export_plan.py --from-file filter.py --llm-extract [--output ...] [--flink]
```

The generated Flink SQL for filter.py matches: `SELECT user_id, COUNT(1) AS purchase_count FROM default.ecommerce_events WHERE event_name = 'purchase' GROUP BY user_id;`  
Other pipelines (joins, window, union) produce plans that may not be fully translated yet by FlinkSQLGenerator; exporting their plan JSON is useful to drive migration-path improvements.

