"""
Run the filter.py pipeline with in-memory data, export the Catalyst logical plan
as JSON, and optionally run the FlinkSQLGenerator visitor to produce Flink SQL.

Usage (from this directory):
  uv run python run_and_export_plan.py [--output catalyst_plan.json] [--flink]
  uv run python run_and_export_plan.py --from-file filter.py [--output ...] [--flink]
  uv run python run_and_export_plan.py --from-file joins_.py --flink
  uv run python run_and_export_plan.py --from-file filter.py --llm-extract [--output ...] [--flink]
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

# Sample data for common temp views (e.g. ecommerce_events)
_SAMPLE_ROWS = [
    ("user_1", "purchase"),
    ("user_1", "view"),
    ("user_2", "purchase"),
]
_SAMPLE_SCHEMA = "user_id string, event_name string"


def build_pipeline(spark: SparkSession):
    """Run the real filter.py pipeline: create view then import and call filter.run(spark)."""
    spark.createDataFrame(_SAMPLE_ROWS, _SAMPLE_SCHEMA).createOrReplaceTempView(
        "ecommerce_events"
    )
    from filter import run

    return run(spark)


def build_pipeline_from_file(spark: SparkSession, file_path: str):
    """
    Load a PySpark pipeline module from file_path, call setup_views(spark) if present,
    then run(spark) and return the resulting DataFrame.
    """
    path = os.path.abspath(file_path)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Pipeline file not found: {path}")
    script_dir = os.path.dirname(path)
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    spec = importlib.util.spec_from_file_location("pipeline_module", path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load module from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if not hasattr(mod, "run"):
        raise ValueError(f"Pipeline module must define run(spark); found in {path}")
    if hasattr(mod, "setup_views"):
        mod.setup_views(spark)
    return mod.run(spark)


def build_pipeline_from_llm_extract(spark: SparkSession, snippet: str, table_names: list[str]):
    """
    Create temp views for table_names, then exec the snippet in a restricted namespace.
    The snippet must assign the final DataFrame to result_df.
    """
    for name in table_names or ["ecommerce_events"]:
        spark.createDataFrame(_SAMPLE_ROWS, _SAMPLE_SCHEMA).createOrReplaceTempView(name)
    namespace = {
        "spark": spark,
        "col": F.col,
        "count": F.count,
        "sum": F.sum,
        "min": F.min,
        "max": F.max,
        "avg": F.avg,
        "lit": F.lit,
        "when": F.when,
        "expr": F.expr,
    }
    exec(snippet, namespace)
    if "result_df" not in namespace:
        raise ValueError("Extracted snippet did not assign a DataFrame to result_df")
    return namespace["result_df"]


def get_plan_json(df) -> str:
    """Get the logical plan as JSON string. Prefer optimized plan if available."""
    qe = df._jdf.queryExecution()
    try:
        plan = qe.optimizedPlan()
    except Exception:
        plan = qe.logical()
    return plan.toJSON()


def main():
    parser = argparse.ArgumentParser(description="Export Catalyst plan and optionally generate Flink SQL")
    parser.add_argument(
        "--output",
        default="catalyst_plan.json",
        help="Path to write the plan JSON",
    )
    parser.add_argument(
        "--flink",
        action="store_true",
        help="Run FlinkSQLGenerator on the plan and print Flink SQL",
    )
    parser.add_argument(
        "--from-file",
        metavar="PATH",
        help="Use this PySpark .py file as the pipeline source",
    )
    parser.add_argument(
        "--llm-extract",
        action="store_true",
        help="Use LLM to extract the migratable snippet from the file (requires --from-file and shift_left)",
    )
    args = parser.parse_args()

    if args.llm_extract and not args.from_file:
        print("--llm-extract requires --from-file", file=sys.stderr)
        sys.exit(1)

    spark = SparkSession.builder.appName("ExportPlan").master("local[1]").getOrCreate()
    try:
        if args.from_file and args.llm_extract:
            pkg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
            parent_dir = os.path.dirname(pkg_dir)
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
            from shift_left.ai.pyspark_extractor import extract_pipeline_from_pyspark

            with open(args.from_file) as f:
                source = f.read()
            snippet, table_names = extract_pipeline_from_pyspark(source)
            print(f"LLM extracted snippet ({len(snippet)} chars), tables: {table_names}", file=sys.stderr)
            result_df = build_pipeline_from_llm_extract(spark, snippet, table_names)
        elif args.from_file:
            result_df = build_pipeline_from_file(spark, args.from_file)
        else:
            result_df = build_pipeline(spark)
        plan_json_str = get_plan_json(result_df)
    finally:
        spark.stop()

    with open(args.output, "w") as f:
        f.write(plan_json_str)
    print(f"Wrote plan to {args.output}", file=sys.stderr)

    if args.flink:
        # Add parent of shift_left package so "from shift_left.ai import ..." works
        # run_and_export_plan.py is at .../shift_left/tests/data/pyspark-project
        pkg_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..")
        )
        parent_dir = os.path.dirname(pkg_dir)
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        try:
            from shift_left.ai.catalyst_to_flink import FlinkSQLGenerator
        except ImportError as e:
            print(
                "FlinkSQLGenerator not found: install shift_left (e.g. uv pip install -e . from src/shift_left) or run from shift_left with uv run.",
                file=sys.stderr,
            )
            print(str(e), file=sys.stderr)
            sys.exit(1)
        plan_obj = json.loads(plan_json_str)
        gen = FlinkSQLGenerator()
        gen.visit(plan_obj)
        print(gen.generate_sql())


if __name__ == "__main__":
    main()
