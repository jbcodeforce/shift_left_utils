"""
Copyright 2024-2025 Confluent, Inc.

Integration tests for the pipeline Typer app (local data under tests/data/flink-project).

Commands that call Confluent/Flink APIs (prepare, analyze-pool-usage) are skipped
unless SHIFT_LEFT_RUN_CLOUD_IT is set to 1/true/yes and you use a real config/credentials.
"""
import os
import tempfile
import unittest
from pathlib import Path

from typer.testing import CliRunner

from shift_left.cli_commands.pipeline import app

_TESTS_ROOT = Path(__file__).resolve().parent.parent.parent
_PIPELINES = Path(os.environ.get("PIPELINES", str(_TESTS_ROOT / "data/flink-project/pipelines")))

_CLOUD_IT = os.environ.get("SHIFT_LEFT_RUN_CLOUD_IT", "").lower() in ("1", "true", "yes")

_DML_P1_FCT = _PIPELINES / "facts/p1/fct_order/sql-scripts/dml.p1_fct_order.sql"


class TestPipelineCLI(unittest.TestCase):
    """Pipeline CLI: report, build-metadata, report-running-statements, optional cloud commands."""

    @classmethod
    def setUpClass(cls):
        data_dir = _TESTS_ROOT / "data"
        os.environ.setdefault("PIPELINES", str(data_dir / "flink-project/pipelines"))

    def test_report_command_success(self):
        """Report command prints hierarchy for a known table."""
        runner = CliRunner()
        pl = str(_PIPELINES)
        result = runner.invoke(app, ["report", "p1_fct_order", pl])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("p1_fct_order", result.stdout)
        self.assertIn("int_p1_table_1", result.stdout)

    def test_report_command_error(self):
        """Report exits with error when table is not in inventory."""
        runner = CliRunner()
        pl = str(_PIPELINES)
        result = runner.invoke(app, ["report", "non_existent_table", pl])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("Table not found", result.stdout)



    def test_build_metadata_command_error_invalid_file(self):
        """build-metadata requires a .sql file as first argument."""
        runner = CliRunner()
        pl = str(_PIPELINES)
        result = runner.invoke(app, ["build-metadata", "invalid_file.txt", pl])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("the first parameter needs to be a dml sql file", result.stdout)

    def test_report_running_statements_command_error_no_params(self):
        """report-running-statements requires table, product, or dir."""
        runner = CliRunner()
        pl = str(_PIPELINES)
        result = runner.invoke(app, ["report-running-statements", pl])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("either table-name, product-name or dir must be provided", result.stdout)

    @unittest.skipUnless(_CLOUD_IT, "Set SHIFT_LEFT_RUN_CLOUD_IT=1 and use real Confluent config")
    def test_prepare_command_with_sql_file(self):
        """prepare runs SQL via Confluent Flink API (opt-in)."""
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("-- test\nSELECT 1;\n")
            path = f.name
        try:
            result = runner.invoke(app, ["prepare", path])
            self.assertEqual(result.exit_code, 0, msg=result.stdout)
            self.assertIn(path, result.stdout)
        finally:
            os.unlink(path)

    @unittest.skipUnless(_CLOUD_IT, "Set SHIFT_LEFT_RUN_CLOUD_IT=1 and use real Confluent config")
    def test_analyze_pool_usage_command(self):
        """analyze-pool-usage calls Confluent APIs (opt-in)."""
        runner = CliRunner()
        pl = str(_PIPELINES)
        result = runner.invoke(app, ["analyze-pool-usage", pl])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("Analyzing compute pool usage", result.stdout)

    @unittest.skipUnless(_CLOUD_IT, "Set SHIFT_LEFT_RUN_CLOUD_IT=1 and use real Confluent config")
    def test_analyze_pool_usage_command_with_product(self):
        runner = CliRunner()
        pl = str(_PIPELINES)
        result = runner.invoke(app, ["analyze-pool-usage", pl, "--product-name", "p1"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)

    @unittest.skipUnless(_CLOUD_IT, "Set SHIFT_LEFT_RUN_CLOUD_IT=1 and use real Confluent config")
    def test_analyze_pool_usage_command_with_directory(self):
        runner = CliRunner()
        pl = str(_PIPELINES)
        test_dir = str(_TESTS_ROOT / "data/flink-project/pipelines/facts")
        result = runner.invoke(app, ["analyze-pool-usage", pl, "--directory", test_dir])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)


if __name__ == "__main__":
    unittest.main()
