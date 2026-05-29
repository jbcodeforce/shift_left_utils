"""
Copyright 2024-2025 Confluent, Inc.

Cloud integration tests for the pipeline Typer app.

Local pipeline CLI behavior is covered by tests/ut/cli/test_pipeline_cli.py.
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


@unittest.skipUnless(_CLOUD_IT, "Set SHIFT_LEFT_RUN_CLOUD_IT=1 and use real Confluent config")
class TestPipelineCLICloud(unittest.TestCase):
    """Pipeline CLI commands that call Confluent/Flink APIs (opt-in)."""

    @classmethod
    def setUpClass(cls):
        data_dir = _TESTS_ROOT / "data"
        os.environ.setdefault("PIPELINES", str(data_dir / "flink-project/pipelines"))

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

    def test_analyze_pool_usage_command(self):
        """analyze-pool-usage calls Confluent APIs (opt-in)."""
        runner = CliRunner()
        pl = str(_PIPELINES)
        result = runner.invoke(app, ["analyze-pool-usage", pl])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("Analyzing compute pool usage", result.stdout)

    def test_analyze_pool_usage_command_with_product(self):
        runner = CliRunner()
        pl = str(_PIPELINES)
        result = runner.invoke(app, ["analyze-pool-usage", pl, "--product-name", "p1"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)

    def test_analyze_pool_usage_command_with_directory(self):
        runner = CliRunner()
        pl = str(_PIPELINES)
        test_dir = str(_TESTS_ROOT / "data/flink-project/pipelines/facts")
        result = runner.invoke(app, ["analyze-pool-usage", pl, "--directory", test_dir])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)


if __name__ == "__main__":
    unittest.main()
