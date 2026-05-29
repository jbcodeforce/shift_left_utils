"""
Copyright 2024-2025 Confluent, Inc.

Integration tests for the table Typer CLI (cloud/demo commands only).

For commands that need your real Confluent/demo project, run from `src/shift_left` after:

  source set_demo_env

Local-only table CLI tests live in tests/ut/cli/test_table_cli.py.
"""
import os
import tempfile
import unittest

from shift_left.cli_commands.table import app
from it.BaseIT import IntegrationTestCase, _run_integration_tests, _SKIP_MSG

_DEMO_IT = os.environ.get("SL_IT_USE_DEMO_ENV", "").lower() in ("1", "true", "yes")

_RUN_IT = _run_integration_tests()


@unittest.skipUnless(_RUN_IT, _SKIP_MSG)
class TestTableCLICloud(IntegrationTestCase):
    """Table CLI commands that require demo/cloud Flink API access."""

    @unittest.skipUnless(_DEMO_IT, "Requires: source set_demo_env (sets SL_IT_USE_DEMO_ENV=1)")
    def test_explain_command_with_table_name(self):
        result = self.runner.invoke(app, ["explain", "--table-name", "p1_fct_order"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)

    @unittest.skipUnless(_DEMO_IT, "Requires: source set_demo_env (sets SL_IT_USE_DEMO_ENV=1)")
    def test_explain_command_with_product_name(self):
        result = self.runner.invoke(app, ["explain", "--product-name", "p1"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)

    @unittest.skipUnless(_DEMO_IT, "Requires: source set_demo_env (sets SL_IT_USE_DEMO_ENV=1)")
    def test_explain_command_with_table_list_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("p1_fct_order\n")
            path = f.name
        try:
            result = self.runner.invoke(
                app, ["explain", "--table-list-file-name", path]
            )
            self.assertEqual(result.exit_code, 0, msg=result.stdout)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
