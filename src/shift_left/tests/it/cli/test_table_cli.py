"""
Copyright 2024-2025 Confluent, Inc.

Integration tests for the table Typer CLI.

For commands that need your real Confluent/demo project, run from `src/shift_left` after:

  source set_demo_env

`set_demo_env` sets SL_CONFIG_FILE, PIPELINES, STAGING, SRC_FOLDER, and API credentials.
Local CI uses tests/config.yaml + dummy env from tests/it/conftest.py when demo env is not loaded.
"""
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from it.BaseIT import IntegrationTestCase
from typer.testing import CliRunner

from shift_left.cli_commands.table import app
from it.BaseIT import _DEFAULT_STAGING,IntegrationTestCase, _run_integration_tests, _SKIP_MSG, _PIPELINES
# True after `source set_demo_env` (export added there)

_DEMO_IT = os.environ.get("SL_IT_USE_DEMO_ENV", "").lower() in ("1", "true", "yes")
_DEFAULT_PIPELINES = os.environ.get("PIPELINES", str(_PIPELINES))


_RUN_IT = _run_integration_tests()
@unittest.skipUnless(_RUN_IT, _SKIP_MSG)
class TestTableCLI(IntegrationTestCase):
    """Table CLI: inventory, makefiles, search-deps, update-tables; optional demo-only commands."""

    def test_update_tables_command_basic(self):
        pl = os.environ.get("PIPELINES", str(_DEFAULT_PIPELINES))
        result = self.runner.invoke(
            app,
            [
                "update-tables",
                pl,
                "--string-to-change-from",
                "test_old",
                "--string-to-change-to",
                "test_new",
            ],
        )
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("Done: processed:", result.stdout)

    def test_update_tables_command_ddl_only(self):
        pl = os.environ.get("PIPELINES", str(_DEFAULT_PIPELINES))
        result = self.runner.invoke(app, ["update-tables", pl, "--ddl"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("Done: processed:", result.stdout)

    def test_update_tables_command_both_ddl_dml(self):
        pl = os.environ.get("PIPELINES", str(_DEFAULT_PIPELINES))
        result = self.runner.invoke(app, ["update-tables", pl, "--both-ddl-dml"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("Done: processed:", result.stdout)

    @unittest.skipUnless(_DEMO_IT, "Requires: source set_demo_env (sets SL_IT_USE_DEMO_ENV=1)")
    def test_explain_command_with_table_name(self):
        result = self.runner.invoke(app, ["explain", "--table-name", "p1_fct_order"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)

    @unittest.skipUnless(_DEMO_IT, "Requires: source set_demo_env (sets SL_IT_USE_DEMO_ENV=1)")
    def test_explain_command_with_product_name(self):
        result = self.runner.invoke(app, ["explain", "--product-name", "p1"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)

    def test_explain_command_error_no_params(self):
        result = self.runner.invoke(app, ["explain"])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("Error: table or dir needs to be provided", result.stdout)

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
