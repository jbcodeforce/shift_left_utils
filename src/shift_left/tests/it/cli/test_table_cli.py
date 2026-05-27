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
from it.BaseIT import _DEFAULT_STAGING,IntegrationTestCase
# True after `source set_demo_env` (export added there)
_DEMO_IT = os.environ.get("SL_IT_USE_DEMO_ENV", "").lower() in ("1", "true", "yes")


class TestTableCLI(IntegrationTestCase):
    """Table CLI: inventory, makefiles, search-deps, update-tables; optional demo-only commands."""

    @classmethod
    def setUpClass(cls):
        cls.super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        staging = os.environ.get("STAGING", str(_DEFAULT_STAGING))
        temp_dir = os.path.join(staging, "data_product_1")
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    def test_init_table(self):
        staging = os.environ.get("STAGING", str(_DEFAULT_STAGING))
        target = os.path.join(staging, "data_product_1", "sources")
        os.makedirs(target, exist_ok=True)
        result = self.runner.invoke(app, ["init", "src_table_5", target])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("table_5", result.stdout)
        self.assertTrue(os.path.exists(os.path.join(target, "src_table_5")))
        self.assertTrue(os.path.exists(os.path.join(target, "src_table_5", "Makefile")))

    def test_build_inventory(self):
        result = self.runner.invoke(app, ["build-inventory"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertTrue(os.path.exists(os.path.join(os.environ.get("PIPELINES"), "inventory.json")))

    def test_search_parents_of_table(self):
        sql_file = os.path.join(os.environ.get("SRC_FOLDER"), "facts", "users", "fct_users.sql")
        self.assertTrue(os.path.exists(sql_file), f"Missing fixture: {sql_file}")
        result = self.runner.invoke(
            app,
            ["search-source-dependencies", sql_file, os.environ.get("SRC_FOLDER")],
        )
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("fct_users", result.stdout)

    def test_update_makefile(self):
        runner = CliRunner()
        pl = os.environ.get("PIPELINES", str(_DEFAULT_PIPELINES))
        # Inventory key is src_a under p2 (not src_p2_a)
        result = runner.invoke(app, ["update-makefile", "src_a", pl])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        mk = os.path.join(pl, "sources", "p2", "src_a", "Makefile")
        self.assertTrue(os.path.exists(mk), f"Expected Makefile at {mk}")

    @unittest.skipUnless(_DEMO_IT, "Requires: source set_demo_env (sets SL_IT_USE_DEMO_ENV=1)")
    def test_migrate_command(self):
        """migrate uses the AI stack; run only with demo env."""
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("SELECT * FROM test_table;\n")
            path = f.name
        try:
            staging = os.environ.get("STAGING", str(_DEFAULT_STAGING))
            result = runner.invoke(
                app,
                ["migrate", "test_migrated_table", path, staging],
            )
            self.assertEqual(result.exit_code, 0, msg=result.stdout)
        finally:
            os.unlink(path)

    def test_update_all_makefiles_command(self):
        runner = CliRunner()
        pl = os.environ.get("PIPELINES", str(_DEFAULT_PIPELINES))
        result = runner.invoke(app, ["update-all-makefiles", pl])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("Updated", result.stdout)
        self.assertIn("Makefiles", result.stdout)

    def test_validate_table_names_command(self):
        runner = CliRunner()
        pl = os.environ.get("PIPELINES", str(_DEFAULT_PIPELINES))
        result = runner.invoke(app, ["validate-table-names", pl])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)

    def test_update_tables_command_basic(self):
        runner = CliRunner()
        pl = os.environ.get("PIPELINES", str(_DEFAULT_PIPELINES))
        result = runner.invoke(
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
        runner = CliRunner()
        pl = os.environ.get("PIPELINES", str(_DEFAULT_PIPELINES))
        result = runner.invoke(app, ["update-tables", pl, "--ddl"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("Done: processed:", result.stdout)

    def test_update_tables_command_both_ddl_dml(self):
        runner = CliRunner()
        pl = os.environ.get("PIPELINES", str(_DEFAULT_PIPELINES))
        result = runner.invoke(app, ["update-tables", pl, "--both-ddl-dml"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("Done: processed:", result.stdout)

    @unittest.skipUnless(_DEMO_IT, "Requires: source set_demo_env (sets SL_IT_USE_DEMO_ENV=1)")
    def test_explain_command_with_table_name(self):
        runner = CliRunner()
        result = runner.invoke(app, ["explain", "--table-name", "p1_fct_order"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)

    @unittest.skipUnless(_DEMO_IT, "Requires: source set_demo_env (sets SL_IT_USE_DEMO_ENV=1)")
    def test_explain_command_with_product_name(self):
        runner = CliRunner()
        result = runner.invoke(app, ["explain", "--product-name", "p1"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)

    def test_explain_command_error_no_params(self):
        runner = CliRunner()
        result = runner.invoke(app, ["explain"])
        self.assertEqual(result.exit_code, 1)
        self.assertIn("Error: table or dir needs to be provided", result.stdout)

    @unittest.skipUnless(_DEMO_IT, "Requires: source set_demo_env (sets SL_IT_USE_DEMO_ENV=1)")
    def test_explain_command_with_table_list_file(self):
        runner = CliRunner()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("p1_fct_order\n")
            path = f.name
        try:
            result = runner.invoke(
                app, ["explain", "--table-list-file-name", path]
            )
            self.assertEqual(result.exit_code, 0, msg=result.stdout)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
