import json
import os
import pathlib
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import yaml

from ut.BaseUT import SLUnitTestCase
from shift_left.cli import app as root_app
from shift_left.cli_commands.table import app, _set_default_post_fix_unit_test
from shift_left.core.utils.app_config import reset_config_cache

import shift_left.core.test_mgr as test_mgr

_TESTS_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent


class TestTableCLI(SLUnitTestCase):

    def test_set_default_post_fix_unit_test(self):
        post_fix_unit_test = _set_default_post_fix_unit_test(None)
        assert post_fix_unit_test == "_jb"  # coming from config.yaml

        original_config_file = os.environ.get("SL_CONFIG_FILE")
        temp_config_path = None
        try:
            with open(_TESTS_ROOT / "config.yaml") as f:
                config_data = yaml.safe_load(f)
            config_data["app"].pop("post_fix_unit_test", None)

            with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                yaml.dump(config_data, f)
                temp_config_path = f.name

            os.environ["SL_CONFIG_FILE"] = temp_config_path
            reset_config_cache()

            post_fix_unit_test = _set_default_post_fix_unit_test(None)
            assert post_fix_unit_test == test_mgr.DEFAULT_POST_FIX_UNIT_TEST
        finally:
            if temp_config_path and os.path.exists(temp_config_path):
                os.unlink(temp_config_path)
            if original_config_file is None:
                os.environ.pop("SL_CONFIG_FILE", None)
            else:
                os.environ["SL_CONFIG_FILE"] = original_config_file
            reset_config_cache()

        post_fix_unit_test = _set_default_post_fix_unit_test("_at")
        assert post_fix_unit_test == "_at"
        try:
            post_fix_unit_test = _set_default_post_fix_unit_test("at")
        except Exception as e:
            assert "Error: post-fix-unit-test must start with _, be 2 or 3 characters and be alpha numeric" in str(e)

        try:
            _set_default_post_fix_unit_test("ut")
        except Exception as e:
            assert "Error: post-fix-unit-test must start with _, be 2 or 3 characters and be alpha numeric" in str(e)

        try:
            _set_default_post_fix_unit_test("ut_")
        except Exception as e:
            assert "Error: post-fix-unit-test must start with _, be 2 or 3 characters and be alpha numeric" in str(e)

    def test_init_table(self):
        dp_folder = os.path.join(self.pipelines, "sources", "dp")
        try:
            result = self.runner.invoke(app, ["init", "test_table", "sources", "--product-name", "dp"])
            assert result.exit_code == 0
            test_folder = os.path.join(dp_folder, "test_table")
            assert os.path.exists(test_folder)
            assert os.path.exists(os.path.join(test_folder, "Makefile"))
            assert os.path.exists(os.path.join(test_folder, "sql-scripts", "ddl.src_dp_test_table.sql"))
            assert os.path.exists(os.path.join(test_folder, "sql-scripts", "dml.src_dp_test_table.sql"))
        finally:
            if os.path.exists(dp_folder):
                shutil.rmtree(dp_folder)

    def test_build_inventory(self):
        result = self.runner.invoke(app, ["build-inventory"])
        assert result.exit_code == 0
        assert os.path.exists(os.path.join(self.pipelines, "inventory.json"))
        with open(os.path.join(self.pipelines, "inventory.json"), "r") as f:
            json.load(f)

    def test_search_source_dependencies(self):
        assert os.path.exists(os.getenv("SL_SRC_FOLDER"))
        src_file = os.path.join(os.getenv("SL_SRC_FOLDER"), "facts", "users", "fct_users.sql")
        result = self.runner.invoke(app, ["search-source-dependencies", str(src_file)])
        assert result.exit_code == 0
        assert "dim_user_groups" in result.stdout
        assert "raw_active_users" in result.stdout

    def test_validate_table_names(self):
        result = self.runner.invoke(app, ["validate-table-names"])
        assert result.exit_code == 0
        assert "Validate_table_names" in result.stdout

    def test_update_tables_command_basic(self):
        result = self.runner.invoke(
            app,
            [
                "update-tables",
                self.pipelines,
                "--string-to-change-from",
                "test_old",
                "--string-to-change-to",
                "test_new",
            ],
        )
        assert result.exit_code == 0, result.stdout
        assert "Done: processed:" in result.stdout

    def test_update_tables_command_ddl_only(self):
        result = self.runner.invoke(app, ["update-tables", self.pipelines, "--ddl"])
        assert result.exit_code == 0, result.stdout
        assert "Done: processed:" in result.stdout

    def test_update_tables_command_both_ddl_dml(self):
        result = self.runner.invoke(app, ["update-tables", self.pipelines, "--both-ddl-dml"])
        assert result.exit_code == 0, result.stdout
        assert "Done: processed:" in result.stdout

    def test_explain_command_error_no_params(self):
        result = self.runner.invoke(app, ["explain"])
        assert result.exit_code == 1
        assert "Error: table or dir needs to be provided" in result.stdout

    def test_update_makefile(self):
        table_folder = os.path.join(self.pipelines, "intermediates", "p3", "it2")
        makefile_path = os.path.join(table_folder, "Makefile")
        backup = None
        try:
            self.runner.invoke(app, ["build-inventory"])
            with open(makefile_path, "r") as f:
                backup = f.read()
            os.remove(makefile_path)
            assert not os.path.exists(makefile_path)

            result = self.runner.invoke(app, ["update-makefile", "int_p3_it2", self.pipelines])
            assert result.exit_code == 0, result.stdout
            assert "Makefile updated for table int_p3_it2" in result.stdout
            assert os.path.exists(makefile_path)
            with open(makefile_path, "r") as f:
                assert "TABLE_NAME=int_p3_it2" in f.read()
        finally:
            if backup is not None:
                with open(makefile_path, "w") as f:
                    f.write(backup)

    def test_update_all_makefiles(self):
        folder_path = os.path.join(self.pipelines, "intermediates")
        result = self.runner.invoke(app, ["update-all-makefiles", folder_path])
        assert result.exit_code == 0, result.stdout
        assert "Updated" in result.stdout
        assert "Makefiles" in result.stdout

    @patch("shift_left.cli_commands.table.migrate_one_file")
    def test_migrate_smoke(self, mock_migrate):
        src_file = os.path.join(os.getenv("SL_SRC_FOLDER"), "facts", "users", "fct_users.sql")
        with tempfile.TemporaryDirectory() as staging:
            result = self.runner.invoke(
                app,
                ["migrate", "fct_users", src_file, staging, "--source-type", "spark"],
            )
            assert result.exit_code == 0, result.stdout
            assert "Migration completed for fct_users" in result.stdout
            mock_migrate.assert_called_once()

    @patch("shift_left.core.test_mgr.statement_mgr.get_statement_list")
    @patch("shift_left.core.test_mgr.statement_mgr.delete_statement_if_exists")
    @patch("shift_left.core.test_mgr.statement_mgr.drop_table")
    def test_delete_unit_tests(self, mock_drop_table, mock_delete_statement, mock_get_statement_list):
        mock_get_statement_list.return_value = []
        result = self.runner.invoke(
            app,
            ["delete-unit-tests", "p1_fct_order", "--compute-pool-id", "test_pool"],
        )
        assert result.exit_code == 0, result.stdout
        assert "Unit tests deletion for p1_fct_order completed" in result.stdout
        assert mock_delete_statement.called
        assert mock_drop_table.called

    @patch("shift_left.core.test_mgr.execute_one_or_all_tests")
    def test_run_unit_tests_smoke(self, mock_execute):
        mock_result = MagicMock()
        mock_result.model_dump_json.return_value = '{"test_results": {}}'
        mock_execute.return_value = mock_result

        with tempfile.TemporaryDirectory() as log_dir:
            with patch("shift_left.cli_commands.table.session_log_dir", log_dir):
                result = self.runner.invoke(
                    app,
                    ["run-unit-tests", "p1_fct_order", "--compute-pool-id", "test_pool"],
                )
            assert result.exit_code == 0, result.stdout
            assert "Unit tests execution for p1_fct_order completed" in result.stdout
            mock_execute.assert_called_once()

    @patch("shift_left.cli_commands.table.run_validation_tests")
    def test_validate_unit_tests_delegates(self, mock_run_validation):
        mock_result = MagicMock()
        mock_result.model_dump_json.return_value = '{"test_results": {}}'
        mock_result.test_results = {}
        mock_run_validation.return_value = mock_result

        result = self.runner.invoke(
            app,
            ["validate-unit-tests", "p1_fct_order", "--compute-pool-id", "test_pool"],
        )
        assert result.exit_code == 0, result.stdout
        mock_run_validation.assert_called_once()

    def test_init_unit_tests(self):
        result = self.runner.invoke(app, ["build-inventory"])
        assert result.exit_code == 0
        result = self.runner.invoke(root_app, ["pipeline", "build-all-metadata", self.pipelines])
        assert result.exit_code == 0
        test_folder = os.path.join(self.pipelines, "facts", "c360", "fct_user_per_group", "tests")

        try:
            result = self.runner.invoke(app, ["init-unit-tests", "sl_c360_fct_user_per_group"])
            assert result.exit_code == 0
            assert os.path.exists(test_folder)
            assert os.path.exists(os.path.join(test_folder, "ddl_sl_c360_dim_users.sql"))
            assert os.path.exists(os.path.join(test_folder, "insert_sl_c360_dim_users_1.sql"))
            assert os.path.exists(os.path.join(test_folder, "validate_sl_c360_fct_user_per_group_1.sql"))
            assert os.path.exists(os.path.join(test_folder, "test_definitions.yaml"))
        finally:
            test_def, table_ref = test_mgr._load_test_suite_definition(table_name="sl_c360_fct_user_per_group")
            assert test_def
            assert test_def.foundations[0].table_name == "sl_c360_dim_users"
            assert test_def.foundations[0].ddl_for_test == "./tests/ddl_sl_c360_dim_users.sql"

            if os.path.exists(test_folder):
                shutil.rmtree(test_folder)


if __name__ == "__main__":
    unittest.main()
