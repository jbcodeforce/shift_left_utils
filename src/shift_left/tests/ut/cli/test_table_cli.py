import os
import pathlib
import shutil
import tempfile

import yaml

from ut.BaseUT import SLUnitTestCase
from shift_left.cli_commands.table import app, _set_default_post_fix_unit_test
from shift_left.cli_commands.pipeline import app as pipeline_app
from shift_left.core.utils.app_config import reset_config_cache

import shift_left.core.test_mgr as test_mgr

import json

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
            inventory = json.load(f)

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
        print( result.stdout)


    def test_init_unit_tests(self):
        result = self.runner.invoke(app, ["build-inventory"])
        assert result.exit_code == 0
        result = self.runner.invoke(pipeline_app, ["build-all-metadata"])
        assert result.exit_code == 0
        test_folder = os.path.join(self.pipelines, "facts", "c360", "fct_user_per_group", "tests")

        try:
            result = self.runner.invoke(app, ["init-unit-tests", "sl_c360_fct_user_per_group"])
            assert result.exit_code == 0
            print( result.stdout)
            assert os.path.exists(test_folder)
            assert os.path.exists(os.path.join(test_folder, "ddl_sl_c360_dim_users.sql"))
            assert os.path.exists(os.path.join(test_folder, "insert_sl_c360_dim_users_1.sql"))
            assert os.path.exists(os.path.join(test_folder, "validate_sl_c360_fct_user_per_group_1.sql"))
            assert os.path.exists(os.path.join(test_folder, "test_definitions.yaml"))
        finally:
            test_def, table_ref = test_mgr._load_test_suite_definition(table_name="sl_c360_fct_user_per_group")
            assert test_def
            print(test_def.model_dump_json(indent=3))
            assert test_def.foundations[0].table_name == "sl_c360_dim_users"
            assert test_def.foundations[0].ddl_for_test == "./tests/ddl_sl_c360_dim_users.sql"

            if os.path.exists(test_folder):
                shutil.rmtree(test_folder)


