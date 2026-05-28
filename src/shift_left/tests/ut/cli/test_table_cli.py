import os
import shutil

from ut.BaseUT import SLUnitTestCase
from shift_left.cli_commands.table import app
from shift_left.cli_commands.pipeline import app as pipeline_app

import json

class TestTableCLI(SLUnitTestCase):


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
            if os.path.exists(test_folder):
                shutil.rmtree(test_folder)


