"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
import shutil
from typer.testing import CliRunner

from shift_left.cli_commands.table import app
from shift_left.core.utils.app_config import shift_left_dir

class TestTableCLI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent.parent / "data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "spark-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        os.environ["CONFIG_FILE"] =  shift_left_dir +  "/it-config.yaml"

    @classmethod
    def tearDownClass(cls):
        temp_dir = os.getenv("STAGING") + "/data_product_1"
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    def test_init_table(self):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "src_table_5", os.getenv("STAGING") + "/data_product_1/sources"])
        assert result.exit_code == 0
        assert "table_5" in result.stdout
        assert os.path.exists( os.getenv("STAGING") + "/data_product_1/sources/src_table_5")
        assert os.path.exists( os.getenv("STAGING") + "/data_product_1/sources/src_table_5/Makefile")


    def test_build_inventory(self):
        runner = CliRunner()
        result = runner.invoke(app, ["build-inventory", os.getenv("PIPELINES")])
        assert result.exit_code == 0
        assert os.path.exists(os.getenv("PIPELINES") + "/inventory.json")

    def test_search_parents_of_table(self):
        runner = CliRunner()
        result = runner.invoke(app, ["search-source-dependencies", os.getenv("SRC_FOLDER") + "/facts/p5/fct_users.sql", os.getenv("SRC_FOLDER")])
        assert result.exit_code == 0
        print(result)

    def test_update_makefile(self):
        runner = CliRunner()
        result = runner.invoke(app, ["update-makefile", "src_p2_a", os.getenv("PIPELINES")])
        assert result.exit_code == 0
        assert os.path.exists(os.getenv("PIPELINES") + "/sources/p2/src_a/Makefile")


    # !!!Test Mamagement of Unit Tests and Integration Tests are done in separate tests. see it/ folder.
    

    def test_migrate_command(self):
        """Test migrate command with basic parameters"""
        runner = CliRunner()
        
        # Create a temporary SQL file for testing migration
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write("SELECT * FROM test_table;\n")
            temp_sql_file = f.name
        
        try:
            result = runner.invoke(app, [
                "migrate", 
                "test_migrated_table", 
                temp_sql_file, 
                os.getenv("STAGING")
            ])
            # This command depends on AI integration, so we just test basic execution
            print(result.stdout)
        finally:
            os.unlink(temp_sql_file)

    def test_update_all_makefiles_command(self):
        """Test update-all-makefiles command"""
        runner = CliRunner()
        
        result = runner.invoke(app, ["update-all-makefiles", os.getenv("PIPELINES")])
        assert result.exit_code == 0
        assert "Updated" in result.stdout
        assert "Makefiles" in result.stdout

    def test_validate_table_names_command(self):
        """Test validate-table-names command"""
        runner = CliRunner()
        
        result = runner.invoke(app, ["validate-table-names", os.getenv("PIPELINES")])
        assert result.exit_code == 0
        print(result.stdout)

    def test_update_tables_command_basic(self):
        """Test update-tables command with basic parameters"""
        runner = CliRunner()
        
        result = runner.invoke(app, [
            "update-tables", 
            os.getenv("PIPELINES"),
            "--string-to-change-from", "test_old",
            "--string-to-change-to", "test_new"
        ])
        assert result.exit_code == 0
        assert "Done: processed:" in result.stdout

    def test_update_tables_command_ddl_only(self):
        """Test update-tables command with DDL only option"""
        runner = CliRunner()
        
        result = runner.invoke(app, [
            "update-tables", 
            os.getenv("PIPELINES"),
            "--ddl"
        ])
        assert result.exit_code == 0
        assert "Done: processed:" in result.stdout

    def test_update_tables_command_both_ddl_dml(self):
        """Test update-tables command with both DDL and DML option"""
        runner = CliRunner()
        
        result = runner.invoke(app, [
            "update-tables", 
            os.getenv("PIPELINES"),
            "--both-ddl-dml"
        ])
        assert result.exit_code == 0
        assert "Done: processed:" in result.stdout


    def test_explain_command_with_table_name(self):
        """Test explain command with table name"""
        runner = CliRunner()
        
        result = runner.invoke(app, ["explain", "--table-name", "p1_fct_order"])
        # This command depends on actual Flink infrastructure
        print(result.stdout)

    def test_explain_command_with_product_name(self):
        """Test explain command with product name"""
        runner = CliRunner()
        
        result = runner.invoke(app, ["explain", "--product-name", "p1"])
        # This command depends on actual Flink infrastructure
        print(result.stdout)

    def test_explain_command_error_no_params(self):
        """Test explain command error when no parameters provided"""
        runner = CliRunner()
        
        result = runner.invoke(app, ["explain"])
        assert result.exit_code == 1
        assert "Error: table or dir needs to be provided" in result.stdout

    def test_explain_command_with_table_list_file(self):
        """Test explain command with table list file"""
        runner = CliRunner()
        
        # Create a temporary table list file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("p1_fct_order\n")
            temp_file = f.name
        
        try:
            result = runner.invoke(app, ["explain", "--table-list-file-name", temp_file])
            # This command depends on actual Flink infrastructure
            print(result.stdout)
        finally:
            os.unlink(temp_file)

if __name__ == '__main__':
    unittest.main()