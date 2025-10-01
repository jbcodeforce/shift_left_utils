"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from typer.testing import CliRunner
import os
from pathlib import Path
os.environ["CONFIG_FILE"] = str(Path(__file__).parent.parent.parent / "config-ccloud.yaml")
os.environ["PIPELINES"] = str(Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
from shift_left.core.utils.app_config import shift_left_dir
from shift_left.cli_commands.pipeline import app
import shift_left.core.table_mgr as table_mgr

class TestPipelineCLI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = Path(__file__).parent.parent.parent / "data"  # Path to the data directory



    def test_report_command_success(self):
        """Test successful execution of the report command"""
       
        runner = CliRunner()
        result = runner.invoke(app, ['report', 'p1_fct_order'])
        assert result.exit_code == 0
        assert "p1_fct_order" in result.stdout
        assert "int_p1_table_1" in result.stdout

    def test_report_command_error(self):
        """Test error handling when pipeline data cannot be retrieved"""

        runner = CliRunner()
        result = runner.invoke(app, ['report', 'non_existent_table'])
        assert result.exit_code == 1
        assert "Table not found" in result.stdout



    def test_build_metadata_command_success(self):
        """Test successful execution of the build-metadata command"""
        
        runner = CliRunner()
        # Using a test data directory path for DML file
        data_dir = Path(__file__).parent.parent.parent / "data"
        test_dml_file = str(os.getenv("PIPELINES") + "/facts/p1/p1_fct_order/sql-scripts/dml.p1_fct_order.sql")
        
        result = runner.invoke(app, ['build-metadata', test_dml_file, os.getenv("PIPELINES")])
        assert result.exit_code == 0
        assert "Pipeline built from" in result.stdout

    def test_build_metadata_command_error_invalid_file(self):
        """Test error handling when invalid file is provided to build-metadata"""
        
        runner = CliRunner()
        result = runner.invoke(app, ['build-metadata', 'invalid_file.txt', os.getenv("PIPELINES")])
        assert result.exit_code == 1
        assert "Error: the first parameter needs to be a dml sql file" in result.stdout

   

    def test_report_running_statements_command_error_no_params(self):
        """Test error handling when no parameters provided to report-running-statements"""
        
        runner = CliRunner()
        result = runner.invoke(app, ['report-running-statements', os.getenv("PIPELINES")])
        assert result.exit_code == 1
        assert "Error: either table-name, product-name or dir must be provided" in result.stdout

    def test_prepare_command_with_sql_file(self):
        """Test prepare command with SQL file"""
        
        runner = CliRunner()
        # Create a temporary SQL file for testing
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write("-- Test SQL content\nSELECT 1;\n")
            temp_sql_file = f.name
        
        try:
            result = runner.invoke(app, ['prepare', temp_sql_file])
            # This command depends on actual Flink infrastructure, so we just test basic execution
            print(result.stdout)
        finally:
            os.unlink(temp_sql_file)

    def test_analyze_pool_usage_command(self):
        """Test successful execution of the analyze-pool-usage command"""
        
        runner = CliRunner()
        result = runner.invoke(app, ['analyze-pool-usage', os.getenv("PIPELINES")])
        # This command depends on actual Flink infrastructure, so we just test basic execution
        print(result.stdout)

    def test_analyze_pool_usage_command_with_product(self):
        """Test analyze-pool-usage command with product filter"""
        
        runner = CliRunner()
        result = runner.invoke(app, ['analyze-pool-usage', os.getenv("PIPELINES"), '--product-name', 'p1'])
        # This command depends on actual Flink infrastructure, so we just test basic execution  
        print(result.stdout)

    def test_analyze_pool_usage_command_with_directory(self):
        """Test analyze-pool-usage command with directory filter"""
        
        runner = CliRunner()
        data_dir = Path(__file__).parent.parent.parent / "data"
        test_dir = str(data_dir / "flink-project/pipelines/facts")
        
        result = runner.invoke(app, ['analyze-pool-usage', os.getenv("PIPELINES"), '--directory', test_dir])
        # This command depends on actual Flink infrastructure, so we just test basic execution
        print(result.stdout)


if __name__ == '__main__':
    unittest.main()