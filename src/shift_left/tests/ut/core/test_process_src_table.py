"""
Copyright 2024-2025 Confluent, Inc.
"""
import pytest
import unittest
import shutil
from unittest.mock import patch, MagicMock, mock_open
import os, pathlib
import tempfile
from typing import List

os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
os.environ["PIPELINES"] =  str(pathlib.Path(__file__).parent.parent.parent /  "data/flink-project/pipelines")

# need to be before the import of migrate_one_file
data_dir = pathlib.Path(__file__).parent.parent.parent / "data"  # Path to the data directory
os.environ["TOPIC_LIST_FILE"] = str(data_dir / "flink-project/src_topic_list.txt")
from shift_left.core.process_src_tables import (
    migrate_one_file, 
    _save_dml_ddl, 
    _search_matching_topic,
    _find_sub_string,
    _remove_already_processed_table,
    _process_ddl_file
)

DDL="""
    CREATE TABLE IF NOT EXISTS a_table (

  -- put here column definitions
  PRIMARY KEY(default_key) NOT ENFORCED
) DISTRIBUTED BY HASH(default_key) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
"""

DML="""
INSERT INTO a_table
SELECT 
-- part to select stuff
FROM src_table
WHERE -- where condition or remove it
"""

@pytest.fixture(autouse=True)
def mock_llm_result():
    return (DDL, DML)

class TestProcessSrcTable(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "spark-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")

    def _get_env_var(self, var_name: str) -> str:
        """Get environment variable with proper null checking."""
        value = os.getenv(var_name)
        if value is None:
            raise ValueError(f"{var_name} environment variable is not set")
        return value

    @patch("shift_left.core.process_src_tables.get_or_build_sql_translator_agent")
    def test_process_one_file(self, TranslatorToFlinkSqlAgent):
        """Test basic file migration without parent processing"""
        mock_translator_agent = TranslatorToFlinkSqlAgent.return_value
        # Fixed: Spark agent returns (ddl_string, dml_string)
        mock_translator_agent.translate_to_flink_sqls.return_value = (DDL, DML)
     
        staging = self._get_env_var("STAGING")
        src_folder = self._get_env_var("SRC_FOLDER")
        
        if os.path.exists(staging):
            shutil.rmtree(staging)        
        migrate_one_file("fct_users",
            src_folder + "/facts/p5/fct_users.sql",   
            staging,   
            src_folder,
            process_parents=False,
            source_type="spark",
            validate=False)
        assert os.path.exists(staging + "/fct_users")
        assert os.path.exists(staging + "/fct_users/fct_users/sql-scripts/dml.fct_users.sql")
        assert os.path.exists(staging + "/fct_users/fct_users/sql-scripts/ddl.fct_users.sql")
        
        # Verify file contents
        with open(staging + "/fct_users/fct_users/sql-scripts/dml.fct_users.sql", "r") as f:
            assert DML.strip() in f.read()
        with open(staging + "/fct_users/fct_users/sql-scripts/ddl.fct_users.sql", "r") as f:
            content = f.read()
            # DDL gets processed, so check for key elements
            assert "CREATE TABLE" in content

    @patch("shift_left.core.process_src_tables.get_or_build_sql_translator_agent")  
    def test_process_one_file_recurring(self, TranslatorToFlinkSqlAgent):
        """Test file migration with parent processing"""
        mock_translator_agent = TranslatorToFlinkSqlAgent.return_value
        # Fixed: Spark agent returns (ddl_string, dml_string)
        mock_translator_agent.translate_to_flink_sqls.return_value = (DDL, DML)
        
        staging = self._get_env_var("STAGING")
        src_folder = self._get_env_var("SRC_FOLDER")
        
        if os.path.exists(staging):
            shutil.rmtree(staging)
            
        migrate_one_file("fct_users",
            src_folder + "/facts/p5/fct_users.sql",   
            staging,   
            src_folder,
            process_parents=True,
            source_type="spark",
            validate=False)
        assert os.path.exists(staging + "/raw_active_users")
        assert os.path.exists(staging + "/raw_active_users/raw_active_users/sql-scripts/dml.raw_active_users.sql")
        assert os.path.exists(staging + "/raw_active_users/raw_active_users/sql-scripts/ddl.raw_active_users.sql")
        
        # Fixed: Check actual file content with proper expectations
        with open(staging + "/dim_user_groups/dim_user_groups/sql-scripts/dml.dim_user_groups.sql", "r") as f:
            content = f.read()
            assert DML.strip() in content
        with open(staging + "/dim_user_groups/dim_user_groups/sql-scripts/ddl.dim_user_groups.sql", "r") as f:
            content = f.read()
            # DDL gets processed, check for key elements instead of exact match
            assert "CREATE TABLE" in content

    def test_save_dml_ddl_with_strings(self):
        """Test _save_dml_ddl function with string inputs (recently fixed bug)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create sql-scripts directory
            scripts_dir = os.path.join(temp_dir, "sql-scripts")
            os.makedirs(scripts_dir)
            
            # Test with strings (the bug scenario)
            _save_dml_ddl(temp_dir, "test_table", DML, DDL)
            
            # Verify files were created correctly
            dml_file = os.path.join(scripts_dir, "dml.test_table.sql")
            ddl_file = os.path.join(scripts_dir, "ddl.test_table.sql")
            
            assert os.path.exists(dml_file)
            assert os.path.exists(ddl_file)
            
            # Verify content (should be full strings, not individual characters)
            with open(dml_file, 'r') as f:
                content = f.read()
                assert len(content) > 10  # Should be full SQL, not single characters
                assert "INSERT INTO" in content
            
            with open(ddl_file, 'r') as f:
                content = f.read()
                assert len(content) > 10  # Should be full SQL, not single characters
                assert "CREATE TABLE" in content

    def test_save_dml_ddl_with_lists(self):
        """Test _save_dml_ddl function with list inputs"""
        with tempfile.TemporaryDirectory() as temp_dir:
            scripts_dir = os.path.join(temp_dir, "sql-scripts")
            os.makedirs(scripts_dir)
            
            # Test with lists
            _save_dml_ddl(temp_dir, "test_table", [DML], [DDL])
            
            dml_file = os.path.join(scripts_dir, "dml.test_table.sql")
            ddl_file = os.path.join(scripts_dir, "ddl.test_table.sql")
            
            assert os.path.exists(dml_file)
            assert os.path.exists(ddl_file)

    def test_save_dml_ddl_with_multiple_statements(self):
        """Test _save_dml_ddl function with multiple statements"""
        with tempfile.TemporaryDirectory() as temp_dir:
            scripts_dir = os.path.join(temp_dir, "sql-scripts")
            os.makedirs(scripts_dir)
            
            # Test with multiple statements
            ddl_list = [DDL, "CREATE TABLE table2 (id INT);"]
            dml_list = [DML, "INSERT INTO table2 VALUES (1);"]
            
            _save_dml_ddl(temp_dir, "test_table", dml_list, ddl_list)
            
            # Should create files with indexes
            assert os.path.exists(os.path.join(scripts_dir, "dml.test_table.sql"))
            assert os.path.exists(os.path.join(scripts_dir, "dml.test_table_1.sql"))
            assert os.path.exists(os.path.join(scripts_dir, "ddl.test_table.sql"))
            assert os.path.exists(os.path.join(scripts_dir, "ddl.test_table_1.sql"))

    def test_save_dml_ddl_with_none_and_empty(self):
        """Test _save_dml_ddl function with None and empty values"""
        with tempfile.TemporaryDirectory() as temp_dir:
            scripts_dir = os.path.join(temp_dir, "sql-scripts")
            os.makedirs(scripts_dir)
            
            # Test with None values
            _save_dml_ddl(temp_dir, "test_table", None, None)
            
            # Should not create any files
            files = os.listdir(scripts_dir)
            assert len(files) == 0
            
            # Test with empty strings
            _save_dml_ddl(temp_dir, "test_table", "", "")
            
            # Should not create any files
            files = os.listdir(scripts_dir)
            assert len(files) == 0

    def test_search_matching_topic_exact_match(self):
        """Test _search_matching_topic with exact match"""
        # Create a temporary topic list file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as tmp_file:
            tmp_file.write("orders,org.orders.v1\n")
            tmp_file.write("users,org.users.v2\n")
            tmp_file.write("products,org.products.v1\n")
            tmp_file_path = tmp_file.name
        
        try:
            # Patch the TOPIC_LIST_FILE at module level
            with patch('shift_left.core.process_src_tables.TOPIC_LIST_FILE', tmp_file_path):
                result = _search_matching_topic("orders", [])
                assert result == "org.orders.v1"
                
                result = _search_matching_topic("users", [])
                assert result == "org.users.v2"
                
        finally:
            os.unlink(tmp_file_path)

    def test_search_matching_topic_with_prefixes(self):
        """Test _search_matching_topic with prefix filtering"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as tmp_file:
            tmp_file.write("orders,org.orders.v1\n")
            tmp_file.write("orders,test.orders.v1\n")
            tmp_file.write("orders,dev.orders.v1\n")
            tmp_file_path = tmp_file.name
        
        try:
            # Patch the TOPIC_LIST_FILE at module level
            with patch('shift_left.core.process_src_tables.TOPIC_LIST_FILE', tmp_file_path):
                # Should filter out test. and dev. prefixes
                result = _search_matching_topic("orders", ["test.", "dev."])
                assert result == "org.orders.v1"
                
        finally:
            os.unlink(tmp_file_path)

    def test_find_sub_string_matching(self):
        """Test _find_sub_string function"""
        # Test exact word matching
        assert _find_sub_string("user_orders", "org.user.orders.v1") == True
        assert _find_sub_string("customer_data", "org.customer.data.v2") == True
        
        # Test partial matching
        assert _find_sub_string("orders", "org.customer.orders.v1") == True
        
        # Test no matching
        assert _find_sub_string("products", "org.customer.orders.v1") == False
        assert _find_sub_string("user_profiles", "org.customer.data.v1") == False

    def test_process_ddl_file(self):
        """Test _process_ddl_file function"""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_content = "SELECT * FROM ``` final AS (\n  SELECT col1\n) \nSELECT * FROM final"
            test_file = os.path.join(temp_dir, "test.sql")
            
            with open(test_file, 'w') as f:
                f.write(test_content)
            
            _process_ddl_file(temp_dir, test_file)
            
            with open(test_file, 'r') as f:
                processed_content = f.read()
            
            # Should remove the final AS part and SELECT * FROM final
            assert "``` final AS (" not in processed_content
            assert "SELECT * FROM final" not in processed_content
            assert "```" in processed_content
            assert "SELECT col1" in processed_content

    def test_migrate_one_file_invalid_source_type(self):
        """Test migrate_one_file with invalid source type"""
        with pytest.raises(Exception) as exc_info:
            migrate_one_file("test_table", "test.sql", "/tmp", "/src", 
                           source_type="invalid_type")
        assert "source_type parameter needs to be one of" in str(exc_info.value)

    def test_migrate_one_file_invalid_file_extension(self):
        """Test migrate_one_file with invalid file extension"""
        with pytest.raises(Exception) as exc_info:
            migrate_one_file("test_table", "test.txt", "/tmp", "/src", 
                           source_type="spark")
        # Fixed: Match actual error message from code
        assert "sql_src_file parameter needs to be a sql file" in str(exc_info.value)

    @patch("shift_left.core.process_src_tables.KsqlToFlinkSqlAgent")
    def test_migrate_one_file_ksql(self, mock_ksql_agent):
        """Test migrate_one_file with KSQL source type"""
        # Mock the KSQL agent
        mock_agent_instance = mock_ksql_agent.return_value
        mock_agent_instance.translate_from_ksql_to_flink_sql.return_value = ([DDL], [DML])
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a test KSQL file
            ksql_file = os.path.join(temp_dir, "test.ksql")
            with open(ksql_file, 'w') as f:
                f.write("CREATE STREAM test_stream AS SELECT * FROM source_stream;")
            
            migrate_one_file("test_stream", ksql_file, temp_dir, temp_dir, 
                           source_type="ksql")
            
            # Verify the KSQL agent was called
            mock_agent_instance.translate_from_ksql_to_flink_sql.assert_called_once()
            
            # Verify folder structure was created
            assert os.path.exists(os.path.join(temp_dir, "test_stream"))

            
if __name__ == "__main__":
    unittest.main()