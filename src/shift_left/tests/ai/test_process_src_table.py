"""
Copyright 2024-2025 Confluent, Inc.
"""
import pytest
import unittest
import shutil
from unittest.mock import patch
import os, pathlib
import tempfile
from typing import List

os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
os.environ["PIPELINES"] =  str(pathlib.Path(__file__).parent.parent /  "data/flink-project/pipelines")

# need to be before the import of migrate_one_file
data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
os.environ["TOPIC_LIST_FILE"] = str(data_dir / "flink-project/src_topic_list.txt")
from shift_left.ai.process_src_tables import (
    migrate_one_file,
    _save_dmls_ddls,
    _search_matching_topic,
    _find_sub_string,
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


    def test_save_dml_ddl_with_strings(self):
        """Test _save_dml_ddl function with string inputs (recently fixed bug)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create sql-scripts directory
            scripts_dir = os.path.join(temp_dir, "sql-scripts")
            os.makedirs(scripts_dir)

            # Test with strings (the bug scenario)
            _save_dmls_ddls(temp_dir, "test_table", [DML], [DDL])

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

    def test_save_dml_ddl_with_multiple_statements(self):
        """Test _save_dml_ddl function with multiple statements"""
        with tempfile.TemporaryDirectory() as temp_dir:
            scripts_dir = os.path.join(temp_dir, "sql-scripts")
            os.makedirs(scripts_dir)

            # Test with multiple statements
            ddl_list = [DDL, "CREATE TABLE table2 (id INT);"]
            dml_list = [DML, "INSERT INTO table2 VALUES (1);"]

            _save_dmls_ddls(temp_dir, "test_table", dml_list, ddl_list)

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
            _save_dmls_ddls(temp_dir, "test_table", [], [])

            # Should not create any files
            files = os.listdir(scripts_dir)
            assert len(files) == 0

            # Test with empty strings
            _save_dmls_ddls(temp_dir, "test_table", [""], [""])

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
            with patch('shift_left.ai.process_src_tables.TOPIC_LIST_FILE', tmp_file_path):
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
            with patch('shift_left.ai.process_src_tables.TOPIC_LIST_FILE', tmp_file_path):
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
            migrate_one_file("test_table", "test.sql",
                           staging_target_folder="/tmp",
                           source_type="invalid_type")
        assert "source_type parameter needs to be one of" in str(exc_info.value)

    def test_migrate_one_file_invalid_file_extension(self):
        """Test migrate_one_file with invalid file extension"""
        with pytest.raises(Exception) as exc_info:
            migrate_one_file("test_table", "test.txt", staging_target_folder="/tmp",
                           source_type="spark")
        # Fixed: Match actual error message from code
        assert "sql_src_file parameter needs to be a sql file" in str(exc_info.value)



    @patch("shift_left.ai.ksql_code_agent.KsqlToFlinkSqlAgent.translate_to_flink_sqls")
    def test_simple_ksql_creation_folders_and_files(self, mock_translate_to_flink_sqls):
        mock_translate_to_flink_sqls.return_value = ["CREATE TABLE BASIC_TABLE_STREAM"], ["SELECT * FROM BASIC_TABLE_STREAM"]
        src_folder = str(data_dir / "ksql-project/sources")
        staging = str(data_dir / "flink-project/staging/ut")
        product_name = "basic"
        migrate_one_file(table_name="BASIC_TABLE_STREAM",
                        sql_src_file=src_folder + "/ddl-basic-table.ksql",
                        staging_target_folder=staging,
                        product_name=product_name,
                        source_type="ksql",
                        validate=False)
        assert os.path.exists(staging + "/"+ product_name + "/basic_table_stream")
        assert os.path.exists(staging + "/"+ product_name + "/basic_table_stream/sql-scripts/dml.basic_table_stream.sql")
        assert os.path.exists(staging + "/"+ product_name + "/basic_table_stream/sql-scripts/ddl.basic_table_stream.sql")
        shutil.rmtree(staging)

    @patch("shift_left.ai.spark_sql_code_agent.SparkToFlinkSqlAgent.translate_to_flink_sqls")
    def test_simple_spark_creation_folders_and_files(self, mock_translate_to_flink_sqls):
        mock_translate_to_flink_sqls.return_value = ["CREATE TABLE src_customer_journey"], ["SELECT * FROM src_customer_journey"]
        src_folder = str(data_dir / "spark-project/sources")
        staging = str(data_dir / "flink-project/staging/ut")
        product_name = "c360"
        migrate_one_file(table_name="src_customer_journey",
                        sql_src_file=src_folder + "/c360/src_customer_journey.sql",
                        staging_target_folder=staging,
                        product_name=product_name,
                        source_type="spark",
                        validate=False)
        assert os.path.exists(staging + "/"+ product_name + "/src_customer_journey")
        assert os.path.exists(staging + "/"+ product_name + "/src_customer_journey/sql-scripts/dml.src_customer_journey.sql")
        assert os.path.exists(staging + "/"+ product_name + "/src_customer_journey/sql-scripts/ddl.src_customer_journey.sql")
        # Verify file contents
        with open(staging + "/"+ product_name + "/src_customer_journey/sql-scripts/dml.src_customer_journey.sql", "r") as f:
            content = f.read()
            assert len(content) > 10
        with open(staging + "/"+ product_name + "/src_customer_journey/sql-scripts/ddl.src_customer_journey.sql", "r") as f:
            content = f.read()
            assert "CREATE TABLE" in content
        shutil.rmtree(staging)

if __name__ == "__main__":
    unittest.main()
