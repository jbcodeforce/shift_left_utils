import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib


import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from shift_left.core.flink_statement_model import Statement

class TestTableManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent.parent / "data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
        tm.build_inventory(os.getenv("PIPELINES"))
        
    def test_extract_table_name(self):
        pathname= "p1/fct_order"
        tbn = tm.extract_table_name(pathname)
        assert tbn == "fct_order"

    def test_create_table_structure(self):
   
        try:
            tbf, tbn =tm.build_folder_structure_for_table("it2",os.getenv("STAGING") + "/intermediates")
            assert os.path.exists(tbf)
            assert os.path.exists(tbf + "/" + tm.SCRIPTS_DIR)
            assert os.path.exists(tbf + "/" + tm.SCRIPTS_DIR + "/ddl.it2.sql" )
            assert os.path.exists(tbf + "/" + tm.SCRIPTS_DIR + "/dml.it2.sql" )
            assert os.path.exists(tbf + "/Makefile")
        except Exception as e:
            print(e)
            self.fail()

    def test_update_make_file(self):
        try:
            tbf, tbn =tm.build_folder_structure_for_table("it2",os.getenv("PIPELINES") + "/intermediates/p3")
            assert os.path.exists(tbf + "/Makefile")
            os.remove(tbf+ "/Makefile")
            assert not os.path.exists(tbf + "/Makefile")
            tm.get_or_build_inventory(os.getenv("PIPELINES"), os.getenv("PIPELINES"), True)
            tm.update_makefile_in_folder(os.getenv("PIPELINES"), "it2")
            assert os.path.exists(tbf + "/Makefile")
        except Exception as e:
            print(e)
            self.fail()

    def test_search_users_of_table(self):
        try:
            users = tm.search_users_of_table("int_table_1",os.getenv("PIPELINES"))
            assert users
            assert "fct_order" in users
            print(users)
        except Exception as e:
            print(e)
            self.fail()
       
    @patch('shift_left.core.table_mgr.delete_statement_if_exists')
    @patch('shift_left.core.table_mgr.ConfluentCloudClient')
    def test_drop_table(self, MockConfluentCloudClient, mock_delete_statement_if_exists):
        table_name = "fct_order"
        mock_delete_statement_if_exists.return_value = "deleted"
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_client_instance.post_flink_statement.return_value =  Statement(name= "drop-fct-order")
        result = tm.drop_table(table_name=table_name)
        
        self.assertEqual(result, "fct_order dropped")

        MockConfluentCloudClient.assert_called_once()
        config = get_config()
        cpi= config['flink']['compute_pool_id']
        properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
    
        mock_client_instance.post_flink_statement.assert_called_with(cpi,
                                                                     "drop-fct-order",
                                                                     "drop table if exists fct_order;",
                                                                     properties)
        mock_delete_statement_if_exists.assert_called_with("drop-fct-order")

    @patch('shift_left.core.table_mgr.ConfluentCloudClient')
    @patch('shift_left.core.table_mgr.delete_statement_if_exists')
    def test_get_table_structure_success(self, mock_delete_statement, MockConfluentCloudClient):
        """Test successful retrieval of table structure"""
        table_name = "test_table"
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_statement = MagicMock()
        mock_statement.status.phase = "COMPLETED"
        mock_statement_result = MagicMock()
        mock_statement_result.results.data = ["CREATE TABLE test_table (...)"]
        mock_client_instance.post_flink_statement.return_value = mock_statement
        mock_client_instance.get_statement_results.return_value = mock_statement_result
        
        result = tm.get_table_structure(table_name)
        
        self.assertIsNotNone(result)
        self.assertEqual(result, "['CREATE TABLE test_table (...)']")
        mock_delete_statement.assert_called_with(f"show-{table_name.replace('_', '-')}")
        mock_client_instance.delete_flink_statement.assert_called_with(f"show-{table_name.replace('_', '-')}")

    @patch('shift_left.core.table_mgr.ConfluentCloudClient')
    @patch('shift_left.core.table_mgr.delete_statement_if_exists')
    def test_get_table_structure_failure(self, mock_delete_statement, MockConfluentCloudClient):
        """Test failure scenario when getting table structure"""
        table_name = "non_existent_table"
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_client_instance.post_flink_statement.side_effect = Exception("API Error")
        
        result = tm.get_table_structure(table_name)
        
        self.assertIsNone(result)
        mock_delete_statement.assert_called_with(f"show-{table_name.replace('_', '-')}")
        mock_client_instance.delete_flink_statement.assert_called_with(f"show-{table_name.replace('_', '-')}")

    @patch('shift_left.core.table_mgr.ConfluentCloudClient')
    @patch('shift_left.core.table_mgr.delete_statement_if_exists')
    def test_get_table_structure_empty_result(self, mock_delete_statement, MockConfluentCloudClient):
        """Test scenario when table structure query returns no results"""
        table_name = "empty_table"
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_statement = MagicMock()
        mock_statement.status.phase = "COMPLETED"
        mock_statement_result = MagicMock()
        mock_statement_result.results.data = []
        mock_client_instance.post_flink_statement.return_value = mock_statement
        mock_client_instance.get_statement_results.return_value = mock_statement_result
        
        result = tm.get_table_structure(table_name)
        
        self.assertIsNone(result)
        mock_delete_statement.assert_called_with(f"show-{table_name.replace('_', '-')}")
        mock_client_instance.delete_flink_statement.assert_called_with(f"show-{table_name.replace('_', '-')}")

    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data="SELECT * FROM test_table;")
    def test_load_sql_content_success(self, mock_file):
        """Test successful loading of SQL content from file"""
        sql_file = "test.sql"
        result = tm.load_sql_content(sql_file)
        self.assertEqual(result, "SELECT * FROM test_table;")
        mock_file.assert_called_once_with(sql_file, "r")

    @patch('builtins.open', side_effect=IOError("File not found"))
    def test_load_sql_content_failure(self, mock_file):
        """Test error handling when loading SQL content fails"""
        sql_file = "nonexistent.sql"
        with self.assertRaises(IOError):
            tm.load_sql_content(sql_file)
        mock_file.assert_called_once_with(sql_file, "r")

    class MockTableWorker:
        def __init__(self, update_result, new_content):
            self.update_result = update_result
            self.new_content = new_content

        def update_sql_content(self, content):
            return self.update_result, self.new_content

    @patch('shift_left.core.table_mgr.load_sql_content')
    @patch('builtins.open', new_callable=unittest.mock.mock_open)
    def test_update_sql_content_success(self, mock_file, mock_load):
        """Test successful update of SQL content"""
        sql_file = "test.sql"
        mock_load.return_value = "SELECT * FROM test_table;"
        processor = self.MockTableWorker(True, "SELECT * FROM updated_table;")
        
        result = tm.update_sql_content_for_file(sql_file, processor)
        
        self.assertTrue(result)
        mock_file.assert_called_once_with(sql_file, "w")
        mock_file().write.assert_called_once_with("SELECT * FROM updated_table;")

    @patch('shift_left.core.table_mgr.load_sql_content')
    def test_update_sql_content_no_update_needed(self, mock_load):
        """Test when no SQL content update is needed"""
        sql_file = "test.sql"
        mock_load.return_value = "SELECT * FROM test_table;"
        processor = self.MockTableWorker(False, "SELECT * FROM test_table;")
        
        result = tm.update_sql_content_for_file(sql_file, processor)
        
        self.assertFalse(result)

    @patch('shift_left.core.table_mgr.load_sql_content', side_effect=IOError("File not found"))
    def test_update_sql_content_file_error(self, mock_load):
        """Test error handling when file operations fail"""
        sql_file = "nonexistent.sql"
        processor = self.MockTableWorker(True, "SELECT * FROM updated_table;")
        
        with self.assertRaises(IOError):
            tm.update_sql_content_for_file(sql_file, processor)

if __name__ == '__main__':
    unittest.main()