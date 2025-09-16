"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from shift_left.core.models.flink_statement_model import Statement
from shift_left.core.utils.file_search import SCRIPTS_DIR

class TestTableManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent.parent / "data"  # Path to the data directory
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        tm.build_inventory(os.getenv("PIPELINES"))
        
    def test_extract_table_name(self):
        pathname= "p1/fct_order"
        tbn = tm.get_short_table_name(pathname)
        assert tbn == "fct_order"

    def test_get_table_name(self):
        table_name = "test_table"
        product_name = "product"
        expected_name = "product_fct_test_table"
        table_type = "fact"
        self.assertEqual(tm.get_long_table_name(table_name, product_name, table_type), expected_name)
        table_type = "intermediate"
        expected_name = "int_product_test_table"
        self.assertEqual(tm.get_long_table_name(table_name, product_name, table_type), expected_name)
        table_type = "source"
        expected_name = "src_product_test_table"
        self.assertEqual(tm.get_long_table_name(table_name, product_name, table_type), expected_name)
        table_type = "dimension"
        expected_name = "product_dim_test_table"
        self.assertEqual(tm.get_long_table_name(table_name, product_name, table_type), expected_name)
        table_type = "view"
        expected_name = "product_mv_test_table"
        self.assertEqual(tm.get_long_table_name(table_name, product_name, table_type), expected_name)

    def test_extract_table_name(self):
        table_name = "product_fct_test_table"
        table_type, product_name, short_table_name= tm.get_short_table_name(table_name)
        self.assertEqual(short_table_name, "test_table")
        self.assertEqual(table_type, "fct")
        self.assertEqual(product_name, "product")
        table_name = "src_product_test_table"
        table_type, product_name, short_table_name= tm.get_short_table_name(table_name)
        self.assertEqual(short_table_name, "test_table")
        self.assertEqual(table_type, "src")
        self.assertEqual(product_name, "product")
        table_name = "int_product_test_table"
        table_type, product_name, short_table_name= tm.get_short_table_name(table_name)
        self.assertEqual(short_table_name, "test_table")
        self.assertEqual(table_type, "int")
        self.assertEqual(product_name, "product")
        table_name = "product_dim_test_table"
        table_type, product_name, short_table_name= tm.get_short_table_name(table_name)
        self.assertEqual(short_table_name, "test_table")
        self.assertEqual(table_type, "dim")
        self.assertEqual(product_name, "product")
        table_name = "test_table"
        table_type, product_name, short_table_name= tm.get_short_table_name(table_name)
        self.assertEqual(short_table_name, "test_table")
        self.assertEqual(table_type, "")
        self.assertEqual(product_name, "")
        table_name = "product_mv_test_table"
        table_type, product_name, short_table_name= tm.get_short_table_name(table_name)
        self.assertEqual(short_table_name, "test_table")
        self.assertEqual(table_type, "mv")
        self.assertEqual(product_name, "product")



    def test_create_table_structure(self):
        try:
            tbf, tbn= tm.build_folder_structure_for_table("it2",os.getenv("STAGING") + "/intermediates", "p3")
            assert os.path.exists(tbf)
            assert os.path.exists(tbf + "/" + SCRIPTS_DIR)
            dirname=tbf + "/" + SCRIPTS_DIR + "/ddl.int_p3_it2.sql"
            assert os.path.exists(dirname)
            assert os.path.exists(tbf + "/" + SCRIPTS_DIR + "/dml.int_p3_it2.sql" )
            assert os.path.exists(tbf + "/Makefile")
            with open(dirname, "r") as f:
                content = f.read()
                assert "CREATE TABLE IF NOT EXISTS int_p3_it2" in content
            with open(tbf + "/Makefile", "r") as f:
                content = f.read()
                assert "TABLE_NAME=int_p3_it2" in content
        except Exception as e:
            print(e)
            self.fail()

    def test_update_make_file(self):
        try:
            tbf, tbn =tm.build_folder_structure_for_table("it2",os.getenv("PIPELINES") + "/intermediates", "p3")
            assert os.path.exists(tbf + "/Makefile")
            os.remove(tbf+ "/Makefile")
            assert not os.path.exists(tbf + "/Makefile")
            tm.get_or_build_inventory(os.getenv("PIPELINES"), os.getenv("PIPELINES"), True)
            tm.update_makefile_in_folder(os.getenv("PIPELINES"), "int_p3_it2")
            assert os.path.exists(tbf + "/Makefile")
            with open(tbf + "/Makefile", "r") as f:
                content = f.read()
                assert "TABLE_NAME=int_p3_it2" in content
        except Exception as e:
            print(e)
            self.fail()

    def test_update_all_makefiles_in_folder(self):
        try:
            folder_path = os.getenv("PIPELINES") + "/intermediates"
            count = tm.update_all_makefiles_in_folder(folder_path)
            assert count > 0
        except Exception as e:
            print(e)

    def test_search_users_of_table(self):
        try:
            users = tm.search_users_of_table("int_p1_table_1",os.getenv("PIPELINES"))
            assert users
            assert "fct_order" in users
            print(users)
        except Exception as e:
            print(e)
            self.fail()
       
 

    class MockTableWorker:
        def __init__(self, update_result, new_content):
            self.update_result = update_result
            self.new_content = new_content

        def update_sql_content(self, content, string_to_change_from, string_to_change_to):
            return self.update_result, self.new_content

    @patch('shift_left.core.table_mgr.load_sql_content_from_file')
    @patch('builtins.open', new_callable=unittest.mock.mock_open)
    def test_update_sql_content_success(self, mock_file, mock_load):
        """Test successful update of SQL content"""
        sql_file = "test.sql"
        mock_load.return_value = "SELECT * FROM test_table;"
        processor = self.MockTableWorker(True, "SELECT * FROM updated_table;")
        
        result = tm.update_sql_content_for_file(sql_file_name=sql_file, 
                                                processor=processor
                                                )
        
        self.assertTrue(result)
        mock_file.assert_called_once_with(sql_file, "w")
        mock_file().write.assert_called_once_with("SELECT * FROM updated_table;")

    @patch('shift_left.core.table_mgr.load_sql_content_from_file')
    @patch('builtins.open', new_callable=unittest.mock.mock_open)
    def test_update_sql_content_no_update_needed(self, mock_file, mock_load):
        """Test when no SQL content update is needed.
        
        This test verifies that
        1. The file content is loaded correctly
        2. The processor correctly identifies no update is needed
        3. The file is not written to when no update is needed
        4. The function returns False to indicate no update occurred
        """
        # Setup
        sql_file = "test.sql"
        original_content = "SELECT * FROM test_table;"
        mock_load.return_value = original_content
        processor = self.MockTableWorker(False, original_content)
        
        # Execute
        result = tm.update_sql_content_for_file(
            sql_file_name=sql_file,
            processor=processor
        )
        
        # Verify
        mock_load.assert_called_once_with(sql_file)
        mock_file.assert_not_called()  # File should not be opened for writing
        self.assertFalse(result, "Should return False when no update is needed")

    @patch('shift_left.core.table_mgr.load_sql_content_from_file', side_effect=IOError("File not found"))
    def test_update_sql_content_file_error(self, mock_load):
        """Test error handling when file operations fail"""
        sql_file = "nonexistent.sql"
        processor = self.MockTableWorker(True, "SELECT * FROM updated_table;")
        
        with self.assertRaises(IOError):
            tm.update_sql_content_for_file(sql_file, processor)

if __name__ == '__main__':
    unittest.main()