import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib


import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from shift_left.core.flink_statement_model import Statement
from shift_left.core.statement_mgr import (
    delete_statement_if_exists
)

class TestTableManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent.parent / "data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent /  "config.yaml")
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

if __name__ == '__main__':
    unittest.main()