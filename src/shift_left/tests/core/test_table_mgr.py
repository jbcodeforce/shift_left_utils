import unittest
import os
from pathlib import Path
import shift_left.core.table_mgr as tm
from shift_left.core.utils.file_search import SCRIPTS_DIR
class TestTableManager(unittest.TestCase):

    def test_create_table_structure(self):
        os.environ["CONFIG_FILE"] = "./tests/config.yaml"
        try:
            tbf, tbn =tm.build_folder_structure_for_table("it2","./tests/data/flink-project/staging/intermediates")
            assert os.path.exists(tbf)
            assert os.path.exists(tbf + "/" + tm.SCRIPTS_DIR)
            assert os.path.exists(tbf + "/Makefile")
        except Exception as e:
            print(e)
            self.fail()
       
    def test_search_source_dependencies_for_dbt_table(self):
        os.environ["CONFIG_FILE"] = "./tests/config.yaml"
        try:
            dependencies = tm.search_source_dependencies_for_dbt_table("./tests/data/src-project/intermediates/it1.sql","./tests/data/src-project")
            assert len(dependencies) == 2
            print(dependencies)
        except Exception as e:
            print(e)
            self.fail()
    
    def test_build_update_makefile(self):
        os.environ["CONFIG_FILE"] = "./tests/config.yaml"
        try:
            tm.build_update_makefile("./tests/data/flink-project/pipelines","int_table_1")
        except Exception as e:
            print(e)
            self.fail()

    def test_search_users_of_table(self):
        os.environ["CONFIG_FILE"] = "./tests/config.yaml"
        os.environ["PIPELINES"] = "./tests/data/flink-project/pipelines"

        try:
            users = tm.search_users_of_table("int_table_1","./tests/data/flink-project/pipelines")
            print(users)
        except Exception as e:
            print(e)
            self.fail()

if __name__ == '__main__':
    unittest.main()