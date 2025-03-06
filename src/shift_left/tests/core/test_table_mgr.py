import unittest
import os
import pathlib
import shift_left.core.table_mgr as tm

class TestTableManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "src-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent /  "config.yaml")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))

    def test_create_table_structure(self):
   
        try:
            tbf, tbn =tm.build_folder_structure_for_table("it2",os.getenv("STAGING") + "/intermediates")
            assert os.path.exists(tbf)
            assert os.path.exists(tbf + "/" + tm.SCRIPTS_DIR)
            assert os.path.exists(tbf + "/Makefile")
        except Exception as e:
            print(e)
            self.fail()
       
    def test_search_source_dependencies_for_dbt_table(self):
        try:
            dependencies = tm.search_source_dependencies_for_dbt_table( os.getenv("SRC_FOLDER")  + "/intermediates/it1.sql", os.getenv("SRC_FOLDER"))
            assert len(dependencies) == 2
            print(dependencies)
        except Exception as e:
            print(e)
            self.fail()
    
    def test_build_update_makefile(self):
        try:
            tm.build_update_makefile(os.getenv("PIPELINES"), "int_table_1")
        except Exception as e:
            print(e)
            self.fail()

    def test_search_users_of_table(self):
        try:
            users = tm.search_users_of_table("int_table_1",os.getenv("PIPELINES"))
            print(users)
        except Exception as e:
            print(e)
            self.fail()

    


if __name__ == '__main__':
    unittest.main()