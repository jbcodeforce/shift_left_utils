import unittest
import pathlib
import os
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
      
import shift_left.core.table_mgr as tm
from shift_left.core.utils.table_worker import TableWorker
from shift_left.core.utils.file_search import list_src_sql_files

class TestTableManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        #os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent /  "config.yaml")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))

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
            assert users
            print(users)
        except Exception as e:
            print(e)
            self.fail()

    
    def test_update_dml_statement(self):
        table_name = "int_table_1"
        print("Test update dml sql content")
        sql_in="""insert into t3 select id,b,c from t2;"""
        class TestUpdate(TableWorker):
            def update_sql_content(sql_in : str):
                return sql_in.replace("from t2", "from t2 join t3 on t3.id = t2.id")
        sql_out=tm.update_sql_content_for_file(sql_in, TestUpdate)
        assert "from t2 join t3 on t3.id" in sql_out
        print(sql_out)
    
    def test_get_ddl_dml_references(self):
        files = list_src_sql_files(os.getenv("PIPELINES")+ "/facts/p1/fct_order")
        assert files["ddl.fct_order"]
        assert files["dml.fct_order"]
        assert ".sql" in files["dml.fct_order"]
        print(files)

 
    


if __name__ == '__main__':
    unittest.main()