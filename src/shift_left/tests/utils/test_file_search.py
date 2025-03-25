import unittest
import os
from shift_left.core.utils.file_search import (
    get_or_build_source_file_inventory, 
    build_inventory,
    get_ddl_dml_from_folder, 
    from_pipeline_to_absolute,
    from_absolute_to_pipeline,
    SCRIPTS_DIR,
    FlinkTableReference)
import json
import pathlib

class TestFileSearch(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")

    def test_table_ref_equality(self):
        ref1 = FlinkTableReference.model_validate({"table_name": "table1", "type": "fact", "dml_ref": "dml1", "ddl_ref": "ddl1", "table_folder_name": "folder1" })
        ref2 = FlinkTableReference.model_validate({"table_name": "table1", "type": "fact", "dml_ref": "dml1", "ddl_ref": "ddl1", "table_folder_name": "folder1" })
        self.assertEqual(ref1, ref2)
        self.assertEqual(ref1.__hash__(), ref2.__hash__())

    def test_table_ref_inequality(self):
        ref1 = FlinkTableReference.model_validate({"table_name": "table1", "type": "fact", "dml_ref": "dml1", "ddl_ref": "ddl1", "table_folder_name": "folder1" })
        ref2 = FlinkTableReference.model_validate({"table_name": "table2", "type": "fact", "dml_ref": "dml2", "ddl_ref": "ddl2", "table_folder_name": "folder2" })
        self.assertNotEqual(ref1, ref2)
        self.assertNotEqual(ref1.__hash__(), ref2.__hash__())  

    def test_path_transformation(self):
      path = "/user/bill/project/pipelines/dataproduct/sources/sql-scripts/ddl.table.sql"
      assert "pipelines/dataproduct/sources/sql-scripts/ddl.table.sql" == from_absolute_to_pipeline(path)
      path = "pipelines/dataproduct/sources/sql-scripts/ddl.table.sql"
      assert "pipelines/dataproduct/sources/sql-scripts/ddl.table.sql" == from_absolute_to_pipeline(path)

    def test_absolute_to_relative(self):
        path= "/home/bill/Code/shift_left_utils/examples/flink-project/pipelines"
        assert "pipelines" == from_absolute_to_pipeline(path)
    
    def test_relative_to_pipeline(self):
        test_path = "pipelines/facts/p1/fct_order"
        assert "flink-project/pipelines/facts/p1/fct_order"  in from_pipeline_to_absolute(test_path)


    def test_build_src_inventory(self):
        """ given a source project, build the inventory of all the sql files """
        inventory_path= os.getenv("SRC_FOLDER")
        all_files= get_or_build_source_file_inventory(inventory_path)
        self.assertIsNotNone(all_files)
        self.assertGreater(len(all_files), 0)
        print(json.dumps(all_files, indent=3))

    def test_validate_ddl_dml_file_retreived(self):
        inventory_path= os.getenv("PIPELINES")
        ddl, dml = get_ddl_dml_from_folder(inventory_path + "/facts/p1/fct_order", SCRIPTS_DIR)
        self.assertIsNotNone(ddl)
        self.assertIsNotNone(dml)
        print(ddl)


    def test_build_flink_sql_inventory(self):
        """ given a source project, build the inventory of all the sql files """
        inventory_path= os.getenv("PIPELINES")
        all_files= build_inventory(inventory_path)
        self.assertIsNotNone(all_files)
        self.assertGreater(len(all_files), 0)
        print(json.dumps(all_files, indent=3))
        print(all_files["src_table_1"])




    
if __name__ == '__main__':
    unittest.main()