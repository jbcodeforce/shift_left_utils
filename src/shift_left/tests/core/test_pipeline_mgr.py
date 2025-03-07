
import unittest
import os
import json 
import pathlib
import shift_left.core.pipeline_mgr as pm
import shift_left.core.table_mgr as tm


class TestPipelineManager(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "src-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent /  "config.yaml")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))

       
  
    def _test_build_a_src_pipelinedef(self):
        print("test_build_a_src_pipelinedef")
        path= os.getenv("PIPELINES")
        src_table_path=path + "/sources/src_table_1/sql-scripts/dml.src_table_1.sql"
        result = pm.build_pipeline_definition_from_table(src_table_path, path)
        assert result
        print(result.model_dump_json(indent=3))

    def _test_build_a_int_pipeline_def(self):
        print("test_build_a_int_pipeline_def")
        path= os.getenv("PIPELINES")
        table_path=path + "/intermediates/p1/int_table_1/sql-scripts/dml.int_table_1.sql"
        result = pm.build_pipeline_definition_from_table(table_path, path)
        assert result
        print(result.model_dump_json(indent=3))

    def _test_build_pipeline_def_for_fact_table(self):
        print("test_build_pipeline_def_for_fact_table")
        path= os.getenv("PIPELINES")
        table_path=path + "/facts/p1/fct_order/sql-scripts/dml.fct_order.sql"
        result = pm.build_pipeline_definition_from_table(table_path, path)
        assert result
        print(result.model_dump_json(indent=3))

    def test_walk_the_hierarchy_for_report_from_table(self):
        print("test_walk_the_hierarchy_for_report_from_table")
        result = pm.walk_the_hierarchy_for_report_from_table("int_table_1", os.getenv("PIPELINES"))
        assert result
        print(result.model_dump_json(indent=3))
    
if __name__ == '__main__':
    unittest.main()