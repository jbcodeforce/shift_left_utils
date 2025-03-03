from textwrap import indent
import unittest
import os
import json 
from pathlib import Path
import shift_left.core.pipeline_mgr as pm

class TestPipelineManager(unittest.TestCase):
    
    def test_create_inventory(self):
        try:
            os.environ["PIPELINES"] = "../../examples/flink-project/pipelines"
            inventory = pm.build_inventory( os.getenv("PIPELINES"))
            assert inventory
            assert len(inventory) > 0
            print(inventory)
        except Exception as e:
            print(e)
            self.fail()
       
    def test_absolute_to_relative(self):
        path= "/home/jerome/Code/shift_left_utils/examples/flink-project/pipelines"
        assert pm.PIPELINE_FOLDER_NAME == pm.from_absolute_to_pipeline(path)
    
    def test_relative_to_pipeline(self):
        os.environ["PIPELINES"] = "../../examples/flink-project/pipelines"
        test_path = "pipelines/facts/p1/fct_order"
        assert "../../examples/flink-project/pipelines/facts/p1/fct_order"  == pm.from_pipeline_to_absolute(test_path)

    def test_build_a_src_pipelinedef(self):
        print("test_build_a_src_pipelinedef")
        os.environ["PIPELINES"] = "../../examples/flink-project/pipelines"
        path= os.getenv("PIPELINES")
        src_table_path=path + "/sources/src_table_1/sql-scripts/dml.src_table_1.sql"
        result = pm.build_pipeline_definition_from_table(src_table_path, path)
        assert result
        print(result.model_dump_json(indent=3))

    def test_build_a_int_pipeline_def(self):
        print("test_build_a_int_pipeline_def")
        os.environ["PIPELINES"] = "../../examples/flink-project/pipelines"
        path= os.getenv("PIPELINES")
        table_path=path + "/intermediates/p1/int_table_1/sql-scripts/dml.int_table_1.sql"
        result = pm.build_pipeline_definition_from_table(table_path, path)
        assert result
        print(result.model_dump_json(indent=3))

    def test_build_pipeline_def_for_fact_table(self):
        print("test_build_pipeline_def_for_fact_table")
        os.environ["PIPELINES"] = "../../examples/flink-project/pipelines"
        path= os.getenv("PIPELINES")
        table_path=path + "/facts/p1/fct_order/sql-scripts/dml.fct_order.sql"
        result = pm.build_pipeline_definition_from_table(table_path, path)
        assert result
        print(result.model_dump_json(indent=3))

    def test_walk_the_hierarchy_for_report_from_table(self):
        print("test_walk_the_hierarchy_for_report_from_table")
        os.environ["PIPELINES"] = "../../examples/flink-project/pipelines"
        result = pm.walk_the_hierarchy_for_report_from_table("int_table_1")
        assert result
        print(json.dumps(result, indent=3))
    
if __name__ == '__main__':
    unittest.main()