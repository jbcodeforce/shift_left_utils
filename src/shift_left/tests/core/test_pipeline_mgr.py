import unittest
import os
from pathlib import Path
import shift_left.core.pipeline_mgr as pm

class TestPipelineManager(unittest.TestCase):
    
    def test_create_inventory(self):
        try:
            inventory = pm.build_inventory( os.getenv("PIPELINES"))
            assert inventory
            assert len(inventory) > 0
        except Exception as e:
            print(e)
            self.fail()
       
    def test_absolute_to_relative(self):
        path= os.getenv("PIPELINES")
        assert pm.PIPELINE_FOLDER_NAME == pm.from_absolute_to_pipeline(path)
    
    def test_relative_to_pipeline(self):
        path= os.getenv("PIPELINES")
        test_path = "pipelines/facts/p1"
        parent_to_pipeline = os.path.dirname(path)
        assert parent_to_pipeline + "/" + test_path  == pm.from_pipeline_to_absolute(test_path)

    def test_build_a_src_pipedef(self):
        path= os.getenv("PIPELINES")
        src_table_path=path + "/sources/src_table_1/sql-scripts/dml.src_table_1.sql"
        result = pm.build_pipeline_definition_from_table(src_table_path, path)
        assert result
        print(result)

    def test_build_a_int_pipedef(self):
        path= os.getenv("PIPELINES")
        table_path=path + "/intermediates/p1/int_table_1/sql-scripts/dml.int_table_1.sql"
        result = pm.build_pipeline_definition_from_table(table_path, path)
        assert result
        print(result)
    
if __name__ == '__main__':
    unittest.main()