"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
from pathlib import Path
import shutil
import shift_left.core.project_manager as pm
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
from shift_left.core.utils.app_config import get_config
import shift_left.core.pipeline_mgr as pipemgr
import shift_left.core.table_mgr as tm

class TestProjectManager(unittest.TestCase):
    data_dir = ""

    @classmethod
    def setUpClass(cls):
        cls.data_dir = str(Path(__file__).parent / "../tmp")  # Path to the tmp directory

    def test_create_data_product_project(self):
        try:
            pm.build_project_structure("test_data_project", self.data_dir, pm.DATA_PRODUCT_PROJECT_TYPE)
            assert os.path.exists(os.path.join(self.data_dir, "test_data_project"))
            assert os.path.exists(os.path.join(self.data_dir, "test_data_project/pipelines"))
            shutil.rmtree(self.data_dir)
        except Exception as e:
            self.fail()
       
    def test_1_create_data_kimball_project(self):
        try:
            pm.build_project_structure("test_data_kimball_project",self.data_dir, pm.KIMBALL_PROJECT_TYPE)
            assert os.path.exists(os.path.join( self.data_dir, "test_data_kimball_project"))
            assert os.path.exists(os.path.join( self.data_dir, "test_data_kimball_project/pipelines"))
            assert os.path.exists(os.path.join( self.data_dir, "test_data_kimball_project/pipelines/intermediates"))
            shutil.rmtree(self.data_dir)
        except Exception as e:
            self.fail()

    def test_report_table_cross_products(self):
        print("test_report_table_cross_products: list the tables used in other products")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
        pipemgr.delete_all_metada_files(os.getenv("PIPELINES"))
        pipemgr.build_all_pipeline_definitions( os.getenv("PIPELINES"))
        result = pm.report_table_cross_products(os.getenv("PIPELINES"))
        assert result
        assert len(result) == 2
        assert "src_table_1" in result
        assert "src_common_tenant" in result
        
if __name__ == '__main__':
    unittest.main()