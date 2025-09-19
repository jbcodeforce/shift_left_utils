"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
from pathlib import Path
import shutil
import shift_left.core.project_manager as pm
from shift_left.core.utils.app_config import get_config

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
        result = pm.report_table_cross_products(self.data_dir)
        assert result
        assert len(result) == 0
        
if __name__ == '__main__':
    unittest.main()