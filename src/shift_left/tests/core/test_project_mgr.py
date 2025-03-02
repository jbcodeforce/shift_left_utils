import unittest
import os
from pathlib import Path
import shift_left.core.project_manager as pm

class TestPojectManager(unittest.TestCase):

    def test_create_project(self):
        try:
            pm.build_project_structure("test_data_project","./tmp", pm.DATA_PRODUCT_PROJECT_TYPE)
            assert os.path.exists("./tmp/test_data_project")
            assert os.path.exists("./tmp/test_data_project/pipelines")
        except Exception as e:
            unittest.fail()
       


if __name__ == '__main__':
    unittest.main()