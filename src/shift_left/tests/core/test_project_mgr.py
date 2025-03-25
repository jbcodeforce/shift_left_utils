import unittest
import os
from pathlib import Path
import shift_left.core.project_manager as pm

class TestPojectManager(unittest.TestCase):

    def _test_create_data_product_project(self):
        try:
            pm.build_project_structure("test_data_project","./tmp", pm.DATA_PRODUCT_PROJECT_TYPE)
            assert os.path.exists(os.path.join("./tmp", "test_data_project"))
            assert os.path.exists(os.path.join("./tmp", "test_data_project/pipelines"))
        except Exception as e:
            self.fail()
       
    def test_create_data_kimball_project(self):
        try:
            pm.build_project_structure("test_data_kimball_project","./tmp", pm.KIMBALL_PROJECT_TYPE)
            assert os.path.exists(os.path.join("./tmp", "test_data_kimball_project"))
            assert os.path.exists(os.path.join("./tmp", "test_data_kimball_project/pipelines"))
            assert os.path.exists(os.path.join("./tmp", "test_data_kimball_project/pipelines/intermediates"))
        except Exception as e:
            self.fail()

    def test_list_topic(self):
        pm.get_topic_list("./tmp/test_data_kimball_project/topics.txt")

    def test_list_of_compute_pools(self):
        env_id="env-nknqp3"
        pm.get_list_of_compute_pool(env_id)
        
if __name__ == '__main__':
    unittest.main()