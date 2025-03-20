import unittest
import pathlib
import os

os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")


from shift_left.core.test_mgr import load_test_definition, SLTestSuite, SLTestCase, SLTestData

class TestTestManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data" 
    
    def test_definition(self):
        td1 = SLTestData(table_name= "tb1", sql_file_name="ftb1")
        tc1 = SLTestCase(name="tc1", inputs=[td1])
        ts = SLTestSuite(testcases=[tc1])
        print(ts)

    def test_load_test_definition(self):
        table_name= str(pathlib.Path(__file__).parent /  "../data/flink-project/pipelines/sources/p1/src_table_2")
        test_definitions = load_test_definition(table_name)
        assert test_definitions
        print(test_definitions)

