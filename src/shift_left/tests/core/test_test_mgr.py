import unittest
import pathlib
import os

#os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent / "../../data/flink-project/pipelines")


from shift_left.core.test_mgr import * #load_test_definition, SLTestSuite, SLTestCase, SLTestData, Foundation

class TestTestManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data" 
    
    def test_definition(self):
        td1 = SLTestData(table_name= "tb1", sql_file_name="ftb1")
        o1 = SLTestData(table_name= "tbo1", sql_file_name="to1")
        tc1 = SLTestCase(name="tc1", inputs=[td1], outputs=[o1])
        fds = [Foundation(table_name="tb1", ddl_for_test="ddl-tb1")]
        ts = SLTestSuite(foundations=fds, test_suite=[tc1])
 #       print(ts)
 #       print(ts.model_dump_json())

    def test_load_test_definition(self):
        table_name= str(pathlib.Path(__file__).parent /  "../data/flink-project/pipelines/sources/p1/src_table_2")
        test_definitions = load_test_definition(table_name)
        assert test_definitions
 #       print(test_definitions)

    def test_load_test_definition_for_fact_table(self):
        table_name= str(pathlib.Path(__file__).parent /  "../data/flink-project/pipelines/facts/p1/fct_order")
        test_definitions = load_test_definition(table_name)
        assert test_definitions
 #       print(test_definitions)

    def test_execute_one_test(self):
        table_folder = str(pathlib.Path(__file__).parent /"../data/flink-project/pipelines/sources/p1/src_table_2")
        compute_pool_id = "compute_pool_id"
        test_case_name = "test_case_1"

        result = execute_one_test(table_folder, test_case_name)
        assert result, f"Test case '{test_case_name}' executed successfully"

        