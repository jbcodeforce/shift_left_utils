import unittest

from shift_left.core.test_mgr import SLTestDefinition, Foundation, SLTestCase, SLTestData
from shift_left.core.utils.ut_ai_data_tuning import AIBasedDataTuning
import pathlib

class TestUtaiDataTuning(unittest.TestCase):
    """
    Test the AIBasedDataTuning class.
    """

    def _test_generate_data(self):
        """
        Test the generate_data method.
        """
        dml_content = """
        SELECT * FROM my_table;
        """
        test_definition = SLTestDefinition(
            foundations=[],
            test_suite=[]
        )
        data = AIBasedDataTuning().update_synthetic_data(dml_content, test_definition)
        self.assertIsNotNone(data)

    def test_modify_data_to_match_column_types(self):
        """
        Test the modify_data_to_match_column_types method.
        """
        foundation_1 = Foundation(table_name="table_1", ddl_for_test="./tests/ddl_table_1.sql")
        input_data_1 = SLTestData(table_name="table_1", file_name="./tests/insert_table_1.sql")
        test_case_1 = SLTestCase(
            name="test_case_1",
            inputs=[input_data_1],
            outputs=[]
        )
        test_definition = SLTestDefinition(
            foundations=[foundation_1],
            test_suite=[test_case_1]
        )
        path_to_table =  str(pathlib.Path(__file__).parent)
        for foundation in test_definition.foundations:
            for test_case in test_definition.test_suite:
                for input_data in test_case.inputs:
                    if input_data.table_name == foundation.table_name:
                        result = AIBasedDataTuning()._modify_data_to_match_column_types(path_to_table, foundation, input_data)
                        self.assertIsNotNone(result)

if __name__ == "__main__":
    unittest.main() 