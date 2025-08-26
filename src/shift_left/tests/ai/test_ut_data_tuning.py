import unittest

from shift_left.core.models.flink_test_model import SLTestDefinition, SLTestCase, SLTestData, Foundation
from shift_left.core.utils.ut_ai_data_tuning import AIBasedDataTuning
import pathlib
import os
from pydantic import BaseModel



class TestUtaiDataTuning(unittest.TestCase):
    """
    Test the AIBasedDataTuning class.
    """

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        os.environ["CONFIG_FILE"] =  str(data_dir / "config-ccloud.yaml")
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")

    def test_generate_data(self):
        """
        Test the generate_data method.
        """
        dml_content = """
       INSERT INTO p1_fct_order
        with cte_table as (
            SELECT
            order_id,
            product_id ,
            customer_id ,
            amount
            FROM int_p1_table_2
        )
        SELECT  
            coalesce(c.id,'N/A') as id,
            c.user_name,
            c.account_name,
            c.balance - ct.amount as balance
        from cte_table ct
        left join int_p1_table_1 c on ct.customer_id = c.id;
        """
        ddl_content = """
        CREATE TABLE IF NOT EXISTS  p1_fct_order(
            id STRING NOT NULL,
            customer_name STRING,
            account_name STRING,
            balance int,
            PRIMARY KEY(id) NOT ENFORCED
        ) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS
        """
        base_table_path = os.environ["PIPELINES"] +  "/facts/p1/fct_order"
        int_table_1 = SLTestData(table_name="int_table_1", file_name=base_table_path + "/tests/insert_int_table_1_1.sql")
        int_table_2 = SLTestData(table_name="int_table_2", file_name=base_table_path + "/tests/insert_int_table_2_1.sql")
        test_case = SLTestCase(
            name="test_case_1",
            inputs=[int_table_1, int_table_2],
            outputs=[]
        )
        foundation_1 = Foundation(table_name="int_table_1", ddl_for_test=base_table_path + "/tests/ddl_int_table_1.sql")
        foundation_2 = Foundation(table_name="int_table_2", ddl_for_test=base_table_path + "/tests/ddl_int_table_2.sql")
        test_suite = SLTestDefinition(
            foundations=[foundation_1, foundation_2],
            test_suite=[test_case]
        )
        data = AIBasedDataTuning().update_synthetic_data(dml_content, test_suite, "test_case_1")
        self.assertIsNotNone(data)
        for output in data:
            print(output.table_name)
            print(output.output_sql_content)


    def _test_modify_data_to_match_column_types(self):
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