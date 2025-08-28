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
        int_table_1 = SLTestData(table_name="int_table_1", file_name= "./tests/insert_int_table_1_1.sql")
        int_table_2 = SLTestData(table_name="int_table_2", file_name= "./tests/insert_int_table_2_1.sql")
        output_data = SLTestData(table_name="p1_fct_order", file_name= "./tests/validate_fct_order_1.sql")
        test_case = SLTestCase(
            name="test_case_1",
            inputs=[int_table_1, int_table_2],
            outputs=[output_data]
        )
        foundation_1 = Foundation(table_name="int_table_1", ddl_for_test= "./tests/ddl_int_table_1.sql")
        foundation_2 = Foundation(table_name="int_table_2", ddl_for_test= "./tests/ddl_int_table_2.sql")
        test_suite = SLTestDefinition(
            foundations=[foundation_1, foundation_2],
            test_suite=[test_case]
        )
        data = AIBasedDataTuning().enhance_test_data(base_table_path, dml_content, test_suite, "test_case_1")
        self.assertIsNotNone(data)
        for output in data:
            print(output.table_name)
            print(output.output_sql_content)
            print(output.file_name)




if __name__ == "__main__":
    unittest.main() 