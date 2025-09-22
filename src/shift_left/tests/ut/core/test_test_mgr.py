"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
import pathlib
import unittest
from unittest.mock import patch, ANY

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

from shift_left.core.models.flink_statement_model import (
    Statement, 
    StatementResult, 
    Data, 
    OpRow, 
    StatementInfo)
import shift_left.core.test_mgr as test_mgr
from shift_left.core.utils.file_search import build_inventory
from shift_left.core.utils.app_config import reset_all_caches
from shift_left.core.test_mgr import (
    SLTestDefinition,
    SLTestCase,
    SLTestData,
    Foundation
)
from shift_left.core.utils.file_search import FlinkTableReference, get_or_build_inventory


class TestTestManager(unittest.TestCase):
    """Unit test suite for test manager functionality."""
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent.parent / "data"
        reset_all_caches() # Reset all caches to ensure test isolation
        build_inventory(os.getenv("PIPELINES"))
    
    def setUp(self):
        # Ensure proper test isolation by resetting caches and rebuilding inventory
        reset_all_caches()
        build_inventory(os.getenv("PIPELINES"))
        self._ddls_executed  = {'int_table_1_ut': False, 'int_table_2_ut': False, 'p1_fct_order_ut': False}
    
    # ---- Mock functions to be used in tests to avoid calling remote services ----
    def _mock_table_exists(self, table_name):
        """
        Mock the _table_exists(table_name) function to return True if the table name is in the _ddls_executed dictionary
        """
        print(f"mock_table_exists: {table_name} returns {self._ddls_executed[table_name]}")
        value = self._ddls_executed[table_name]
        self._ddls_executed[table_name] = True  # mock the table will exist after the tet execution
        return value

    def _mock_get_None_statement(self, statement_name):
        print(f"mock_get_statement: {statement_name} returns None")  
        return None
    
    # --------- tests creation and preparation ---------
    def test_create_tests_structure(self):
        """Test creation of tests structure with templates & test definitions.
        The table e uses c so c will be part of foundations SQL and CSV inputs are created.
        """
        # Clean up any existing test files
        test_folder = os.path.join(os.getenv("PIPELINES"), "facts/p2/e/tests")
        if os.path.exists(test_folder):
            for file in os.listdir(test_folder):
                os.remove(os.path.join(test_folder, file))
            os.rmdir(test_folder)
        table_name = "e"
        test_mgr.init_unit_test_for_table(table_name, create_csv=True)
        
        self.assertTrue(os.path.exists(os.getenv("PIPELINES") + "/facts/p2/e/tests"))
        self.assertTrue(os.path.exists(os.getenv("PIPELINES") + "/facts/p2/e/tests/test_definitions.yaml"))
        self.assertTrue(os.path.exists(os.getenv("PIPELINES") + "/facts/p2/e/tests/validate_e_2.sql"))
        self.assertTrue(os.path.exists(os.getenv("PIPELINES") + "/facts/p2/e/tests/insert_c_2.csv"))
        self.assertTrue(os.path.exists(os.getenv("PIPELINES") + "/facts/p2/e/tests/insert_c_1.sql"))
        test_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        self.assertTrue(test_def)
        print(test_def.model_dump_json(indent=3))
        self.assertEqual(test_def.foundations[0].table_name, "c")
        self.assertEqual(test_def.foundations[0].ddl_for_test, "./tests/ddl_c.sql")

    def test_validate_test_model(self):
        """Test loading of test definition."""
        td1 = SLTestData(table_name="tb1", file_name="ftb1")
        o1 = SLTestData(table_name="tbo1", file_name="to1")
        tc1 = SLTestCase(name="tc1", inputs=[td1], outputs=[o1])
        fds = [Foundation(table_name="tb1", ddl_for_test="ddl-tb1")]
        ts = SLTestDefinition(foundations=fds, test_suite=[tc1])
        self.assertTrue(ts)
        print(ts.model_dump_json(indent=3))

    def test_load_test_definition_for_fact_table(self):
        """Test loading test definition for fact table."""
        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        
        self.assertTrue(test_suite_def)
        print(test_suite_def.model_dump_json(indent=3))
        
        self.assertEqual(len(test_suite_def.test_suite), 2)
        self.assertEqual(len(test_suite_def.foundations), 2)
        self.assertEqual(test_suite_def.test_suite[0].name, "test_case_1")
        self.assertEqual(test_suite_def.test_suite[1].name, "test_case_2")
        self.assertEqual(test_suite_def.foundations[0].table_name, "int_table_1")
        self.assertEqual(test_suite_def.foundations[1].table_name, "int_table_2")
       
    def test_statement_with_quoted_table_name(self):
        table_name = "int_p3_user_role"
        test_definition, table_ref = test_mgr._load_test_suite_definition(table_name)
        tests_folder_path = os.path.join(os.getenv("PIPELINES"), "intermediates", "p3", "user_role", "tests")
        table_inventory = build_inventory(os.getenv("PIPELINES"))
        table_struct = test_mgr._process_foundation_ddl_from_test_definitions(test_definition, 
                                                               tests_folder_path, 
                                                               table_inventory)
        assert table_struct
        cnames = {}
        table_rows = {}
        for table in table_struct:
            cname, rows= test_mgr._build_data_sample(table_struct[table])
            cnames[table]=cname
            table_rows[table] = rows
        assert "`id`, `name`, `description`, `created_at`" in cnames["src_p3_tenants"]
        assert "`user_id`, `tenant_id`, `role_id`, `status`" in cnames["src_p3_users"]
        assert "`role_id`, `role_name`" in cnames["src_p3_roles"]
        assert "('id_1', 'name_1', 'description_1', TIMESTAMP '2021-01-01 00:00:00'" in table_rows["src_p3_tenants"]
        assert "('user_id_2', 'tenant_id_2', 'role_id_2', 'status_2')" in table_rows["src_p3_users"]
        assert "('role_id_1', 'role_name_1')" in table_rows["src_p3_roles"]
        
    def test_read_csv_file_to_sql(self):
        print("test_read_csv_file_to_sqlc to validate csv content is transformed into SQL insert into.")
        pipeline_folder = os.getenv("PIPELINES")
        fname = pipeline_folder + "/intermediates/p3/user_role/tests/insert_src_p3_tenants_2.csv"
        headers, rows = test_mgr._read_csv_file(fname)
        assert headers == "id, name, description, created_at"
        assert len(rows) == 5
        sql = test_mgr._transform_csv_to_sql("src_p3_tenants_ut", headers, rows)
        assert sql.startswith("insert into src_p3_tenants_ut (id, name, description, created_at) values")
        print(sql)

    # ---------- test execution -------------

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_run_statement_under_test_happy_path(self, 
                                     mock_post_flink_statement, 
                                     mock_get_statement_info,
                                     mock_table_exists,
                                     mock_get_statement):
        """Test should create ddl and dml statements as the table under tests does not exist
        """
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            if "ddl" in statement_name:
                return Statement(name=statement_name, status={"phase": "COMPLETED"})
            else:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
        
        def _mock_statement_info(statement_name):
            """
            Mock the statement_mgr.get_statement_info(statement_name) function to return None
            to enforce execution of the statement
            """
            print(f"mock_statement_info: {statement_name}")
            return None

        mock_get_statement_info.side_effect = _mock_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = self._mock_table_exists
        mock_get_statement.side_effect = self._mock_get_None_statement

        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_suite_def is not None
        statements = test_mgr._start_ddl_dml_for_flink_under_test(table_name, table_ref)
        
        self.assertEqual(len(statements), 2)
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")
        assert "dev-ddl-p1-fct-order-ut" in statements[0].name
        assert "dev-dml-p1-fct-order-ut" in statements[1].name




    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_table_exists_run_dml_only(self, 
                                     mock_post_flink_statement, 
                                     mock_get_statement_info,
                                     mock_table_exists,
                                     mock_get_statement):
        """Test starting the statement under test: should not run ddl as table exists 
        but dml statements as statement is unknown
        """
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            if "dml" in statement_name:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
            else:
                return Statement(name=statement_name, status={"phase": "UNKNOWN"})

        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            # return no statement found so tool can run it
            return None

        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name}")
            return True
        

        mock_get_statement_info.side_effect = _mock_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = self._mock_get_None_statement

        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_suite_def is not None
        statements = test_mgr._start_ddl_dml_for_flink_under_test(table_name, table_ref)
        
        self.assertEqual(len(statements), 1)
        for statement in statements:
            assert isinstance(statement, Statement)
            print(f"statement: {statement.name} {statement.status}")
        assert "dev-dml-p1-fct-order-ut" in statements[0].name


    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_do_not_run_statements_if_table_exists_and_dml_running(self, 
                                     mock_post_flink_statement, 
                                     mock_get_statement_info,
                                     mock_table_exists,
                                     mock_get_statement):
        """Table exists so no DDL execution, DLM already RUNNING so not restart it
        """
        self._sql_content = ""
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            self._sql_content = sql_content
            if "dml" in statement_name:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
            else:
                return Statement(name=statement_name, status={"phase": "UNKNOWN"})

        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            return StatementInfo(name=statement_name, status_phase="RUNNING", sql_content=self._sql_content)

        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name}")
            return True
        
        def _mock_get_statement(statement_name):
            print(f"mock_get_statement: {statement_name}")  
            return Statement(name=statement_name, status={"phase": "RUNNING"})

        mock_get_statement_info.side_effect = _mock_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = _mock_get_statement

        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_suite_def is not None
        statements = test_mgr._start_ddl_dml_for_flink_under_test(table_name, table_ref)
        self.assertEqual(len(statements), 0)

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_foundations(self, 
                              mock_post_flink_statement, 
                              mock_get_statement_info, 
                              mock_table_exists,
                              mock_get_statement):
        """Test execution of sql unit test foundations for the fact table that uses 2 input tables."""
        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            if statement_name.startswith("dev-p1-ddl-int-table-1"):
                return None
            return StatementInfo(name=statement_name, status_phase="UNKNOWN", sql_content=self._sql_content)
        
        self._sql_content = ""
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            self._sql_content = sql_content
            if "ddl" in statement_name:
                return Statement(name=statement_name, status={"phase": "COMPLETED"})
            else:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
        
        mock_get_statement_info.side_effect = _mock_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = self._mock_table_exists
        mock_get_statement.side_effect = self._mock_get_None_statement

        table_name = "p1_fct_order"
        test_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        self.assertEqual(test_def.foundations[0].table_name, "int_table_1")
        statements = test_mgr._execute_foundation_statements(test_def, table_ref)
        self.assertEqual(len(statements), 2)
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")
        assert "dev-ddl-int-table-1-ut" in statements[0].name
        assert "dev-ddl-int-table-2-ut" in statements[1].name

    @patch('shift_left.core.test_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_one_testcase(self, 
                                mock_post_flink_statement, 
                                mock_get_statement_info, 
                                mock_get_statement_results,
                                mock_table_exists,
                                mock_get_statement,
                                mock_delete_statement):
        
        """Test the execution of one test case for p1_fct_order"""
        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            if statement_name.startswith("dev-p1-ddl-int-table-1"):
                return None
            return StatementInfo(name=statement_name, status_phase="UNKNOWN", sql_content=self._sql_content)
        
        self._sql_content = ""
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            self._sql_content = sql_content
            if "ddl" in statement_name or "-ins" in statement_name:
                return Statement(name=statement_name, status={"phase": "COMPLETED"})
            else:
                return Statement(name=statement_name, status={"phase": "RUNNING"})

        def _mock_statement_results(statement_name):
            print(f"mock_statement_results: {statement_name}")
            # Return PASS for validation statements related to test_case_1, FAIL for others
            op_row = OpRow(op=0, row=["PASS"]) if "val-1" in statement_name else OpRow(op=0, row=["FAIL"])
            data = Data(data=[op_row])
            result = StatementResult(results=data, 
                                     api_version="v1", 
                                     kind="StatementResult", 
                                     metadata=None)
            return result

        mock_get_statement_results.side_effect = _mock_statement_results
        mock_get_statement_info.side_effect = _mock_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = self._mock_table_exists
        mock_get_statement.side_effect = self._mock_get_None_statement
        mock_delete_statement.return_value = None  # Mock delete operation

        table_name = "p1_fct_order"
        test_suite_result = test_mgr.execute_one_or_all_tests(table_name, "test_case_1", run_validation=True)
        assert test_suite_result
        assert len(test_suite_result.test_results) == 1
        test_result = test_suite_result.test_results["test_case_1"]
        assert test_result
        self.assertEqual(len(test_result.statements), 3)
        self.assertEqual(len(test_suite_result.foundation_statements), 4)
        assert test_result.result == "PASS"
        for statement in test_result.statements:
            print(f"statement: {statement.name} {statement.status}")
        print(test_result.model_dump_json(indent=2))


    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_execute_csv_inputs(self, 
                                mock_post_flink_statement, 
                                mock_get_statement_info, 
                                mock_table_exists, 
                                mock_get_statement):
        
        self._sql_content = ""
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            self._sql_content = sql_content
            if "dml" in statement_name or "ins" in statement_name:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
            else:
                return Statement(name=statement_name, status={"phase": "UNKNOWN"})

        def _mock_running_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            return StatementInfo(name=statement_name, status_phase="RUNNING", sql_content=self._sql_content)

        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name}")
            return True
        
        def _mock_get_statement(statement_name):
            print(f"mock_get_statement: {statement_name}")
            if "ins" in statement_name:
                return None
            return Statement(name=statement_name, status={"phase": "RUNNING"})

        mock_get_statement_info.side_effect = _mock_running_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = _mock_get_statement

        table_name = "int_p3_user_role"
        test_case_name = "test_int_p3_user_role_2"
        compute_pool_id = "dev_pool_id"
        test_suite_def, table_ref, prefix, test_result= test_mgr._init_test_foundations(table_name, 
                                                                                       test_case_name, 
                                                                                       compute_pool_id)
        print(f"test_suite_def: {test_suite_def.test_suite[1]}")
        test_case = test_suite_def.test_suite[1]
        
        statements = test_mgr._execute_test_inputs(test_case=test_case,
                                                      table_ref=table_ref,
                                                      prefix="dev-ins",
                                                      compute_pool_id=compute_pool_id)
        assert len(statements) == 3
        assert statements[0].name == "dev-ins-src-p3-roles-ut"
        print(f"statement: {statements[0]}")


    @patch('shift_left.core.test_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_test_suite(self, 
                        mock_post_flink_statement, 
                        mock_get_statement_info, 
                        mock_get_statement_results,
                        mock_table_exists,
                        mock_get_statement,
                        mock_delete_statement):
        
        """Test the execution of one test case."""
        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            if statement_name.startswith("dev-p1-ddl-int-table-1"):
                return None
            return StatementInfo(name=statement_name, status_phase="UNKNOWN", sql_content=self._sql_content)

        self._sql_content = ""
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            self._sql_content = sql_content
            if "ddl" in statement_name or "-ins" in statement_name:
                return Statement(name=statement_name, status={"phase": "COMPLETED"})
            else:
                return Statement(name=statement_name, status={"phase": "RUNNING"})

        def _mock_statement_results(statement_name):
            print(f"mock_statement_results: {statement_name}")
            op_row = OpRow(op=0, row=["PASS"]) if "val-1" in statement_name else OpRow(op=0, row=["FAIL"])
            data = Data(data=[op_row])
            result = StatementResult(results=data, 
                                     api_version="v1", 
                                     kind="StatementResult", 
                                     metadata=None)
            return result

        mock_get_statement_results.side_effect = _mock_statement_results
        mock_get_statement_info.side_effect = _mock_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = self._mock_table_exists
        mock_get_statement.side_effect = self._mock_get_None_statement
        mock_delete_statement.return_value = None  # Mock delete operation

        table_name = "p1_fct_order"
        suite_result = test_mgr.execute_one_or_all_tests(table_name, run_validation=True)
        assert suite_result
        assert len(suite_result.test_results) == 2
        assert len(suite_result.foundation_statements) == 4
        assert suite_result.test_results["test_case_1"].result == "PASS"
        assert suite_result.test_results["test_case_2"].result == "FAIL"
        print(suite_result.model_dump_json(indent=2))

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_list')
    @patch('shift_left.core.test_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.drop_table')
    def test_delete_test_artifacts(self, 
                                   mock_drop_table, 
                                   mock_delete_statement, 
                                   mock_get_statement_list):
        """Test deletion of test artifacts including statements and tables."""
        mock_get_statement_list.return_value = []
        
        table_name = "p1_fct_order"
        compute_pool_id = "test_pool"
        
        test_mgr.delete_test_artifacts(table_name, compute_pool_id)
        
        # Verify statement deletions were called
        self.assertTrue(mock_delete_statement.called)
        # Verify table drops were called
        self.assertTrue(mock_drop_table.called)
        
        # Check that drop_table was called for main table and foundations
        expected_calls = []
        expected_calls.append(unittest.mock.call(table_name + "_ut", compute_pool_id))
        expected_calls.append(unittest.mock.call("int_table_1_ut", compute_pool_id))
        expected_calls.append(unittest.mock.call("int_table_2_ut", compute_pool_id))
        
        mock_drop_table.assert_has_calls(expected_calls, any_order=True)

    def test_load_test_suite_definition_file_not_found(self):
        """Test error handling when test definition file is not found."""
        with self.assertRaises(Exception) as context:
            test_mgr._load_test_suite_definition("nonexistent_table")
        
        self.assertIn("not in inventory", str(context.exception))

    def test_load_test_suite_definition_invalid_yaml(self):
        """Test error handling when test definition file contains invalid YAML."""
        # Create a temporary invalid test definition file
        test_folder = os.path.join(os.getenv("PIPELINES"), "facts/p1/fct_order/tests")
        invalid_yaml_file = os.path.join(test_folder, "test_definitions_invalid.yaml")
        
        with open(invalid_yaml_file, "w") as f:
            f.write("invalid: yaml: content: [")
        
        try:
            # Temporarily rename the valid file
            valid_file = os.path.join(test_folder, "test_definitions.yaml")
            backup_file = os.path.join(test_folder, "test_definitions_backup.yaml")
            if os.path.exists(valid_file):
                os.rename(valid_file, backup_file)
            os.rename(invalid_yaml_file, valid_file)
            
            with self.assertRaises(Exception):
                test_mgr._load_test_suite_definition("p1_fct_order")
        finally:
            # Restore the original file
            if os.path.exists(valid_file):
                os.remove(valid_file)
            if os.path.exists(backup_file):
                os.rename(backup_file, valid_file)
            if os.path.exists(invalid_yaml_file):
                os.remove(invalid_yaml_file)

    def test_read_csv_file(self):
        """Test reading CSV file and parsing headers and rows."""
        # Create a temporary CSV file
        test_csv_content = "id,name,value\n1,test1,100\n2,test2,200\n3,test3,300"
        temp_csv_file = "/tmp/test_csv.csv"
        
        with open(temp_csv_file, "w") as f:
            f.write(test_csv_content)
        
        try:
            headers, rows = test_mgr._read_csv_file(temp_csv_file)
            
            self.assertEqual(headers, "id,name,value")
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0], "1,test1,100")
            self.assertEqual(rows[1], "2,test2,200")
            self.assertEqual(rows[2], "3,test3,300")
        finally:
            if os.path.exists(temp_csv_file):
                os.remove(temp_csv_file)

    def test_transform_csv_to_sql(self):
        """Test transformation of CSV data to SQL insert statement."""
        table_name = "test_table"
        headers = "id,name,value"
        rows = ["1,test1,100", "2,test2,200", "3,test3,300"]
        
        sql = test_mgr._transform_csv_to_sql(table_name, headers, rows)
        
        self.assertIn("insert into test_table (id,name,value) values", sql)
        self.assertIn("(1,test1,100)", sql)
        self.assertIn("(2,test2,200)", sql)
        self.assertIn("(3,test3,300)", sql)
        self.assertTrue(sql.endswith(";\n"))

    def test_transform_csv_to_sql_large_content(self):
        """Test CSV to SQL transformation with large content that exceeds size limit."""
        table_name = "test_table"
        headers = "id,name,description"
        # Create large rows that will exceed the 4MB limit
        large_row_data = "x" * 1000000  # 1MB per row
        rows = [f"{i},name{i},{large_row_data}" for i in range(5)]  # 5MB total
        
        sql = test_mgr._transform_csv_to_sql(table_name, headers, rows)
        
        # Should truncate and end with semicolon
        self.assertTrue(sql.endswith(";\n"))
        self.assertIn("insert into test_table", sql)

    def test_build_statement_name(self):
        """Test building statement names with various inputs."""
        # Normal case
        result = test_mgr._build_statement_name("test_table", "dev-ddl")
        self.assertEqual(result, "dev-ddl-test-table-ut")
        
        # Long table name truncation
        long_table_name = "a" * 60  # Longer than 52 characters
        result = test_mgr._build_statement_name(long_table_name, "dev-ddl")
        expected_truncated = "a" * 52
        self.assertEqual(result, f"dev-ddl-{expected_truncated}-ut")
        
        # Special characters replacement
        result = test_mgr._build_statement_name("test.table_name", "dev-ddl")
        self.assertEqual(result, "dev-ddl-test-table-name-ut")

    def test_build_data_sample_different_types(self):
        """Test building data samples with different column types."""
        columns = {
            "id": {"type": "BIGINT"},
            "name": {"type": "VARCHAR"},
            "description": {"type": "TEXT"},
            "count": {"type": "BIGINT"}
        }
        
        column_names, rows = test_mgr._build_data_sample(columns)
        
        # Check column names
        self.assertIn("`id`", column_names)
        self.assertIn("`name`", column_names)
        self.assertIn("`description`", column_names)
        self.assertIn("`count`", column_names)
        
        # Check data generation
        self.assertIn("0,", rows)  # BIGINT columns should have 0
        self.assertIn("'name_1'", rows)  # VARCHAR columns should have quoted values
        self.assertIn("'description_2'", rows)
        
        # Should have DEFAULT_TEST_DATA_ROWS rows
        self.assertEqual(rows.count("),"), test_mgr.DEFAULT_TEST_DATA_ROWS - 1)  # 4 commas between 5 rows

    def test_build_data_sample_with_offset(self):
        """Test building data samples with index offset."""
        columns = {"id": {"type": "BIGINT"}, "name": {"type": "VARCHAR"}}
        
        column_names, rows = test_mgr._build_data_sample(columns, idx_offset=10)
        
        # Should start from index 11 (10 + 1)
        self.assertIn("'name_11'", rows)
        self.assertIn("'name_1" + str(test_mgr.DEFAULT_TEST_DATA_ROWS) +"'", rows)  # Last row should be 15 (10 + DEFAULT_TEST_DATA_ROWS)

    @patch('shift_left.core.test_mgr.get_config')
    @patch('shift_left.core.test_mgr.os.remove')
    @patch('shift_left.core.test_mgr.ConfluentCloudClient')
    @patch('shift_left.core.test_mgr.os.path.exists')
    def test_table_exists_cache_hit(self, mock_exists, mock_ccloud_client, mock_remove, mock_get_config):
        """Test _table_exists function with cache hit scenario."""
        from shift_left.core.test_mgr import TopicListCache
        from datetime import datetime
        
        # Mock get_config to return cache_ttl
        mock_get_config.return_value = {'app': {'cache_ttl': 3600}}
        
        # Mock file doesn't exist to force cache miss and trigger API call
        mock_exists.return_value = False
        
        # Mock Confluent Cloud client
        mock_client_instance = mock_ccloud_client.return_value
        mock_client_instance.list_topics.return_value = {
            "data": [
                {"topic_name": "test_table"},
                {"topic_name": "another_table"}
            ]
        }
        
        # Reset the global cache
        test_mgr._topic_list_cache = None
        
        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            result = test_mgr._table_exists("test_table")
            self.assertTrue(result)
            
            result = test_mgr._table_exists("nonexistent_table")
            self.assertFalse(result)

    @patch('shift_left.core.test_mgr.get_config')
    @patch('shift_left.core.test_mgr.ConfluentCloudClient')
    @patch('shift_left.core.test_mgr.os.path.exists')
    def test_table_exists_cache_miss(self, mock_exists, mock_ccloud_client, mock_get_config):
        """Test _table_exists function with cache miss - fetch from API."""
        # Mock get_config to return a valid config
        mock_get_config.return_value = {"confluent_cloud": {"api_key": "test", "api_secret": "test"}}
        
        # Mock file doesn't exist, so cache miss
        mock_exists.return_value = False
        
        # Mock Confluent Cloud client
        mock_client_instance = mock_ccloud_client.return_value
        mock_client_instance.list_topics.return_value = {
            "data": [
                {"topic_name": "fresh_table"},
                {"topic_name": "api_table"}
            ]
        }
        
        # Reset the global cache
        test_mgr._topic_list_cache = None
        
        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            result = test_mgr._table_exists("fresh_table")
            self.assertTrue(result)
            
            result = test_mgr._table_exists("missing_table")
            self.assertFalse(result)
            
            # Verify that the cache file was written
            mock_file.assert_called()

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    def test_poll_response_success_first_try(self, mock_get_results, mock_get_statement):
        """Test _poll_response function when results are available on first try."""
        from shift_left.core.models.flink_statement_model import StatementResult, Data, OpRow, Statement
        
        # Mock get_statement to return a successful statement
        mock_statement = Statement(name="test_statement", status={"phase": "COMPLETED"})
        mock_get_statement.return_value = mock_statement
        
        # Mock successful response on first call
        op_row = OpRow(op=0, row=["PASS"])
        data = Data(data=[op_row])
        result = StatementResult(results=data, api_version="v1", kind="StatementResult", metadata=None)
        mock_get_results.return_value = result
        
        final_result, statement_result = test_mgr._poll_response("test_statement")
        
        self.assertEqual(final_result, "PASS")
        self.assertEqual(statement_result, result)
        mock_get_results.assert_called_once_with("test_statement")

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.time.sleep')
    def test_poll_response_retry_logic(self, mock_sleep, mock_get_results, mock_get_statement):
        """Test _poll_response function retry logic with empty results."""
        from shift_left.core.models.flink_statement_model import StatementResult, Data, OpRow, Statement
        
        # Mock get_statement to return a successful statement
        mock_statement = Statement(name="test_statement", status={"phase": "RUNNING"})
        mock_get_statement.return_value = mock_statement
        
        # First few calls return empty results, last call returns data
        empty_result = StatementResult(results=Data(data=[]), api_version="v1", kind="StatementResult", metadata=None)
        op_row = OpRow(op=0, row=["PASS"])
        data = Data(data=[op_row])
        success_result = StatementResult(results=data, api_version="v1", kind="StatementResult", metadata=None)
        
        mock_get_results.side_effect = [empty_result, empty_result, success_result]
        
        final_result, statement_result = test_mgr._poll_response("test_statement")
        
        self.assertEqual(final_result, "PASS")
        self.assertEqual(statement_result, success_result)
        self.assertEqual(mock_get_results.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)  # Sleep called for first 2 empty results

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    def test_poll_response_max_retries_exceeded(self, mock_get_results, mock_get_statement):
        """Test _poll_response function when max retries are exceeded."""
        from shift_left.core.models.flink_statement_model import StatementResult, Data, Statement
        
        # Mock get_statement to return a running statement
        mock_statement = Statement(name="test_statement", status={"phase": "RUNNING"})
        mock_get_statement.return_value = mock_statement
        
        # Always return empty results
        empty_result = StatementResult(results=Data(data=[]), api_version="v1", kind="StatementResult", metadata=None)
        mock_get_results.return_value = empty_result
        
        with patch('shift_left.core.test_mgr.time.sleep'):
            final_result, statement_result = test_mgr._poll_response("test_statement")
        
        self.assertEqual(final_result, "FAIL")  # Default when no data
        # Should call get_results for max_retries - 1 times (range(1, 7) = 1,2,3,4,5,6)
        self.assertEqual(mock_get_results.call_count, 6)  # max_retries - 1

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    def test_poll_response_exception_handling(self, mock_get_results, mock_get_statement):
        """Test _poll_response function exception handling."""
        from shift_left.core.models.flink_statement_model import Statement
        
        # Mock get_statement to return a running statement
        mock_statement = Statement(name="test_statement", status={"phase": "RUNNING"})
        mock_get_statement.return_value = mock_statement
        
        # Mock exception on first call
        mock_get_results.side_effect = Exception("API Error")
        
        final_result, statement_result = test_mgr._poll_response("test_statement")
        
        self.assertEqual(final_result, "FAIL")
        self.assertIsNone(statement_result)

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_or_build_sql_content_transformer')
    def test_execute_flink_test_statement_new_statement(self, 
                                                         mock_transformer, 
                                                         mock_post_statement, 
                                                         mock_get_statement):
        """Test _execute_flink_test_statement when statement doesn't exist."""
        from shift_left.core.models.flink_statement_model import StatementError
        
        # Mock that statement doesn't exist
        mock_get_statement.return_value = StatementError(message="Not found")
        
        # Mock transformer
        mock_transformer_instance = mock_transformer.return_value
        mock_transformer_instance.update_sql_content.return_value = ("", "transformed_sql")
        
        # Mock post statement success
        expected_statement = Statement(name="test_statement", status={"phase": "RUNNING"})
        mock_post_statement.return_value = expected_statement
        
        result, is_new = test_mgr._execute_flink_test_statement(
            sql_content="SELECT * FROM test",
            statement_name="test_statement",
            compute_pool_id="test_pool"
        )
        
        self.assertEqual(result, expected_statement)
        self.assertTrue(is_new)  # Should be new since we created it
        mock_post_statement.assert_called_once()
        mock_transformer_instance.update_sql_content.assert_called_once()

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    def test_execute_flink_test_statement_existing_statement(self, mock_get_statement):
        """Test _execute_flink_test_statement when statement already exists."""
        # Mock that statement exists
        existing_statement = Statement(name="test_statement", status={"phase": "RUNNING"})
        mock_get_statement.return_value = existing_statement
        
        result, is_new = test_mgr._execute_flink_test_statement(
            sql_content="SELECT * FROM test", 
            statement_name="test_statement",
            compute_pool_id="test_pool"
        )
        
        self.assertEqual(result, existing_statement)
        self.assertFalse(is_new)  # Should not be new since it already exists

    def test_execute_one_or_all_tests_error_handling(self):
        """Test execute_one_or_all_tests function error handling."""
        with patch('shift_left.core.test_mgr._init_test_foundations') as mock_init:
            mock_init.side_effect = Exception("Foundation error")
            
            with self.assertRaises(Exception) as context:
                test_mgr.execute_one_or_all_tests("nonexistent_table", "test_case")
            
            self.assertIn("Foundation error", str(context.exception))

    def test_execute_one_or_all_tests_error_handling(self):
        """Test execute_one_or_all_tests function error handling."""
        with patch('shift_left.core.test_mgr._init_test_foundations') as mock_init:
            mock_init.side_effect = Exception("Foundation error")
            
            with self.assertRaises(Exception) as context:
                test_mgr.execute_one_or_all_tests("nonexistent_table")
            
            self.assertIn("Foundation error", str(context.exception))

    @patch('shift_left.core.test_mgr.from_pipeline_to_absolute')
    def test_read_and_treat_sql_content_for_ut(self, mock_from_pipeline):
        """Test reading and treating SQL content for unit tests."""
        # Create a temporary SQL file
        temp_sql_content = "SELECT * FROM test_table WHERE id > 0;"
        temp_sql_file = "/tmp/test_sql.sql"
        mock_from_pipeline.return_value = temp_sql_file
        
        def _transform_sql_content(sql_content, table_name):
            return sql_content

        def _transform_sql_content_upper(sql_content, table_name):
            return sql_content.upper()
        
        with open(temp_sql_file, "w") as f:
            f.write(temp_sql_content)
        
        try:
            # Test with identity function
            result = test_mgr._read_and_treat_sql_content_for_ut("test_path", _transform_sql_content, "test_table")
            self.assertEqual(result, temp_sql_content)
            
            # Test with transformation function
            result = test_mgr._read_and_treat_sql_content_for_ut("test_path", _transform_sql_content_upper, "test_table")
            self.assertEqual(result, temp_sql_content.upper())
        finally:
            if os.path.exists(temp_sql_file):
                os.remove(temp_sql_file)

    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr._read_and_treat_sql_content_for_ut')
    @patch('shift_left.core.test_mgr._execute_flink_test_statement')
    def test_load_sql_and_execute_statement_ddl_table_exists(self, 
                                                              mock_execute, 
                                                              mock_read_sql, 
                                                              mock_table_exists):
        """Test _load_sql_and_execute_statement when DDL table already exists."""
        mock_table_exists.return_value = True
        mock_read_sql.return_value = "CREATE TABLE test_table (id INT);"
        
        result = test_mgr._load_sql_and_execute_statement(
            table_name="test_table",
            sql_path="test.sql",
            prefix="dev-ddl"
        )
        
        # Should return [] when table exists and prefix is ddl
        self.assertEqual(result, [])
        mock_execute.assert_not_called()

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr._read_and_treat_sql_content_for_ut')
    @patch('shift_left.core.test_mgr._execute_flink_test_statement')
    def test_load_sql_and_execute_statement_dml_running(self, 
                                                         mock_execute, 
                                                         mock_read_sql, 
                                                         mock_table_exists, 
                                                         mock_get_statement_info):
        """Test _load_sql_and_execute_statement when DML statement is already running."""
        from shift_left.core.models.flink_statement_model import StatementInfo
        
        mock_table_exists.return_value = False
        mock_read_sql.return_value = "INSERT INTO test_table VALUES (1);"
        mock_get_statement_info.return_value = StatementInfo(
            name="test_statement", 
            status_phase="RUNNING", 
            sql_content="INSERT INTO test_table VALUES (1);"
        )
        
        result = test_mgr._load_sql_and_execute_statement(
            table_name="test_table",
            sql_path="test.sql",
            prefix="dev-dml"
        )
        
        # Should return [] when DML statement is running
        self.assertEqual(result, [])
        mock_execute.assert_not_called()

    @patch('shift_left.core.test_mgr.from_pipeline_to_absolute')
    @patch('shift_left.core.test_mgr.SQLparser')
    def test_process_foundation_ddl_from_test_definitions(self, mock_parser_class, mock_from_pipeline):
        """Test processing foundation DDL from test definitions."""
        # Mock parser
        mock_parser = mock_parser_class.return_value
        mock_parser.build_column_metadata_from_sql_content.return_value = {
            "id": {"type": "BIGINT"},
            "name": {"type": "VARCHAR"}
        }
        
        # Create temporary DDL file
        temp_ddl_content = "CREATE TABLE test_table (id BIGINT, name VARCHAR(100));"
        temp_ddl_file = "/tmp/test_ddl.sql"
        mock_from_pipeline.return_value = temp_ddl_file
        
        with open(temp_ddl_file, "w") as f:
            f.write(temp_ddl_content)
        
        try:
            # Create the tests directory
            test_dir = "/tmp/tests"
            os.makedirs(test_dir, exist_ok=True)
            
            # Create test definition
            foundation = Foundation(table_name="test_table", ddl_for_test="./tests/ddl_test_table.sql")
            test_definition = SLTestDefinition(foundations=[foundation], test_suite=[])
            
            # Mock table inventory
            table_inventory = {
                "test_table": {
                    "table_name": "test_table",
                    "ddl_ref": "test_ddl.sql"
                }
            }
            
            result = test_mgr._process_foundation_ddl_from_test_definitions(
                test_definition, 
                test_dir, 
                table_inventory
            )
            
            self.assertIn("test_table", result)
            self.assertEqual(result["test_table"]["id"]["type"], "BIGINT")
            self.assertEqual(result["test_table"]["name"]["type"], "VARCHAR")
        finally:
            if os.path.exists(temp_ddl_file):
                os.remove(temp_ddl_file)
            # Clean up any created DDL test files
            test_ddl_out_file = "/tmp/tests/ddl_test_table.sql"
            if os.path.exists(test_ddl_out_file):
                os.remove(test_ddl_out_file)
            # Clean up test directory
            if os.path.exists("/tmp/tests"):
                import shutil
                shutil.rmtree("/tmp/tests")

    def test_build_save_test_definition_json_file(self):
        """Test building and saving test definition JSON file."""
        import tempfile
        import shutil
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp()
        
        try:
            table_name = "test_fact_table"
            referenced_tables = ["input_table_1", "input_table_2"]
            
            result = test_mgr._build_save_test_definition_json_file(
                temp_dir, 
                table_name, 
                referenced_tables
            )
            
            # Verify test definition structure
            self.assertEqual(len(result.foundations), 2)
            self.assertEqual(len(result.test_suite), 2)
            
            # Check foundations
            foundation_names = [f.table_name for f in result.foundations]
            self.assertIn("input_table_1", foundation_names)
            self.assertIn("input_table_2", foundation_names)
            
            # Check test cases
            self.assertEqual(result.test_suite[0].name, "test_test_fact_table_1")
            self.assertEqual(result.test_suite[1].name, "test_test_fact_table_2")
            
            # Verify file was created
            yaml_file = os.path.join(temp_dir, "test_definitions.yaml")
            self.assertTrue(os.path.exists(yaml_file))
            
            # Verify file content
            with open(yaml_file, "r") as f:
                content = f.read()
                self.assertIn("test_test_fact_table_1", content)
                self.assertIn("input_table_1", content)
        finally:
            shutil.rmtree(temp_dir)

    @patch('shift_left.core.test_mgr.from_pipeline_to_absolute')
    @patch('shift_left.core.test_mgr.SQLparser')
    def test_add_test_files_no_referenced_tables(self, mock_parser_class, mock_from_pipeline):
        """Test _add_test_files when no referenced tables are found."""
        # Mock parser to return empty list
        mock_parser = mock_parser_class.return_value
        mock_parser.extract_table_references.return_value = []
        
        # Create temporary DML file
        temp_dml_content = "SELECT 1;"
        temp_dml_file = "/tmp/test_dml.sql"
        mock_from_pipeline.return_value = temp_dml_file
        
        with open(temp_dml_file, "w") as f:
            f.write(temp_dml_content)
        
        try:
            # Create mock table reference
            table_ref = FlinkTableReference(
                table_name="test_table",
                dml_ref="test_dml.sql",
                table_folder_name="/tmp"
            )
            
            with self.assertRaises(ValueError) as context:
                test_mgr._add_test_files(table_ref, "/tmp/tests", {})
            
            self.assertIn("No referenced table names found", str(context.exception))
        finally:
            if os.path.exists(temp_dml_file):
                os.remove(temp_dml_file)

    @patch('shift_left.core.test_mgr.get_config')
    @patch('shift_left.core.test_mgr.os.remove')
    @patch('shift_left.core.test_mgr.datetime')
    def test_table_exists_cache_error_handling(self, mock_datetime, mock_remove, mock_get_config):
        """Test _table_exists cache error handling when loading corrupted cache."""
        import json
        from datetime import datetime
        
        # Mock get_config to return a valid config
        mock_get_config.return_value = {"confluent_cloud": {"api_key": "test", "api_secret": "test"}}
        
        # Mock datetime
        mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
        mock_datetime.strptime.side_effect = ValueError("Invalid date format")
        
        # Create corrupted cache file
        corrupted_cache = "/tmp/corrupted_topic_list.json"
        with open(corrupted_cache, "w") as f:
            json.dump({"corrupted": "data"}, f)
        
        # Patch the TOPIC_LIST_FILE constant
        with patch('shift_left.core.test_mgr.TOPIC_LIST_FILE', corrupted_cache):
            with patch('shift_left.core.test_mgr.ConfluentCloudClient') as mock_ccloud:
                mock_client = mock_ccloud.return_value
                mock_client.list_topics.return_value = {"data": [{"topic_name": "test_table"}]}
                
                # Reset cache
                test_mgr._topic_list_cache = None
                
                with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
                    result = test_mgr._table_exists("test_table")
                    self.assertTrue(result)
                    
                    # Verify corrupted file was removed
                    mock_remove.assert_called_with(corrupted_cache)
        
        # Clean up
        if os.path.exists(corrupted_cache):
            os.remove(corrupted_cache)


    @patch('shift_left.core.test_mgr.SQLparser')
    def test_replace_table_name_substring_issue_fix(self, mock_parser_class):
        """
        Test the improved replace_table_name function to ensure it fixes the substring replacement issue.
        
        The original function had a bug where table names that were substrings of other table names
        would cause incorrect replacements. For example:
        - table_name_a and table_name_a_b_c would become:
        - table_name_a_ut and table_name_a_ut_b_c (WRONG)
        
        The improved function should produce:
        - table_name_a_ut and table_name_a_b_c_ut (CORRECT)
        """
        # Access the nested function from _start_ddl_dml_for_flink_under_test
        from shift_left.core.utils.file_search import FlinkTableReference
        
        # Create a mock parser
        mock_parser = mock_parser_class.return_value
        
        # Test Case 1: Simple case with no substring conflicts
        mock_parser.extract_table_references.return_value = {'table_a', 'table_b'}
        
        # Create a dummy table reference to access the replace_table_name function
        table_ref = FlinkTableReference(
            table_name="test_table",
            dml_ref="test.sql",
            ddl_ref="test_ddl.sql"
        )
        
        # We need to call the function through _start_ddl_dml_for_flink_under_test
        # but we'll extract the replace_table_name logic by testing the SQL transformation
        
        sql_input = "SELECT * FROM table_a JOIN table_b ON table_a.id = table_b.id"
        
        with patch('shift_left.core.test_mgr._load_sql_and_execute_statement') as mock_load_sql:
            # Mock the file loading to return our test SQL
            def mock_sql_loader(table_name, sql_path, prefix, compute_pool_id, fct, product_name, statements=None):
                # Apply the function transformation to our test SQL
                return fct(sql_input, table_name)
            
            mock_load_sql.side_effect = mock_sql_loader
            
            # This will call replace_table_name internally
            test_mgr._start_ddl_dml_for_flink_under_test("test_table", table_ref)
            
            # Verify the function was called with our SQL
            self.assertTrue(mock_load_sql.called)
            
        # Test Case 2: Substring conflict case - the main bug we're fixing
        mock_parser.extract_table_references.return_value = {'table_name_a', 'table_name_a_b_c'}
        
        sql_with_substring_issue = "SELECT * FROM table_name_a JOIN table_name_a_b_c ON table_name_a.id = table_name_a_b_c.id"
        
        with patch('shift_left.core.test_mgr._load_sql_and_execute_statement') as mock_load_sql:
            transformed_sql = None
            
            def capture_transformed_sql(table_name, sql_path, prefix, compute_pool_id, fct, product_name, statements=None):
                nonlocal transformed_sql
                transformed_sql = fct(sql_with_substring_issue, table_name)
                return None
            
            mock_load_sql.side_effect = capture_transformed_sql
            
            test_mgr._start_ddl_dml_for_flink_under_test("test_table", table_ref)
            
            # Verify the transformation was applied correctly
            self.assertIsNotNone(transformed_sql)
            
            # Check that both table names got the correct suffix
            self.assertIn('table_name_a_ut', transformed_sql)
            self.assertIn('table_name_a_b_c_ut', transformed_sql)
            
            # Most importantly, verify the substring issue is fixed:
            # table_name_a_b_c should NOT become table_name_a_ut_b_c
            self.assertNotIn('table_name_a_ut_b_c', transformed_sql)
            
            # Verify the exact expected result
            expected_result = "SELECT * FROM table_name_a_ut JOIN table_name_a_b_c_ut ON table_name_a_ut.id = table_name_a_b_c_ut.id"
            self.assertEqual(transformed_sql, expected_result)
        
        # Test Case 3: Multiple overlapping table names
        mock_parser.extract_table_references.return_value = {'user_data', 'user_data_archive', 'user_data_backup'}
        
        sql_multiple_overlaps = "SELECT * FROM user_data JOIN user_data_archive JOIN user_data_backup ON user_data.id = user_data_archive.id"
        
        with patch('shift_left.core.test_mgr._load_sql_and_execute_statement') as mock_load_sql:
            transformed_sql = None
            
            def capture_transformed_sql(table_name, sql_path, prefix, compute_pool_id, fct, product_name, statements=None):
                nonlocal transformed_sql
                transformed_sql = fct(sql_multiple_overlaps, table_name)
                return None
            
            mock_load_sql.side_effect = capture_transformed_sql
            
            test_mgr._start_ddl_dml_for_flink_under_test("test_table", table_ref)
            
            # Verify all table names got the correct suffix
            self.assertIn('user_data_ut', transformed_sql)
            self.assertIn('user_data_archive_ut', transformed_sql)
            self.assertIn('user_data_backup_ut', transformed_sql)
            
            # Verify NO incorrect substring replacements occurred
            self.assertNotIn('user_data_ut_archive', transformed_sql)
            self.assertNotIn('user_data_ut_backup', transformed_sql)
        
        # Test Case 4: Case insensitive matching
        mock_parser.extract_table_references.return_value = {'Table_Name_A', 'table_name_b'}
        
        sql_case_insensitive = "SELECT * FROM Table_Name_A JOIN table_name_b ON Table_Name_A.id = table_name_b.id"
        
        with patch('shift_left.core.test_mgr._load_sql_and_execute_statement') as mock_load_sql:
            transformed_sql = None
            
            def capture_transformed_sql(table_name, sql_path, prefix, compute_pool_id, fct, product_name, statements=None):
                nonlocal transformed_sql
                transformed_sql = fct(sql_case_insensitive, table_name)
                return None
            
            mock_load_sql.side_effect = capture_transformed_sql
            
            test_mgr._start_ddl_dml_for_flink_under_test("test_table", table_ref)
            
            # Verify case insensitive replacement works
            self.assertIn('Table_Name_A_ut', transformed_sql)
            self.assertIn('table_name_b_ut', transformed_sql)
        
        # Test Case 5: Edge case with empty table names list
        mock_parser.extract_table_references.return_value = set()
        
        sql_no_tables = "SELECT 1 as test_value"
        
        with patch('shift_left.core.test_mgr._load_sql_and_execute_statement') as mock_load_sql:
            transformed_sql = None
            
            def capture_transformed_sql(table_name, sql_path, prefix, compute_pool_id, fct, product_name, statements=None):
                nonlocal transformed_sql
                transformed_sql = fct(sql_no_tables, table_name)
                return None
            
            mock_load_sql.side_effect = capture_transformed_sql
            
            test_mgr._start_ddl_dml_for_flink_under_test("test_table", table_ref)
            
            # SQL should remain unchanged when no tables are found
            self.assertEqual(transformed_sql, sql_no_tables)

    def test_generate_test_readme(self):
        """Test _generate_test_readme function."""
        # Create mock table reference
        table_ref = FlinkTableReference(
            table_name="test_table",
            dml_ref="test.sql",
            ddl_ref="test_ddl.sql"
        )
        foundation_1 = Foundation(table_name="test_table_1", ddl_for_test="./tests/ddl_test_table.sql")
        foundation_2 = Foundation(table_name="test_table_2", ddl_for_test="./tests/ddl_test_table.sql")
        test_definition = SLTestDefinition(foundations=[foundation_1, foundation_2], test_suite=[])
        primary_keys = ["id", "name"]
        tests_folder_path = str(pathlib.Path(__file__)) + "../tests"
        if not os.path.exists(tests_folder_path):
            os.makedirs(tests_folder_path)
        test_mgr._generate_test_readme(table_ref, test_definition, primary_keys, tests_folder_path)
        with open(tests_folder_path + "/README.md", "r") as f:
            content = f.read()
            self.assertIn("test_table", content)
            self.assertIn("id", content)
            self.assertIn("name", content)
            print(content)
        os.remove(tests_folder_path + "/README.md")
        os.rmdir(tests_folder_path)


    def test_create_validation_sql_content(self):
        inventory_path = os.path.join(os.getenv("PIPELINES"),)
        table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
        sql_content = test_mgr._build_validation_sql_content(table_name="fct_user_per_group", 
                                                        table_inventory=table_inventory)
        print(f"sql_content: {sql_content}")    
        assert sql_content is not None
        assert "expected_group_id" in sql_content
        assert "expected_group_name" in sql_content
        assert "expected_group_type" in sql_content
        assert "expected_total_users" in sql_content
        assert "expected_active_users" in sql_content
        assert "expected_inactive_users" in sql_content
        assert "expected_latest_user_created_date" in sql_content
        assert "case when a.group_id = e.expected_group_id then 'PASS' else 'FAIL' end as group_id_check" in sql_content
        assert "case when a.group_name = e.expected_group_name then 'PASS' else 'FAIL' end as group_name_check" in sql_content
        assert "case when a.group_type = e.expected_group_type then 'PASS' else 'FAIL' end as group_type_check" in sql_content
        assert "case when a.total_users = e.expected_total_users then 'PASS' else 'FAIL' end as total_users_check" in sql_content
        assert "case when a.active_users = e.expected_active_users then 'PASS' else 'FAIL' end as active_users_check" in sql_content
        assert "case when a.inactive_users = e.expected_inactive_users then 'PASS' else 'FAIL' end as inactive_users_check" in sql_content
        assert "case when a.latest_user_created_date = e.expected_latest_user_created_date then 'PASS' else 'FAIL' end as latest_user_created_date_check" in sql_content
        assert "case when a.fact_updated_at = e.expected_fact_updated_at then 'PASS' else 'FAIL' end as fact_updated_at_check" in sql_content
   

if __name__ == '__main__':
    unittest.main()