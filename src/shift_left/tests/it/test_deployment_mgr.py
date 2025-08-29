"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
from datetime import datetime
import pathlib

os.environ["CONFIG_FILE"] =   str(pathlib.Path(__file__).parent.parent / "config-ccloud.yaml")
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
import shift_left.core.table_mgr as table_mgr
import shift_left.core.pipeline_mgr as pipeline_mgr
from shift_left.core.utils.app_config import get_config
from  shift_left.core.models.flink_statement_model import *
import shift_left.core.statement_mgr as sm
import shift_left.core.deployment_mgr as dm
from shift_left.core.utils.file_search import FlinkTablePipelineDefinition, FlinkStatementNode
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.file_search import get_ddl_dml_names_from_pipe_def
from datetime import datetime, timedelta, timezone


"""
This test suite is used to test the deployment manager with connection to Confluent Cloud.
"""
class TestDeploymentManager(unittest.TestCase):
    
    data_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        os.environ["PIPELINES"] = str(cls.data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "spark-project")
        table_mgr.build_inventory(os.getenv("PIPELINES"))
        pipeline_mgr.build_all_pipeline_definitions(os.getenv("PIPELINES"))

    def test_1_clean_things(self):
        for table in ["src_table_1", "src_p1_table_2", "src_p1_table_3", "int_p1_table_2", "int_p1_table_1", "p1_fct_order"]:
            try:
                print(f"Dropping table {table}")
                sm.drop_table(table)
                print(f"Table {table} dropped")
            except Exception as e:
                print(e)
    
       
    def test_3_deploy_one_src_table(self):
        """
        Given a source table with children, deploy the DDL and DML without the children.
        """
        inventory_path = os.getenv("PIPELINES")
        table_name = "src_table_1"
        summary,execution_plan = dm.build_deploy_pipeline_from_table(table_name=table_name, 
                                               inventory_path=inventory_path, 
                                               compute_pool_id=None, 
                                               dml_only=False, 
                                               may_start_descendants=False,
                                               force_ancestors=False,
                                               execute_plan=True)
        assert execution_plan
        print(execution_plan)
        assert summary
        print(summary)
        for node in execution_plan.nodes:
            print(f"statement: {node.existing_statement_info.name} - {node.existing_statement_info.status_phase} - {node.existing_statement_info.compute_pool_id}")
        report = dm.report_running_flink_statements_for_a_table(table_name, inventory_path)
        assert report
        print(report)


    def test_4_deploy_pipeline_from_sink_table(self):
        config = get_config()
        table_name="p1_fct_order"
        inventory_path= os.getenv("PIPELINES")
        summary, execution_plan = dm.build_deploy_pipeline_from_table(table_name=table_name, 
                                               inventory_path=inventory_path, 
                                               compute_pool_id=config['flink']['compute_pool_id'], 
                                               dml_only=False, 
                                               may_start_descendants=False,
                                               execute_plan=True,
                                               force_ancestors=True)
        assert execution_plan
        print(summary)
        print("Validating running dml")
        result = dm.report_running_flink_statements_for_a_table(table_name, inventory_path)
        assert result
        print(result)
        


        
    def test_5_deploy_pipeline_from_int_table(self):
        config = get_config()
        table_name="int_p1_table_1"
        inventory_path= os.getenv("PIPELINES")
        summary, execution_plan = dm.build_deploy_pipeline_from_table(table_name=table_name, 
                                               inventory_path=inventory_path, 
                                               compute_pool_id=config['flink']['compute_pool_id'], 
                                               dml_only=False, 
                                               may_start_descendants=True,
                                               execute_plan=True,
                                               force_ancestors=True)
        assert execution_plan
        print(summary)
        print("Validating running dml")
        result = dm.report_running_flink_statements_for_a_table(table_name, inventory_path)
        assert result
        print(result)


    def test_6_0_deploy_by_medals_src(self):
        """
        """
        os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
        config = get_config()
        for table in ["src_x", "src_y", "src_p2_a", "src_b"]:
            try:
                print(f"Dropping table {table}")
                sm.drop_table(table)
                print(f"Table {table} dropped")
            except Exception as e:
                print(e)

        summary, execution_plan = dm.build_and_deploy_all_from_directory( directory=os.getenv("PIPELINES") + "/sources/p2",
                                               inventory_path=os.getenv("PIPELINES"), 
                                               compute_pool_id=config.get('flink').get('compute_pool_id'), 
                                               dml_only=False, 
                                               may_start_descendants=False,
                                               execute_plan=True,
                                               force_ancestors=False)
        print(summary)
        assert execution_plan
        from_date = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        report = dm.report_running_flink_statements_for_a_product('p2', os.getenv("PIPELINES"), from_date)
        assert report
        print(report)
        
    def test_6_1_deploy_by_medals_int(self):
        """
        """
        os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
        config = get_config()
        
        summary, execution_plan = dm.build_and_deploy_all_from_directory( directory=os.getenv("PIPELINES") + "/intermediates/p2",
                                               inventory_path=os.getenv("PIPELINES"), 
                                               compute_pool_id=config.get('flink').get('compute_pool_id'), 
                                               dml_only=False, 
                                               may_start_descendants=False,
                                               execute_plan=True,
                                               force_ancestors=False)
        print(summary)
        assert summary



    def test_6_2_deploy_by_medals_fact(self):
        """
        """
        os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
        config = get_config()
        summary, execution_plan = dm.build_and_deploy_all_from_directory( directory=os.getenv("PIPELINES") + "/facts/p2",
                                               inventory_path=os.getenv("PIPELINES"), 
                                               compute_pool_id=config.get('flink').get('compute_pool_id'), 
                                               dml_only=False, 
                                               may_start_descendants=False,
                                               execute_plan=True,
                                               force_ancestors=False)
        print(summary)
        assert summary

    def test_9_report_running_flink_statements_for_all_from_product(self):
        os.environ["PIPELINES"]= str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
        from_date = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        result = dm.report_running_flink_statements_for_a_product('p1', os.getenv("PIPELINES"), from_date)
        assert result
        print(result)

    def test_10_prepare_tables_from_sql_file_integration(self):
        """
        Integration test for prepare_tables_from_sql_file function.
        Tests the end-to-end functionality of preparing tables from a SQL file.
        """
        print("Running integration test for prepare_tables_from_sql_file")
        
        # Setup paths
        test_sql_file = str(self.data_dir / "flink-project/pipelines/test_prepare_tables_integration.sql")
        
        # Get config for compute pool
        config = get_config()
        compute_pool_id = config.get('flink', {}).get('compute_pool_id')
        if not compute_pool_id:
            self.skipTest("No compute pool ID configured - skipping integration test")
        
        print(f"Using compute pool: {compute_pool_id}")
        print(f"Preparing tables from file: {test_sql_file}")
        
        # Track statements that get created so we can clean them up
        statement_prefix = f"prepare-table-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        created_statements = []
        
        try:
            # Get initial statement list to compare against later
            initial_statements = sm.get_statement_list()
            initial_count = len(initial_statements)
            print(f"Initial statement count: {initial_count}")
            
            # Execute the prepare function
            dm.prepare_tables_from_sql_file(test_sql_file, compute_pool_id=compute_pool_id)
            print("prepare_tables_from_sql_file completed successfully")
            
            # Verify that statements were created and processed
            # Note: The function should clean up statements after completion,
            # so we mainly verify the function executed without errors
            
            # Get final statement list
            final_statements = sm.get_statement_list()
            final_count = len(final_statements)
            print(f"Final statement count: {final_count}")
            
            # The function should have created and then deleted temporary statements,
            # so the final count should be the same as initial count
            self.assertEqual(final_count, initial_count, 
                           "Statement count changed - some prepare statements may not have been cleaned up")
            
            print("Integration test completed successfully")
            
        except Exception as e:
            print(f"Integration test failed with error: {e}")
            # Try to clean up any statements that might have been left behind
            try:
                current_statements = sm.get_statement_list()
                for stmt_name, stmt_info in current_statements.items():
                    if "prepare-table-" in stmt_name:
                        print(f"Cleaning up leftover statement: {stmt_name}")
                        try:
                            sm.delete_statement_if_exists(stmt_name)
                        except Exception as cleanup_error:
                            print(f"Failed to cleanup statement {stmt_name}: {cleanup_error}")
            except Exception as cleanup_error:
                print(f"Failed to perform cleanup: {cleanup_error}")
            
            # Re-raise the original exception
            raise e


if __name__ == '__main__':
    unittest.main()