"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
from datetime import datetime
import time
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
import shift_left.core.pipeline_mgr as pm
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from  shift_left.core.statement_mgr import *
import shift_left.core.deployment_mgr as dm
from shift_left.core.deployment_mgr import (
    deploy_pipeline_from_table,
    full_pipeline_undeploy_from_table,
    build_execution_plan_from_any_table,
        FlinkStatementNode,
    FlinkStatementExecutionPlan,
    _build_statement_node_map,
    _execute_plan
)
from shift_left.core.flink_statement_model import Statement, StatementInfo
from shift_left.core.utils.file_search import (
    FlinkTablePipelineDefinition,
)

class TestDeploymentManager(unittest.TestCase):
    
    data_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent.parent / "./data"  # Path to the data directory
        os.environ["PIPELINES"] = str(cls.data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "dbt-project")
        os.environ["STAGING"] = str(cls.data_dir / "flink-project/staging")
        #pm.build_all_pipeline_definitions(os.getenv("PIPELINES"))

    def setUp(self):
        self.config = get_config()
        self.compute_pool_id = "test-pool-123"
        self.table_name = "test_table"
        self.inventory_path = os.getenv("PIPELINES")

    def test_build_node_map(self):
        """Test building node map"""
        print("test_build_node_map")
        pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(self.inventory_path + "/intermediates/p2/z/" + PIPELINE_JSON_FILE_NAME)
        node_map = dm._build_statement_node_map(pipeline_def.to_node())
        assert len(node_map) == 14
        for node in node_map.values():
            print(node.table_name, node.upgrade_mode, node.dml_statement_name)
        assert node_map["src_y"].upgrade_mode == "Stateless"
        assert node_map["src_x"].upgrade_mode == "Stateless"
        assert node_map["src_b"].upgrade_mode == "Stateless"
        assert node_map["src_p2_a"].upgrade_mode == "Stateless"
        assert node_map["x"].upgrade_mode == "Stateless"
        assert node_map["y"].upgrade_mode == "Stateless"
        assert node_map["z"].upgrade_mode == "Stateful"
        assert node_map["d"].upgrade_mode == "Stateful"
        assert node_map["c"].upgrade_mode == "Stateless"
        assert node_map["p"].upgrade_mode == "Stateless"
        assert node_map["a"].upgrade_mode == "Stateful"
        assert node_map["b"].upgrade_mode == "Stateless"
        assert node_map["e"].upgrade_mode == "Stateless"
        assert node_map["f"].upgrade_mode == "Stateless"
        

    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status')
    def test_build_execution_plan_for_one_table_parent_running(self, mock_get_status): 
        """
        Should lead to only parents to run when they are not running.
        F has one parent D.
        """
        print("test_build_execution_plan_for_one_table_parent_running")
        ## The parent D is running so only the fact F should be run.
        def mock_status(statement_name):
            if statement_name.startswith("dev-dml-d"):
                return StatementInfo(status_phase="RUNNING", compute_pool_id="test-pool-123")
            else:
                return StatementInfo(status_phase="UNKNOWN", compute_pool_id=None)
        mock_get_status.side_effect = mock_status
        pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(self.inventory_path + "/facts/p2/f/" + PIPELINE_JSON_FILE_NAME)
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def, 
                                                                get_config()['flink']['compute_pool_id'], 
                                                                False, 
                                                                False, datetime.now())
        print(dm.build_summary_from_execution_plan(execution_plan))
        assert len(execution_plan.nodes) == 1
        assert execution_plan.nodes[0].table_name == "f"
        assert execution_plan.nodes[0].to_run == True
        assert execution_plan.nodes[0].to_restart == False
        

    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status')
    def test_build_execution_plan_for_one_table_parent_not_running(self, mock_get_status): 
        """
        start node is f
        The parent D is not running so the execution plan includes D before fact F.
        D has Y and Z as parents, Z is runnning but not Y
        so the plan will be y,d,f
        """
        print("test_build_execution_plan_for_one_table_parent_not_running")
        def mock_status(statement_name):
            if statement_name.startswith("dev-dml-d") or statement_name.startswith("dev-dml-y") or statement_name.startswith("dev-dml-f"):
                return StatementInfo(status_phase="UNKNOWN", compute_pool_id=None)  
            if  statement_name.startswith("dev-dml-z"):
                return StatementInfo(status_phase="RUNNING", compute_pool_id="test-pool-123")

        mock_get_status.side_effect = mock_status
        pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(self.inventory_path + "/facts/p2/f/" + PIPELINE_JSON_FILE_NAME)
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def, 
                                                                get_config()['flink']['compute_pool_id'], 
                                                                False, 
                                                                False, datetime.now())  
        print(dm.build_summary_from_execution_plan(execution_plan))   
        assert len(execution_plan.nodes) == 3

        assert execution_plan.nodes[0].table_name == "y"
        assert execution_plan.nodes[0].to_run == True
        assert execution_plan.nodes[0].to_restart == False
        assert execution_plan.nodes[1].table_name == "d"
        assert execution_plan.nodes[1].to_run == True
        assert execution_plan.nodes[1].to_restart == True
        assert execution_plan.nodes[2].table_name == "f"
        assert execution_plan.nodes[2].to_run == True
        assert execution_plan.nodes[2].to_restart == True
          
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status')
    def test_build_execution_plan_with_all_src_running(self, mock_get_status): 
        """
        From a leaf like f start parents up to running sources that are already running.
        plan should have: y,x,z,d,f
        """
        print("test_build_execution_plan_with_all_src_running")
        def mock_status(statement_name):
            if (statement_name.startswith("dev-dml-src-y") or 
                statement_name.startswith("dev-dml-src-x") or 
                statement_name.startswith("dev-dml-src-p2-a") or 
                statement_name.startswith("dev-dml-src-b")):
                return StatementInfo(status_phase="RUNNING", compute_pool_id="test-pool-123")  
            else:
                return StatementInfo(status_phase="UNKNOWN", compute_pool_id=None)

        mock_get_status.side_effect = mock_status
        pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(self.inventory_path + "/facts/p2/f/" + PIPELINE_JSON_FILE_NAME)
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def, 
                                                                get_config()['flink']['compute_pool_id'], 
                                                                False, 
                                                                False, datetime.now())  
        print(dm.build_summary_from_execution_plan(execution_plan))
        dm.persist_execution_plan(execution_plan)
        # 8 as the 3 children of Z are not running   
        assert len(execution_plan.nodes) == 8

        assert execution_plan.nodes[0].table_name == "y" or execution_plan.nodes[0].table_name == "x"
        assert execution_plan.nodes[0].to_run == True
        assert execution_plan.nodes[0].to_restart == False
        assert execution_plan.nodes[1].table_name == "z" or execution_plan.nodes[1].table_name == "x" or execution_plan.nodes[1].table_name == "y"
        assert execution_plan.nodes[1].to_run == True
        assert execution_plan.nodes[1].to_restart == False
        assert execution_plan.nodes[2].table_name == "z"  or execution_plan.nodes[1].table_name == "d" 
        print(execution_plan.nodes[2].table_name, execution_plan.nodes[2].to_run, execution_plan.nodes[2].to_restart)
        assert execution_plan.nodes[2].to_run == True
        assert execution_plan.nodes[2].to_restart ==  False  


    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status')
    def test_build_execution_plan_with_all_src_running_and_z_children_running(self, mock_get_status): 
        """
        From a leaf like f start parents up to running sources that are already running.
        plan should have: y,x,z,d,f
        """
        print("test_build_execution_plan_with_all_src_running_and_z_children_running")
        def mock_status(statement_name):
            if (statement_name.startswith("dev-dml-src-y") or 
                statement_name.startswith("dev-dml-src-x") or 
                statement_name.startswith("dev-dml-src-a") or 
                statement_name.startswith("dev-dml-src-b") or
                statement_name.startswith("dev-dml-p") or 
                statement_name.startswith("dev-dml-c")):
                return StatementInfo(status_phase="RUNNING", compute_pool_id="test-pool-123")  
            else:
                return StatementInfo(status_phase="UNKNOWN", compute_pool_id=None)

        mock_get_status.side_effect = mock_status
        pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(self.inventory_path + "/facts/p2/f/" + PIPELINE_JSON_FILE_NAME)
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def, 
                                                                get_config()['flink']['compute_pool_id'], 
                                                                False, 
                                                                False, datetime.now())  
        print(dm.build_summary_from_execution_plan(execution_plan))
        # children of Z 
        assert len(execution_plan.nodes) == 5

        assert execution_plan.nodes[0].table_name == "y" or execution_plan.nodes[0].table_name == "x"
        assert execution_plan.nodes[0].to_run == True
        assert execution_plan.nodes[0].to_restart == False
        assert execution_plan.nodes[1].table_name == "z" or execution_plan.nodes[1].table_name == "x" or execution_plan.nodes[1].table_name == "y"
        assert execution_plan.nodes[1].to_run == True
        assert execution_plan.nodes[1].to_restart == False
        assert execution_plan.nodes[2].table_name == "z"  or execution_plan.nodes[1].table_name == "d" 
        assert execution_plan.nodes[2].to_run == True
        assert execution_plan.nodes[2].to_restart == False


    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status')
    def test_build_execution_plan_for_intermediated_table(self, mock_get_status): 
        """
        From an intermediate like z, start parents up to running sources that are not already running.
        plan should have: src_y, src_x, x, ,y, z, D. As force childen is false only stateful children will be restarted.
        """
        print("test_build_execution_plan_for_intermediated_table")
        def mock_status(statement_name):
            if (statement_name.startswith("dev-dml-p") or 
                statement_name.startswith("dev-dml-c")):
                return StatementInfo(status_phase="RUNNING", compute_pool_id="test-pool-123")  
            else:
                return StatementInfo(status_phase="UNKNOWN", compute_pool_id=None)

        mock_get_status.side_effect = mock_status
        pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(self.inventory_path + "/intermediates/p2/z/" + PIPELINE_JSON_FILE_NAME)
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def, 
                                                                get_config()['flink']['compute_pool_id'], 
                                                                False, 
                                                                False, 
                                                                datetime.now())  
        print(dm.build_summary_from_execution_plan(execution_plan))
        # children of Z are running except D
        assert len(execution_plan.nodes) == 6

        assert execution_plan.nodes[0].table_name == "src_y" or execution_plan.nodes[0].table_name == "src_x"
        assert execution_plan.nodes[0].to_run == True
        assert execution_plan.nodes[0].to_restart == False
        assert (execution_plan.nodes[1].table_name == "z" 
                or execution_plan.nodes[1].table_name == "x" 
                or execution_plan.nodes[1].table_name == "y"
                or execution_plan.nodes[1].table_name == "src_y"
                or execution_plan.nodes[1].table_name == "src_x")
        error = False
        for node in execution_plan.nodes:
            if node.table_name == "c" or node.table_name == "p":
                error = True
        assert error == False
        




    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status')
    def test_build_execution_plan_for_intermediate_table_including_children(self, mock_get_status):
        """
        From an intermediate like z, start parents up to running sources that are not already running.
        plan should have: src_x, x, src_a, a, y, z, d, src_b, b. f and e are stateless so started differently.  
        """
        print("test_build_execution_plan_for_intermediate_table_including_children")
        def mock_status(statement_name):
            if (statement_name.startswith("dev-dml-src-y") or 
                statement_name.startswith("dev-dml-p")):
                return StatementInfo(status_phase="RUNNING", compute_pool_id="test-pool-123")  
            else:
                return StatementInfo(status_phase="UNKNOWN", compute_pool_id=None)

        mock_get_status.side_effect = mock_status
        pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(self.inventory_path + "/intermediates/p2/z/" + PIPELINE_JSON_FILE_NAME)
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def, 
                                                                get_config()['flink']['compute_pool_id'], 
                                                                False, 
                                                                True, 
                                                                datetime.now())  
        print(dm.build_summary_from_execution_plan(execution_plan))
        assert len(execution_plan.nodes) == 10 


    @patch('shift_left.core.deployment_mgr._deploy_ddl_dml')
    @patch('shift_left.core.deployment_mgr._deploy_dml')
    @patch('shift_left.core.deployment_mgr._get_statement_status')
    def _test_deploy_pipeline_from_table_success(self, mock_get_status, mock_deploy_dml, mock_deploy_ddl_dml):
        print("test_deploy_pipeline_from_table_success")
        table_name = "z"
        def mock_status(statement_name):
            if statement_name.startswith("dev-dml-z"):
                return StatementInfo(status_phase="RUNNING", compute_pool_id="test-pool-123")
            else:
                return StatementInfo(status_phase="UNKNOWN", compute_pool_id=None)
        mock_get_status.side_effect = mock_status
        
        mock_statement = MagicMock(spec=Statement)
        mock_deploy_ddl_dml.return_value = mock_statement
        mock_deploy_dml.return_value = mock_statement

        # Execute
        result = deploy_pipeline_from_table(
            table_name=table_name,
            inventory_path=self.inventory_path,
            compute_pool_id=self.compute_pool_id
        )

        # Verify
        self.assertEqual(result.table_name, self.table_name)
        self.assertEqual(result.compute_pool_id, self.compute_pool_id)


    @patch('shift_left.core.deployment_mgr.get_or_build_inventory')
    @patch('shift_left.core.deployment_mgr.get_table_ref_from_inventory')
    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    @patch('shift_left.core.deployment_mgr.delete_statement_if_exists')
    @patch('shift_left.core.deployment_mgr.drop_table')
    def _test_full_pipeline_undeploy_from_table_success(self, mock_drop, mock_delete, mock_read, mock_get_ref, mock_inventory):
        """Test successful pipeline undeployment"""
        print("test_full_pipeline_undeploy_from_table_success")
        # Setup mocks
        mock_table_ref = MagicMock()
        mock_get_ref.return_value = mock_table_ref
        
        mock_pipeline = MagicMock(spec=FlinkTablePipelineDefinition)
        mock_pipeline.children = set()  # No children means it's a sink table
        mock_read.return_value = mock_pipeline
        
        # Execute
        result = full_pipeline_undeploy_from_table(
            table_name=self.table_name,
            inventory_path=self.inventory_path
        )

        # Verify
        self.assertTrue(result.startswith(f"{self.table_name} deleted"))
        mock_delete.assert_called()
        mock_drop.assert_called_with(self.table_name, self.config['flink']['compute_pool_id'])




    @patch('shift_left.core.deployment_mgr.get_or_build_inventory')
    @patch('shift_left.core.deployment_mgr.get_table_ref_from_inventory')
    def _test_full_pipeline_undeploy_from_table_not_found(self, mock_get_ref, mock_inventory):
        """Test undeployment when table is not found"""
        print("test_full_pipeline_undeploy_from_table_not_found")
        # Setup mocks
        mock_get_ref.return_value = None

        # Execute
        result = full_pipeline_undeploy_from_table(
            table_name=self.table_name,
            inventory_path=self.inventory_path
        )

        # Verify
        self.assertEqual(result, f"ERROR: Table {self.table_name} not found in table inventory")



    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    def test_build_table_graph_for_node(self, mock_read):
        """Test building table graph for a node"""
        print("test_build_table_graph_for_node")
        # Setup
        start_node = MagicMock(spec=FlinkStatementNode)
        start_node.table_name = "start_table"
        start_node.parents = []
        start_node.children = []

        mock_pipeline = MagicMock(spec=FlinkTablePipelineDefinition)
        mock_read.return_value = mock_pipeline

        # Execute
        result = _build_statement_node_map(start_node)

        # Verify
        self.assertIsInstance(result, dict)
        self.assertIn(start_node.table_name, result)
        self.assertEqual(result[start_node.table_name], start_node)



    @patch('shift_left.core.deployment_mgr._deploy_ddl_dml')
    @patch('shift_left.core.deployment_mgr._deploy_dml')
    def test_execute_plan(self, mock_deploy_dml, mock_deploy_ddl_dml):
        """Test execution of a plan"""
        print("test_execute_plan")
        # Setup
        plan = FlinkStatementExecutionPlan(nodes=[])
        node1 = FlinkStatementNode(table_name="table1", product_name="product1", dml_statement_name="dml1", ddl_statement_name="ddl1", compute_pool_id="cp001")
        node1.to_run = True
        node1.dml_only = False
        node2 = FlinkStatementNode(table_name="table2", product_name="product1", dml_statement_name="dml2", ddl_statement_name="ddl2", compute_pool_id="cp001")
        node2.to_run = False
        node2.to_restart = True
        node2.dml_only = True
        plan.nodes = [node1, node2]

        mock_statement = MagicMock(spec=Statement)
        mock_deploy_ddl_dml.return_value = mock_statement
        mock_deploy_dml.return_value = mock_statement

        # Execute
        result = _execute_plan(plan, self.compute_pool_id)

        # Verify
        self.assertEqual(len(result), 2)
        mock_deploy_ddl_dml.assert_called_once()
        mock_deploy_dml.assert_called_once()

      

    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status')
    def test_execution_plan_with_src_running(self, mock_get_status):
        print("test_execution_plan_with_src_running")
        #statement_mgr_instance = MockStatementMgr.return_value
        
        def mock_status(statement_name):
            print(f"mock_status: {statement_name}")
            mock_info = MagicMock(spec=StatementInfo)
            if statement_name.startswith("dev-dml-src-"):
                mock_info.name = statement_name
                mock_info.return_value.status_phase.return_value = "RUNNING"
                mock_info.return_value.compute_pool_id.return_value = "test-pool-123"
                return mock_info
            else:
                return StatementInfo(name=statement_name, status_phase="UNKNOWN", compute_pool_id=None)
        mock_get_status.side_effect = mock_status
        #statement_mgr_instance.get_statement_status.side_effect = mock_status

        inventory_path = os.getenv("PIPELINES")
        pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(inventory_path + "/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME)
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def, get_config()['flink']['compute_pool_id'], False, True, datetime.now())
        print(dm.build_summary_from_execution_plan(execution_plan))
        
        # Verify source tables are not in the execution plan
        for node in execution_plan.nodes:
            assert not node.table_name.startswith("src_"), f"Source table {node.table_name} should not be in execution plan"
            
        # Verify the expected intermediate and fact tables are present
        assert len(execution_plan.nodes) == 3
        assert execution_plan.nodes[0].table_name == "int_p1_table_1" or execution_plan.nodes[0].table_name == "int_p1_table_2"
        assert execution_plan.nodes[1].table_name == "int_p1_table_1" or execution_plan.nodes[1].table_name == "int_p1_table_2"
        assert execution_plan.nodes[2].table_name == "p1_fct_order"

    

if __name__ == '__main__':
    unittest.main()