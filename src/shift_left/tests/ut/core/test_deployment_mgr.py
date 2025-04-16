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
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "../data/flink-project/pipelines")
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
    _build_statement_node_map,
    _execute_plan,
    _deploy_ddl_dml,
    _deploy_dml,
    _delete_not_shared_parent
)
from shift_left.core.flink_statement_model import Statement, StatementInfo, StatementResult
from shift_left.core.utils.file_search import (
    FlinkTablePipelineDefinition,
    FlinkStatementNode,
    FlinkStatementExecutionPlan
)

class TestDeploymentManager(unittest.TestCase):
    
    data_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent.parent / "./data"  # Path to the data directory
        os.environ["PIPELINES"] = str(cls.data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "dbt-project")
        os.environ["STAGING"] = str(cls.data_dir / "flink-project/staging")
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES"))

    def setUp(self):
        self.config = get_config()
        self.compute_pool_id = "test-pool-123"
        self.table_name = "test_table"
        self.inventory_path = os.getenv("PIPELINES")

    @patch('shift_left.core.deployment_mgr._deploy_ddl_dml')
    @patch('shift_left.core.deployment_mgr._deploy_dml')
    @patch('shift_left.core.deployment_mgr._get_statement_status')
    def test_deploy_pipeline_from_table_success(self, mock_get_status, mock_deploy_dml, mock_deploy_ddl_dml):
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
    def test_full_pipeline_undeploy_from_table_success(self, mock_drop, mock_delete, mock_read, mock_get_ref, mock_inventory):
        """Test successful pipeline undeployment"""
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
    def test_full_pipeline_undeploy_from_table_not_found(self, mock_get_ref, mock_inventory):
        """Test undeployment when table is not found"""
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
        # Setup
        plan = MagicMock(spec=FlinkStatementExecutionPlan)
        node1 = MagicMock(spec=FlinkStatementNode)
        node1.to_run = True
        node1.dml_only = False
        node1.compute_pool_id = "cp001"   
        node1.table_name = "table1"
        node1.dml_statement = "dml1"
        node1.ddl_statement = "ddl1"
        node2 = MagicMock(spec=FlinkStatementNode)
        node2.to_run = False
        node2.to_restart = True
        node2.dml_only = True
        node2.compute_pool_id = "cp001"   
        node2.table_name = "table2"
        node2.dml_statement = "dml2"
        node2.ddl_statement = "ddl2"
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


    @patch('shift_left.core.deployment_mgr._build_statement_node_map')
    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    @patch('shift_left.core.deployment_mgr._get_statement_status')
    def test_execution_plan_with_completed_statement(self, mock_get_status, mock_read, mock_build_statement_node_map):
        """Test how completed statements affect the execution plan"""
        # Setup mock current node
        current_node = MagicMock(spec=FlinkStatementNode)
        current_node.table_name = "test_table"
        current_node.to_run = False
        current_node.compute_pool_id = "test-pool-123"
        current_node.update_children = True
        current_node.dml_statement = "current_dml"
        current_node.dml_only = False
        current_node.parents = set()
        current_node.children = set()
        # Setup mock pipeline definition
        mock_pipeline = MagicMock(spec=FlinkTablePipelineDefinition)
        mock_pipeline.table_name = "test_table"
        mock_pipeline.to_node.return_value = current_node
        mock_read.return_value = mock_pipeline

        # Setup mock nodes with parent-child relationship
        parent_node = MagicMock(spec=FlinkStatementNode)
        parent_node.table_name = "parent_table"
        parent_node.parents = set()
        parent_node.children = set()
        parent_node.to_run = False
        parent_node.children.add(current_node)
        parent_node.dml_statement = "parent_dml"
        parent_node.existing_statement_info = MagicMock()
        parent_node.existing_statement_info.status_phase = "RUNNING"
        parent_node.existing_statement_info.compute_pool_id = "test-pool-123"
        parent_node.is_running.return_value = True

        current_node.parents.add(parent_node)

        child_node = MagicMock(spec=FlinkStatementNode)
        child_node.table_name = "child_table"
        child_node.parents = set()
        child_node.parents.add(current_node)
        child_node.children = set()
        child_node.to_run = False
        child_node.dml_statement = "child_dml"
        child_node.existing_statement_info = MagicMock()
        child_node.existing_statement_info.status_phase = "UNKNOWN"
        child_node.existing_statement_info.compute_pool_id = "test-pool-123"
        child_node.is_running.return_value = False
        current_node.children.add(child_node)

        # Setup node map
        node_map = {
            parent_node.table_name: parent_node,
            child_node.table_name: child_node,
            current_node.table_name: current_node
        }
        mock_build_statement_node_map.return_value=node_map

        # Mock statement status responses
        def mock_status(statement_name):
            if statement_name == "parent_dml":
                return MagicMock(status_phase="RUNNING", compute_pool_id="test-pool-123")
            elif statement_name == "child_dml":
                return MagicMock(status_phase="UNKNOWN", compute_pool_id="test-pool-123")
            return MagicMock(status_phase="UNKNOWN")
        mock_get_status.side_effect = mock_status

        # Execute
        execution_plan = dm.build_execution_plan_from_any_table(
            pipeline_def=mock_pipeline,
            compute_pool_id="test-pool-123",
            dml_only=False,
            force_children=False
        )

        # Verify
        # Parent should not be marked to run since it's running
        self.assertFalse(parent_node.to_run)
        # Child should be marked to restart
        self.assertTrue(child_node.to_restart)
        # Verify the execution plan contains both nodes
        self.assertEqual(len(execution_plan.nodes), 2)
        # Verify parent is before child in execution order
        self.assertEqual(execution_plan.nodes[0].table_name, current_node.table_name)
        self.assertEqual(execution_plan.nodes[1].table_name, child_node.table_name)
      

    @patch('shift_left.core.deployment_mgr._get_statement_status')
    def test_execution_plan_with_src_running(self, mock_get_status):

        def mock_status(statement_name):
            if statement_name.startswith("dev-p1-dml-src-") or statement_name.startswith("dev-dml-src-"):
                return StatementInfo(status_phase="RUNNING", compute_pool_id="test-pool-123")
            else:
                return StatementInfo(status_phase="UNKNOWN", compute_pool_id=None)
        mock_get_status.side_effect = mock_status

        inventory_path= os.getenv("PIPELINES")
        pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(inventory_path + "/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME)
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def, get_config()['flink']['compute_pool_id'], False, True, datetime.now())
        for node in execution_plan.nodes:
            print(f"{node}\n\n\n")
        assert execution_plan.nodes[0].table_name == "int_table_1" or execution_plan.nodes[0].table_name == "int_table_2"
        assert execution_plan.nodes[1].table_name == "int_table_1" or execution_plan.nodes[1].table_name == "int_table_2"
        assert execution_plan.nodes[2].table_name == "fct_order"

    

if __name__ == '__main__':
    unittest.main()