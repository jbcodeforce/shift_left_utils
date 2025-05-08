"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
from datetime import datetime
from typing import Tuple

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

import shift_left.core.pipeline_mgr as pm
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.file_search import read_pipeline_definition_from_file
import shift_left.core.statement_mgr as sm
from shift_left.core.compute_pool_mgr import ComputePoolList, ComputePoolInfo
import shift_left.core.deployment_mgr as dm
from shift_left.core.models.flink_statement_model import (
    Statement, 
    StatementInfo
)
from shift_left.core.deployment_mgr import (
    FlinkStatementNode,
    FlinkStatementExecutionPlan
)
from shift_left.core.models.flink_statement_model import Statement, StatementInfo
from shift_left.core.utils.file_search import FlinkTablePipelineDefinition

class TestDeploymentManager(unittest.TestCase):
    """Test suite for the deployment manager functionality."""
    
    data_dir = None
    TEST_COMPUTE_POOL_ID = "test-pool-123"
    TEST_COMPUTE_POOL_ID_2 = "test-pool-120"
    TEST_COMPUTE_POOL_ID_3 = "test-pool-121"
    @classmethod
    def setUpClass(cls) -> None:
        """Set up test environment before running tests."""
        cls.data_dir = pathlib.Path(__file__).parent.parent.parent / "data"
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "dbt-project")
        os.environ["STAGING"] = str(cls.data_dir / "flink-project/staging")
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES"))

    def setUp(self) -> None:
        """Set up test case before each test."""
        self.config = get_config()
        self.compute_pool_id = self.TEST_COMPUTE_POOL_ID
        self.table_name = "test_table"
        self.inventory_path = os.getenv("PIPELINES")

    # Following set of methods are used to create reusable mock objects and functions
    def _create_mock_statement_info(
        self, 
        name: str = "statement_name",
        status_phase: str = "UNKNOWN",
        compute_pool_id: str = TEST_COMPUTE_POOL_ID
    ) -> StatementInfo:
        """Create a mock StatementInfo object."""
        return StatementInfo(
            name=name,
            status_phase=status_phase,
            compute_pool_id=compute_pool_id
        )

    def _create_mock_compute_pool_list(self, env_id: str = "test-env-123", region: str = "test-region-123") -> ComputePoolList:
        """Create a mock ComputePoolList object."""
        pool_1 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID,
            name="test-pool",
            env_id=env_id,
            max_cfu=100,
            current_cfu=50
        )
        pool_2 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_2,
            name="test-pool-2",
            env_id=env_id,
            max_cfu=100,
            current_cfu=50
        )
        pool_3 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_3,
            name="dev-p1-fct-order",
            env_id=env_id,
            max_cfu=10,
            current_cfu=0
        )
        return ComputePoolList(pools=[pool_1, pool_2, pool_3])

    def _create_mock_statement_node(
        self,
        table_name: str,
        product_name: str = "product1",
        dml_statement_name: str = "dml1",
        ddl_statement_name: str = "ddl1",
        compute_pool_id: str = TEST_COMPUTE_POOL_ID
    ) -> FlinkStatementNode:
        """Create a mock FlinkStatementNode object."""
        return FlinkStatementNode(
            table_name=table_name,
            product_name=product_name,
            dml_statement_name=dml_statement_name,
            ddl_statement_name=ddl_statement_name,
            compute_pool_id=compute_pool_id
        )

    def _mock_assign_compute_pool(self, node: FlinkStatementNode, compute_pool_id: str) -> FlinkStatementNode:
        """Mock function for assigning compute pool to node."""
        node.compute_pool_id = compute_pool_id
        node.compute_pool_name = "test-pool"
        return node

    def _mock_get_and_update_node(self, node: FlinkStatementNode) -> Statement:
        """Mock function for getting and updating node statement info."""
        node.existing_statement_info = self._create_mock_statement_info(
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_2
        )
        return node

    #  ----------- TESTS -----------
    def test_build_node_map(self) -> None:
        """Test building node map from pipeline definition.
        
        Loading a pipeline definition for an intermediate table should get all reachable 
        related tables. Direct descendants and ancestors should be included.
        """
        print("test_build node_map")
        pipeline_def = read_pipeline_definition_from_file(
            self.inventory_path + "/intermediates/p2/z/" + PIPELINE_JSON_FILE_NAME
        )
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

    def test_build_topological_sorted_parents(self) -> None:
        """Test building topologically sorted parents.
        
        f has 6 parents: d, then z, x, y then src_y, src_x.
        The topological sort should return src_y, src_x, y, x, z, d, f.
        """
        print("test_dfs_search_parents_to_run")
        pipeline_def = read_pipeline_definition_from_file(
            self.inventory_path + "/facts/p2/f/" + PIPELINE_JSON_FILE_NAME
        )
        node_map = dm._build_statement_node_map(pipeline_def.to_node())
        current_node = pipeline_def.to_node()
        nodes_to_run = dm._build_topological_sorted_parents(current_node, node_map)
        
        assert len(nodes_to_run) == 7
        for node in nodes_to_run:
            print(node.table_name, node.to_run, node.to_restart)
        assert nodes_to_run[0].table_name in ("src_y", "src_x")

    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def test_build_execution_plan_for_one_table_parent_running(
        self,
        mock_assign_compute_pool_id,
        mock_get_status
    ) -> None:
        """Test building execution plan when parent is running.
        
        Should lead to only parents to run when they are not running.
        F has one parent d. f -> Dd -> [y, z], z -> x, y-> src_y and x -> src_x.
        """
        print("test_build_execution_plan_for_one_table_parent_running")
        
        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name.startswith("dev-dml-d"):
                return self._create_mock_statement_info(status_phase="RUNNING")
            return self._create_mock_statement_info()
            
        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool

        pipeline_def = read_pipeline_definition_from_file(
            self.inventory_path + "/facts/p2/f/" + PIPELINE_JSON_FILE_NAME
        )
        execution_plan = dm.build_execution_plan_from_any_table(
            pipeline_def=pipeline_def, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID, 
            dml_only=False, 
            may_start_children=False, 
            force_sources=False,
            start_time=datetime.now()
        )
        
        print(dm.build_summary_from_execution_plan(execution_plan, self._create_mock_compute_pool_list()))
        assert len(execution_plan.nodes) == 7  # all nodes are present as we want to see running ones too
        assert execution_plan.nodes[6].table_name == "f"
        assert execution_plan.nodes[6].to_run is True
        assert execution_plan.nodes[6].to_restart is True

    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status')
    def test_build_execution_plan_for_one_table_parent_not_running(
        self,
        mock_get_status,
        mock_get_compute_pool_list,
        mock_assign_compute_pool_id
    ) -> None:
        """Test building execution plan when parent is not running.
        
        Start node is e. The parent c is not running so the execution plan includes c before fact e.
        c has B and Z as parents, Z is running but not B so the plan should start with src_b, b, e.
        """
        print("test_build_execution_plan_for_one_table_parent_not_running")
        
        def mock_status(statement_name: str) -> StatementInfo:
            if statement_name.startswith(("dev-p2-dml-z", "dev-p2-dml-x", "dev-p2-dml-y","dev-p2-dml-src-x", "dev-p2-dml-src-y")):
                return self._create_mock_statement_info(status_phase="RUNNING")
            return self._create_mock_statement_info()

        mock_get_status.side_effect = mock_status
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool

        pipeline_def = read_pipeline_definition_from_file(
            self.inventory_path + "/facts/p2/e/" + PIPELINE_JSON_FILE_NAME
        )
        execution_plan = dm.build_execution_plan_from_any_table(
            pipeline_def=pipeline_def, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID, 
            dml_only=False, 
            may_start_children=False, 
            force_sources=False,
            start_time=datetime.now()
        )  
        print(dm.build_summary_from_execution_plan(execution_plan, self._create_mock_compute_pool_list()))
        assert len(execution_plan.nodes) == 9
        for node in execution_plan.nodes:
            if node.table_name == "src_b":
                assert node.to_run is True
                assert node.to_restart is False
            if node.table_name == "b":
                assert node.to_run is True
                assert node.to_restart is False
            if node.table_name == "e":
                assert node.to_run is True
                assert node.to_restart is True
            if node.table_name == "c":
                assert node.to_run is True
                assert node.to_restart is False


  

    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status')
    def test_build_execution_plan_with_all_src_running(
        self,
        mock_get_status,
        mock_get_compute_pool_list,
        mock_assign_compute_pool_id
    ) -> None:
        """Test building execution plan with all sources running.
        
        From a leaf like f start parents up to the running sources. All sources are already running.
        Plan should have: y,x,z,d,f.
        """
        print("test_build_execution_plan_with_all_src_running")
        
        def mock_status(statement_name: str) -> StatementInfo:
            if statement_name.startswith((
                "dev-p2-dml-src-y",
                "dev-p2-dml-src-x",
                "dev-p2-dml-src-p2-a",
                "dev-p2-dml-src-b"
            )):
                return self._create_mock_statement_info(status_phase="RUNNING")  
            return self._create_mock_statement_info()

        mock_get_status.side_effect = mock_status
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool

        pipeline_def = read_pipeline_definition_from_file(
            self.inventory_path + "/facts/p2/f/" + PIPELINE_JSON_FILE_NAME
        )
        execution_plan = dm.build_execution_plan_from_any_table(
            pipeline_def=pipeline_def, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID, 
            dml_only=False, 
            may_start_children=False, 
            force_sources=False,
            start_time=datetime.now()
        )  
        
        print(dm.build_summary_from_execution_plan(execution_plan, self._create_mock_compute_pool_list()))
        dm.persist_execution_plan(execution_plan)
        # 8 as the 3 children of Z are not running   
        assert len(execution_plan.nodes) == 7

        assert execution_plan.nodes[2].table_name in ("y", "x")
        assert execution_plan.nodes[2].to_run is True
        assert execution_plan.nodes[2].to_restart is False
        assert execution_plan.nodes[3].table_name in ("z", "x", "y")
        assert execution_plan.nodes[3].to_run is True
        assert execution_plan.nodes[3].to_restart is False
        assert execution_plan.nodes[4].table_name in ("z", "d")
        assert execution_plan.nodes[4].to_restart is False
        assert execution_plan.nodes[6].table_name == "f"
        assert execution_plan.nodes[6].to_run is True
        assert execution_plan.nodes[6].to_restart is True

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status')
    def test_build_execution_plan_for_intermediate_table_including_children(
        self,
        mock_get_status, 
        mock_assign_compute_pool_id,
        mock_get_compute_pool_list
    ) -> None:
        """Test building execution plan for intermediate table including children.
        
        From an intermediate like z, look for parents, x,y up to running sources src_y, src_x that are not already running.
        Plan should have: src_x, src_y, x, y, z, d, src_b, b, c, f and e.
        """
        print("test_build_execution_plan_for_intermediate_table_including_children")
        
        def mock_status(statement_name: str) -> StatementInfo:
            print(f"mock_status: {statement_name}")
            return self._create_mock_statement_info()
            
        mock_get_status.side_effect = mock_status
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list

        pipeline_def = read_pipeline_definition_from_file(
            self.inventory_path + "/intermediates/p2/z/" + PIPELINE_JSON_FILE_NAME
        )
        execution_plan = dm.build_execution_plan_from_any_table(
            pipeline_def=pipeline_def, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID, 
            dml_only=False, 
            may_start_children=True, 
            force_sources=False,
            start_time=datetime.now()
        )  
        
        print(dm.build_summary_from_execution_plan(execution_plan, self._create_mock_compute_pool_list()))
        assert len(execution_plan.nodes) == 12
        for node in execution_plan.nodes:
            print(node.table_name, node.to_run, node.to_restart)
            if node.table_name in ['src_y', 'src_x', 'x', 'y', 'z']:
                assert node.to_run is True
            if node.table_name in ['p', 'd', 'c', 'z']:
                assert node.to_restart is True



    @patch('shift_left.core.deployment_mgr._deploy_ddl_dml')
    @patch('shift_left.core.deployment_mgr._deploy_dml')
    @patch('shift_left.core.deployment_mgr._get_statement_status')
    def _test_deploy_pipeline_from_table_success(
        self,
        mock_get_status,
        mock_deploy_dml,
        mock_deploy_ddl_dml
    ) -> None:
        """Test successful pipeline deployment from table."""
        print("test_deploy_pipeline_from_table_success")
        table_name = "z"
        
        def mock_status(statement_name: str) -> StatementInfo:
            if statement_name.startswith("dev-dml-z"):
                return self._create_mock_statement_info(status_phase="RUNNING")
            return self._create_mock_statement_info()
            
        mock_get_status.side_effect = mock_status
        
        mock_statement = MagicMock(spec=Statement)
        mock_deploy_ddl_dml.return_value = mock_statement
        mock_deploy_dml.return_value = mock_statement

        # Execute
        result = dm.deploy_pipeline_from_table(
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
    def _test_full_pipeline_undeploy_from_table_success(
        self,
        mock_drop,
        mock_delete,
        mock_read,
        mock_get_ref,
        mock_inventory
    ) -> None:
        """Test successful pipeline undeployment."""
        print("test_full_pipeline_undeploy_from_table_success")
        # Setup mocks
        mock_table_ref = MagicMock()
        mock_get_ref.return_value = mock_table_ref
        
        mock_pipeline = MagicMock(spec=FlinkTablePipelineDefinition)
        mock_pipeline.children = set()  # No children means it's a sink table
        mock_read.return_value = mock_pipeline
        
        # Execute
        result = dm.full_pipeline_undeploy_from_table(
            table_name=self.table_name,
            inventory_path=self.inventory_path
        )

        # Verify
        self.assertTrue(result.startswith(f"{self.table_name} deleted"))
        mock_delete.assert_called()
        mock_drop.assert_called_with(self.table_name, self.config['flink']['compute_pool_id'])

    @patch('shift_left.core.deployment_mgr.get_or_build_inventory')
    @patch('shift_left.core.deployment_mgr.get_table_ref_from_inventory')
    def _test_full_pipeline_undeploy_from_table_not_found(
        self,
        mock_get_ref,
        mock_inventory
    ) -> None:
        """Test undeployment when table is not found."""
        print("test_full_pipeline_undeploy_from_table_not_found")
        # Setup mocks
        mock_get_ref.return_value = None

        # Execute
        result = dm.full_pipeline_undeploy_from_table(
            table_name=self.table_name,
            inventory_path=self.inventory_path
        )

        # Verify
        self.assertEqual(
            result,
            f"ERROR: Table {self.table_name} not found in table inventory"
        )


    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    def test_build_table_graph_for_node(self, mock_read) -> None:
        """Test building table graph for a node."""
        print("test_build_table_graph_for_node")
        # Setup
        start_node = self._create_mock_statement_node("start_table")

        mock_pipeline = MagicMock(spec=FlinkTablePipelineDefinition)
        mock_read.return_value = mock_pipeline

        # Execute
        result = dm._build_statement_node_map(start_node)

        # Verify
        self.assertIsInstance(result, dict)
        self.assertIn(start_node.table_name, result)
        self.assertEqual(result[start_node.table_name], start_node)

    @patch('shift_left.core.deployment_mgr._deploy_ddl_dml')
    @patch('shift_left.core.deployment_mgr._deploy_dml')
    def test_execute_plan(self, mock_deploy_dml, mock_deploy_ddl_dml) -> None:
        """Test execution of a plan."""
        print("test_execute_plan")
        # Setup
        plan = FlinkStatementExecutionPlan(nodes=[])
        node1 = self._create_mock_statement_node("table1")
        node1.to_run = True
        node1.dml_only = False
        
        node2 = self._create_mock_statement_node("table2")
        node2.to_run = False
        node2.to_restart = True
        node2.dml_only = True
        plan.nodes = [node1, node2]

        mock_statement = MagicMock(spec=Statement)
        mock_deploy_ddl_dml.return_value = mock_statement
        mock_deploy_dml.return_value = mock_statement

        # Execute
        result = dm._execute_plan(plan, self.compute_pool_id)

        # Verify
        self.assertEqual(len(result), 2)
        mock_deploy_ddl_dml.assert_called_once()
        mock_deploy_dml.assert_called_once()

    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status')
    def test_execution_plan_with_src_running(
        self, 
        mock_get_status, 
        mock_get_compute_pool_list, 
        mock_assign_compute_pool_id
    ) -> None:
        """Test execution plan with source tables running."""
        print("test_execution_plan_with_src_running")
        
        def mock_status(statement_name: str) -> StatementInfo:
            print(f"mock_status: {statement_name}")
            if statement_name.startswith("dev-dml-src-") or statement_name.startswith("dev-p1-dml-src-"):
                return self._create_mock_statement_info(status_phase="RUNNING")
            return self._create_mock_statement_info()
            
        mock_get_status.side_effect = mock_status
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        inventory_path = os.getenv("PIPELINES")
        pipeline_def = read_pipeline_definition_from_file(
            inventory_path + "/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME
        )
        execution_plan = dm.build_execution_plan_from_any_table(
            pipeline_def=pipeline_def, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID, 
            dml_only=False, 
            may_start_children=False, 
            force_sources=False,
            start_time=datetime.now()
        )
        print(dm.build_summary_from_execution_plan(execution_plan, self._create_mock_compute_pool_list()))
        
        # Verify source tables are in the execution plan with skip action
        for node in execution_plan.nodes:
            if node.table_name.startswith("src_"):
                assert not node.to_restart and not node.to_run, \
                    f"Source table {node.table_name} should not be in execution plan"
            
        # Verify the expected intermediate and fact tables are present
        assert len(execution_plan.nodes) == 6
        assert execution_plan.nodes[3].table_name in ("int_p1_table_1", "int_p1_table_2")
        assert execution_plan.nodes[4].table_name in ("int_p1_table_1", "int_p1_table_2")
        assert execution_plan.nodes[5].table_name == "p1_fct_order"

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.create_compute_pool')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    def test_create_compute_pool_for_node(self, mock_get_compute_pool_list, mock_create_compute_pool):
        """Test assigning compute pool id to a node."""
        print("test_assign_compute_pool_id_to_node")

        def mock_create_compute_pool(table_name: str) -> Tuple[str, str]:
            return "test-pool-123", "test-pool-name"
        # Setup
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_create_compute_pool.side_effect = mock_create_compute_pool
        node = self._create_mock_statement_node("table1")
        inventory_path = os.getenv("PIPELINES")
        table_name="p1_fct_order"
        pipeline_def=  pm.get_pipeline_definition_for_table(table_name, inventory_path)
        node = pipeline_def.to_node()
        node = dm._assign_compute_pool_id_to_node(node, None)
        assert node.compute_pool_id == "test-pool-121"

if __name__ == '__main__':
    unittest.main()