"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
from datetime import datetime
import uuid
from typing import Tuple

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

import shift_left.core.pipeline_mgr as pm
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.file_search import read_pipeline_definition_from_file
from shift_left.core.compute_pool_mgr import ComputePoolList, ComputePoolInfo
import shift_left.core.deployment_mgr as dm
import shift_left.core.utils.report_mgr as report_mgr
from shift_left.core.models.flink_statement_model import (
    Statement, 
    StatementInfo,
    Status,
    Spec,
    Metadata
)
from shift_left.core.deployment_mgr import (
    FlinkStatementNode,
    FlinkStatementExecutionPlan
)
from shift_left.core.utils.report_mgr import DeploymentReport, StatementBasicInfo
from shift_left.core.models.flink_statement_model import Statement, StatementInfo
from shift_left.core.utils.file_search import FlinkTablePipelineDefinition

class TestDeploymentManager(unittest.TestCase):
    """Test suite for the deployment manager functionality."""
    
    TEST_COMPUTE_POOL_ID_1 = "test-pool-123"
    TEST_COMPUTE_POOL_ID_2 = "test-pool-120"
    TEST_COMPUTE_POOL_ID_3 = "test-pool-121"
    @classmethod
    def setUpClass(cls) -> None:
        """Set up test environment before running tests."""
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES"))

    def setUp(self) -> None:
        """Set up test case before each test."""
        self.config = get_config()
        self.compute_pool_id = self.TEST_COMPUTE_POOL_ID_1
        self.table_name = "test_table"
        self.inventory_path = os.getenv("PIPELINES")
        self.count = 0  # Initialize count as instance variable

    # Following set of methods are used to create reusable mock objects and functions
    def _create_mock_get_statement_info(
        self, 
        name: str = "statement_name",
        status_phase: str = "UNKNOWN",
        compute_pool_id: str = TEST_COMPUTE_POOL_ID_1
    ) -> StatementInfo:
        """Create a mock StatementInfo object."""
        return StatementInfo(
            name=name,
            status_phase=status_phase,
            compute_pool_id=compute_pool_id
        )
    
    def _create_mock_statement(
        self, 
        name: str = "statement_name",
        status_phase: str = "UNKNOWN"
    ) -> Statement:
        """Create a mock Statement object."""
        status = Status(phase=status_phase)
        return Statement(name=name, status=status)


    def _create_mock_compute_pool_list(self, env_id: str = "test-env-123", region: str = "test-region-123") -> ComputePoolList:
        """Create a mock ComputePoolList object."""
        pool_1 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_1,
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
        compute_pool_id: str = TEST_COMPUTE_POOL_ID_1
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
        node.existing_statement_info = self._create_mock_get_statement_info(
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
            
        assert node_map["src_y"].upgrade_mode == "Stateful"
        assert node_map["src_x"].upgrade_mode == "Stateful"
        assert node_map["src_b"].upgrade_mode == "Stateful"
        assert node_map["src_p2_a"].upgrade_mode == "Stateless"
        assert node_map["x"].upgrade_mode == "Stateful"
        assert node_map["y"].upgrade_mode == "Stateless"
        assert node_map["z"].upgrade_mode == "Stateful"
        assert node_map["d"].upgrade_mode == "Stateful"
        assert node_map["c"].upgrade_mode == "Stateless"
        assert node_map["p"].upgrade_mode == "Stateless"
        assert node_map["a"].upgrade_mode == "Stateful"
        assert node_map["b"].upgrade_mode == "Stateful"
        assert node_map["e"].upgrade_mode == "Stateless"
        assert node_map["f"].upgrade_mode == "Stateless"

    def test_build_topological_sorted_parents(self) -> None:
        """Test building topologically sorted parents.
        
        f has 6 parents: d, then z, x, y then src_y, src_x.
        The topological sort should return src_y, src_x, y, x, z, d, f.
        """
        print("test_build_topological_sorted_parents ")
        pipeline_def = read_pipeline_definition_from_file(
            self.inventory_path + "/facts/p2/f/" + PIPELINE_JSON_FILE_NAME
        )
        current_node = pipeline_def.to_node()
        node_map = dm._build_statement_node_map(current_node)
        nodes_to_run = dm._build_topological_sorted_parents([current_node], node_map)
        
        assert len(nodes_to_run) == 7
        for node in nodes_to_run:
            print(node.table_name, node.to_run, node.to_restart)
        assert nodes_to_run[0].table_name in ("src_y", "src_x")

    def test_build_ancestor_sorted_graph(self):
        node_map = {}
        node_map["src_x"] = FlinkStatementNode(table_name="src_x")
        node_map["src_y"] = FlinkStatementNode(table_name="src_y")
        node_map["src_b"] = FlinkStatementNode(table_name="src_b")
        node_map["src_a"] = FlinkStatementNode(table_name="src_a")
        node_map["x"] = FlinkStatementNode(table_name="x", parents=[node_map["src_x"]])
        node_map["y"] = FlinkStatementNode(table_name="y", parents=[node_map["src_y"]])
        node_map["b"] = FlinkStatementNode(table_name="b", parents=[node_map["src_b"]])
        node_map["z"] = FlinkStatementNode(table_name="z", parents=[node_map["x"], node_map["y"]])
        node_map["d"] = FlinkStatementNode(table_name="d", parents=[node_map["z"], node_map['y']])
        node_map["c"] = FlinkStatementNode(table_name="c", parents=[node_map["z"], node_map["b"]])
        node_map["p"] = FlinkStatementNode(table_name="p", parents=[node_map["z"]])
        node_map["a"] = FlinkStatementNode(table_name="a", parents=[node_map["src_x"], node_map["src_a"]])
   
        node_map["e"] = FlinkStatementNode(table_name="e", parents=[node_map["c"]])
        node_map["f"] = FlinkStatementNode(table_name="f", parents=[node_map["d"]])

        ancestors = dm._build_topological_sorted_parents([node_map["z"]], node_map)
        assert ancestors[0].table_name == "src_x" or ancestors[0].table_name == "src_y"
        assert ancestors[1].table_name == "src_x" or ancestors[1].table_name == "src_y"
        assert ancestors[2].table_name == "x" or ancestors[2].table_name == "y"
        assert ancestors[3].table_name == "x" or ancestors[3].table_name == "y"
        assert ancestors[4].table_name == "z"

    
    def test_build_children_sorted_graph_from_z(self):
        node_map = {}
        node_map["src_x"] = FlinkStatementNode(table_name="src_x")
        node_map["src_y"] = FlinkStatementNode(table_name="src_y")
        node_map["src_b"] = FlinkStatementNode(table_name="src_b")
        node_map["src_a"] = FlinkStatementNode(table_name="src_a")
        node_map["x"] = FlinkStatementNode(table_name="x", parents=[node_map["src_x"]])
        node_map["y"] = FlinkStatementNode(table_name="y", parents=[node_map["src_y"]])
        node_map["b"] = FlinkStatementNode(table_name="b", parents=[node_map["src_b"]])
        node_map["z"] = FlinkStatementNode(table_name="z", parents=[node_map["x"], node_map["y"]])
        node_map["d"] = FlinkStatementNode(table_name="d", parents=[node_map["z"], node_map['y']])
        node_map["c"] = FlinkStatementNode(table_name="c", parents=[node_map["z"], node_map["b"]])
        node_map["p"] = FlinkStatementNode(table_name="p", parents=[node_map["z"]])
        node_map["a"] = FlinkStatementNode(table_name="a", parents=[node_map["src_x"], node_map["src_a"]])
        node_map["e"] = FlinkStatementNode(table_name="e", parents=[node_map["c"]])
        node_map["f"] = FlinkStatementNode(table_name="f", parents=[node_map["d"]])
        node_map["z"].children = [node_map["d"], node_map["c"], node_map["p"]]
        node_map["d"].children = [node_map["f"]]
        node_map["c"].children = [node_map["e"]]
        descendants = dm._build_topological_sorted_children(node_map["z"], node_map)
        for node in descendants:
            print(node.table_name, node.to_run, node.to_restart)
            assert node.table_name in ["p","d", "f", "c", "e", "z"]
        
    def test_build_children_sorted_graph_from_src_x(self):
        pipeline_def = read_pipeline_definition_from_file(
            self.inventory_path + "/sources/p2/src_x/" + PIPELINE_JSON_FILE_NAME
        )
        current_node = pipeline_def.to_node()
        node_map = dm._build_statement_node_map(current_node)
        descendants = dm._build_topological_sorted_children(current_node, node_map)
        for node in descendants:
            print(node.table_name, node.to_run, node.to_restart)
            assert node.table_name in ["p","e", "d", "f", "c", "a", "z", "x", "src_x"]

    def test_topological_sort(self):
        node_map = {}
        node_map["src_x"] = FlinkStatementNode(table_name="src_x")
        node_map["src_y"] = FlinkStatementNode(table_name="src_y")
        node_map["src_b"] = FlinkStatementNode(table_name="src_b")
        node_map["x"] = FlinkStatementNode(table_name="x", parents=[node_map["src_x"]])
        node_map["y"] = FlinkStatementNode(table_name="y", parents=[node_map["src_y"]])
        node_map["b"] = FlinkStatementNode(table_name="b", parents=[node_map["src_b"]])
        node_map["z"] = FlinkStatementNode(table_name="z", parents=[node_map["x"], node_map["y"]])
        node_map["d"] = FlinkStatementNode(table_name="d", parents=[node_map["z"], node_map['y']])
        node_map["c"] = FlinkStatementNode(table_name="c", parents=[node_map["z"], node_map["b"]])
        node_map["z"].children = [node_map["d"], node_map["c"]]
        node_map["src_x"].children = [node_map["x"]]
        node_map["src_y"].children = [node_map["y"]]
        node_map["src_b"].children = [node_map["b"]]
        node_map["x"].children = [node_map["z"]]
        node_map["y"].children = [node_map["z"]]
        node_map["b"].children = [node_map["c"]]

        ancestors = dm._build_topological_sorted_parents([node_map["src_x"]], node_map)
        assert len(ancestors) == 1
        assert ancestors[0].table_name == "src_x"
        print("\nancestors of src_x:")
        for node in ancestors:
            print(node.table_name)
        descendants = dm._build_topological_sorted_children(node_map["src_x"], node_map)
        assert len(descendants) == 5
        print("\ndescendants of src_x:")
        for node in descendants:
            print(node.table_name)
        combined = ancestors + descendants
        new_ancestors = dm._build_topological_sorted_parents(combined, node_map)
        print("\nnew sorted ancestors:")
        for node in new_ancestors:
            print(node.table_name)
        ancestors = dm._build_topological_sorted_parents([node_map["z"]], node_map)
        print("\nancestors of z:")
        for node in ancestors:
            print(node.table_name)
        descendants = dm._build_topological_sorted_children(node_map["z"], node_map)
        print("\ndescendants of z:")
        for node in descendants:
            print(node.table_name)
        combined_2 = ancestors + descendants
        new_ancestors_2 = dm._build_topological_sorted_parents(combined_2, node_map)
        print("\nnew sorted ancestors:")
        for node in new_ancestors_2:
            print(node.table_name)


    def test_pass_list_of_tables_to_build_ancestor_sorted_graph(self):
        """
        Tst to pass a list of table to build the ancestor sorted graph
        """
        node_map = {}
        node_map["src_x"] = FlinkStatementNode(table_name="src_x")
        node_map["src_y"] = FlinkStatementNode(table_name="src_y")
        node_map["src_b"] = FlinkStatementNode(table_name="src_b")
        node_map["src_a"] = FlinkStatementNode(table_name="src_a")
        node_map["x"] = FlinkStatementNode(table_name="x", parents=[node_map["src_x"]])
        node_map["y"] = FlinkStatementNode(table_name="y", parents=[node_map["src_y"]])
        node_map["b"] = FlinkStatementNode(table_name="b", parents=[node_map["src_b"]])
        node_map["z"] = FlinkStatementNode(table_name="z", parents=[node_map["x"], node_map["y"]])
        node_map["d"] = FlinkStatementNode(table_name="d", parents=[node_map["z"], node_map['y']])
        node_map["c"] = FlinkStatementNode(table_name="c", parents=[node_map["z"], node_map["b"]])
        node_map["p"] = FlinkStatementNode(table_name="p", parents=[node_map["z"]])
        node_map["a"] = FlinkStatementNode(table_name="a", parents=[node_map["src_x"], node_map["src_a"]])
        node_map["e"] = FlinkStatementNode(table_name="e", parents=[node_map["c"]])
        node_map["f"] = FlinkStatementNode(table_name="f", parents=[node_map["d"]])

        table_list = ['x','y','z','a','b']
        merged_nodes = {}
        merged_dependencies = []
        for node in table_list:
            nodes, dependencies = dm._get_ancestor_subgraph(node_map[node], node_map)
            print(len(nodes) , len(dependencies))
            merged_nodes.update(nodes)
            for dep in dependencies:
                merged_dependencies.append(dep)
        assert len(merged_nodes) == 9
        assert len(merged_dependencies) == 11
        sorted_nodes = dm._topological_sort(merged_nodes, merged_dependencies)
        for node in sorted_nodes:
            print(f"{node.table_name}")
        assert len(sorted_nodes) == 9
        assert sorted_nodes[0].table_name in ["src_x", "src_a", "src_y", "src_b"]
        assert sorted_nodes[4].table_name in ["x", "y", "a", "b"]
        assert sorted_nodes[8].table_name == "z"

    @patch('shift_left.core.deployment_mgr.statement_mgr.build_and_deploy_flink_statement_from_sql_content')    
    @patch('shift_left.core.deployment_mgr.statement_mgr.drop_table')
    @patch('shift_left.core.deployment_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def test_deploy_pipeline_from_table(self, 
                                        mock_assign_compute_pool_id, 
                                        mock_get_status, 
                                        mock_get_compute_pool_list,
                                        mock_delete,
                                        mock_drop,
                                        mock_build_and_deploy_flink_statement_from_sql_content):
        def _mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-p2-dml-z", "dev-p2-dml-y", "dev-p2-dml-src-y", "dev-p2-dml-src-x", "dev-p2-dml-x"]:  
                print(f"mock_ get statement info: {statement_name} -> RUNNING")
                return self._create_mock_get_statement_info(status_phase="RUNNING")
            else:
                print(f"mock_ get statement info: {statement_name} -> UNKNOWN")
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")
 
        def _drop_table(table_name: str, compute_pool_id: str) -> str:
            print(f"drop_table {table_name} {compute_pool_id}")
            return "deleted"

        def _build_statement(node: FlinkStatementNode, flname: str, statement_name: str) -> str:
            print(f"build_statement {statement_name}")
            metadata = Metadata(created_at=str(datetime.now()), uid="test-uid")
            spec = Spec(compute_pool_id=self.TEST_COMPUTE_POOL_ID_1, 
                        principal="test-principal",
                        properties={'sql.current-catalog': 'j9r-catalog', 'sql.current-database': 'j9r-database'},
                        statement="sql statement",
                        execution_time=5,
                        stopped=False)
            if "ddl" in statement_name:
                status = Status(phase="COMPLETED", detail="")
            else:
                status = Status(phase="RUNNING", detail="")
            return Statement(name=statement_name, 
                             status=status, 
                             spec=spec, 
                             metadata=metadata,
                             organization_id="org_test",
                             environment_id="env_test")

        mock_get_status.side_effect = _mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_delete.return_value = "deleted"
        mock_drop.side_effect = _drop_table
        mock_build_and_deploy_flink_statement_from_sql_content.side_effect = _build_statement

        summary, execution_plan = dm.build_deploy_pipeline_from_table(table_name="d", 
                                       inventory_path=self.inventory_path, 
                                       compute_pool_id=self.TEST_COMPUTE_POOL_ID_1, 
                                       dml_only=False, 
                                       execute_plan=True,
                                       may_start_descendants=False,
                                       force_ancestors=False)
        assert execution_plan.start_table_name == "d"
        assert len(execution_plan.nodes) == 6
        assert execution_plan.nodes[0].table_name in ["src_x", "src_y"]
        assert execution_plan.nodes[2].table_name in ["x", "y"]
        print(f"summary: {summary}")
        print(f"execution_plan: {execution_plan.model_dump_json(indent=3)}")


    @patch('shift_left.core.deployment_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.deployment_mgr.statement_mgr.drop_table')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def _test_full_pipeline_undeploy(
        self,
        mock_assign_compute_pool_id,
        mock_get_status,
        mock_get_compute_pool_list,
        mock_drop,
        mock_delete
    ) -> None:
        """Test successful pipeline undeployment."""
        print("test_full_pipeline_undeploy")
        self.count = 0  # Reset count for this test
        def mock_statement(statement_name: str) -> StatementInfo:
            print(f"mock_statement {statement_name}")
            return self._create_mock_get_statement_info(status_phase="RUNNING")
 
        def drop_table(table_name: str, compute_pool_id: str) -> str:
            print(f"drop_table {table_name} {compute_pool_id}")
            self.count += 1 
            return "deleted"

        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_delete.return_value = "deleted"
        mock_drop.side_effect = drop_table
        
        # Execute
        result = dm.full_pipeline_undeploy_from_table(
            table_name="z",
            inventory_path=self.inventory_path
        )
        print(result)
        # Verify
        mock_delete.assert_called()
        assert self.count == 11  # call for all tables

if __name__ == '__main__':
    unittest.main()