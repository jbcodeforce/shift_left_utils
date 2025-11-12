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
import time
TEST_PIPELINES_DIR = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = TEST_PIPELINES_DIR

import shift_left.core.pipeline_mgr as pm
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.file_search import read_pipeline_definition_from_file
import shift_left.core.deployment_mgr as dm

from shift_left.core.models.flink_statement_model import (
    Statement,
    StatementInfo,
    Status,
    Spec,
    Metadata
)
from shift_left.core.deployment_mgr import (
    FlinkStatementNode
)

from shift_left.core.models.flink_statement_model import Statement, StatementInfo
from ut.core.BaseUT import BaseUT

class TestDeploymentManager(BaseUT):
    """Test suite for the deployment manager functionality."""


    @classmethod
    def setUpClass(cls) -> None:
        """Set up test environment before running tests."""
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES",TEST_PIPELINES_DIR))

    def setUp(self) -> None:
        """Set up test case before each test."""
        self.config = get_config()
        self.compute_pool_id = self.TEST_COMPUTE_POOL_ID_1
        self.table_name = "test_table"
        self.inventory_path = os.getenv("PIPELINES",TEST_PIPELINES_DIR)
        self.count = 0  # Initialize count as instance variable


    #  ----------- TESTS -----------


    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_list')
    def test_build_topological_sorted_graph(self, mock_statement_list) -> None:
        """Test building topologically sorted parents.

        f has 6 parents: d, then z, x, y then src_y, src_x.
        The topological sort should return src_y, src_x, y, x, z, d, f.
        """
        mock_statement_list.return_value = {
            "test-statement-1": StatementInfo(name= "test-statement-1", status_phase= "RUNNING"),
            "test-statement-2": StatementInfo(name= "test-statement-2", status_phase= "COMPLETED")
        }
        print("test_build_topological_sorted_graph ")
        pipeline_def = read_pipeline_definition_from_file(
            self.inventory_path + "/facts/p2/f/" + PIPELINE_JSON_FILE_NAME
        )
        current_node = pipeline_def.to_node()
        node_map = dm._build_statement_node_map(current_node)
        nodes_to_run = dm._build_topological_sorted_graph([current_node], node_map)

        assert len(nodes_to_run) == 7
        for node in nodes_to_run:
            print(node.table_name, node.to_run, node.to_restart)
        assert nodes_to_run[0].table_name in ("src_y", "src_x")


    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_list')
    def test_build_ancestor_sorted_graph(self, mock_get_statement_list):
        mock_get_statement_list.return_value = {
            "test-statement-1": StatementInfo(name= "test-statement-1", status_phase= "RUNNING"),
            "test-statement-2": StatementInfo(name= "test-statement-2", status_phase= "COMPLETED")
        }
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

        ancestors = dm._build_topological_sorted_graph([node_map["z"]], node_map)
        assert ancestors[0].table_name == "src_x" or ancestors[0].table_name == "src_y"
        assert ancestors[1].table_name == "src_x" or ancestors[1].table_name == "src_y"
        assert ancestors[2].table_name == "x" or ancestors[2].table_name == "y"
        assert ancestors[3].table_name == "x" or ancestors[3].table_name == "y"
        assert ancestors[4].table_name == "z"


    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_list')
    def test_build_children_sorted_graph_from_z(self, mock_statement_list):
        mock_statement_list.return_value = {
            "test-statement-1": StatementInfo(name= "test-statement-1", status_phase= "RUNNING"),
            "test-statement-2": StatementInfo(name= "test-statement-2", status_phase= "COMPLETED")
        }
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

    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_list')
    def test_build_children_sorted_graph_from_src_x(self, mock_statement_list):
        mock_statement_list.return_value = {
            "test-statement-1": StatementInfo(name= "test-statement-1", status_phase= "RUNNING"),
            "test-statement-2": StatementInfo(name= "test-statement-2", status_phase= "COMPLETED")
        }
        pipeline_def = read_pipeline_definition_from_file(
            self.inventory_path + "/sources/p2/src_x/" + PIPELINE_JSON_FILE_NAME
        )
        current_node = pipeline_def.to_node()
        node_map = dm._build_statement_node_map(current_node)
        descendants = dm._build_topological_sorted_children(current_node, node_map)
        for node in descendants:
            print(node.table_name, node.to_run, node.to_restart)
            assert node.table_name in ["p","e", "d", "f", "c", "a", "z", "x", "src_x"]

    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_list')
    def test_topological_sort(self, mock_statement_list):
        mock_statement_list.return_value = {
            "test-statement-1": StatementInfo(name= "test-statement-1", status_phase= "RUNNING"),
            "test-statement-2": StatementInfo(name= "test-statement-2", status_phase= "COMPLETED")
        }
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

        ancestors = dm._build_topological_sorted_graph([node_map["src_x"]], node_map)
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
        new_ancestors = dm._build_topological_sorted_graph(combined, node_map)
        print("\nnew sorted ancestors:")
        for node in new_ancestors:
            print(node.table_name)
        ancestors = dm._build_topological_sorted_graph([node_map["z"]], node_map)
        print("\nancestors of z:")
        for node in ancestors:
            print(node.table_name)
        descendants = dm._build_topological_sorted_children(node_map["z"], node_map)
        print("\ndescendants of z:")
        for node in descendants:
            print(node.table_name)
        combined_2 = ancestors + descendants
        new_ancestors_2 = dm._build_topological_sorted_graph(combined_2, node_map)
        print("\nnew sorted ancestors:")
        for node in new_ancestors_2:
            print(node.table_name)




    @patch('shift_left.core.deployment_mgr.report_mgr.build_simple_report')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_list')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.deployment_mgr.statement_mgr.drop_table')
    @patch('shift_left.core.deployment_mgr.statement_mgr.delete_statement_if_exists')
    def test_deploy_table_pipeline(self,
                                    mock_delete,
                                    mock_drop,
                                    mock_post,
                                    mock_get_status,
                                    mock_get_compute_pool_list,
                                    mock_get_statement_list,
                                    mock_build_simple_report):


        """
        start src_x, src_y, x, y, z, d
        """
        def _drop_table(table_name: str, compute_pool_id: str) -> str:
            print(f"@@@@ drop_table {table_name} {compute_pool_id}")
            time.sleep(1)
            return "deleted"

        def _post_flink_statement(compute_pool_id: str, statement_name: str, sql_content: str) -> Statement:
            print(f"\n@@@@ post_flink_statement {compute_pool_id} {statement_name} {sql_content}")
            time.sleep(1)
            if statement_name in ["dev-usw2-p2-dml-z", "dev-usw2-p2-dml-y", "dev-usw2-p2-dml-src-y", "dev-usw2-p2-dml-src-x", "dev-usw2-p2-dml-x","dev-usw2-p2-dml-d"]:
                print(f"mock_ get statement info: {statement_name} -> RUNNING")
                return self._create_mock_statement(name=statement_name, status_phase="RUNNING")
            elif "ddl" in statement_name:
                return self._create_mock_statement(name=statement_name, status_phase="COMPLETED")
            else:
                print(f"mock_ get statement info: {statement_name} -> UNKNOWN")
                return self._create_mock_statement(name=statement_name, status_phase="UNKNOWN")

        def _get_status(statement_name: str) -> StatementInfo:
            print(f"@@@@ get status {statement_name}")
            if statement_name in ["dev-usw2-p2-dml-src-y", "dev-usw2-p2-dml-src-x"]:
                return self._create_mock_get_statement_info(name=statement_name, status_phase="RUNNING")
            return self._create_mock_get_statement_info(name=statement_name, status_phase="UNKNOWN")

        def _delete_statement(statement_name: str):
            print(f"@@@@ delete statement {statement_name}")
            time.sleep(1)
            return "deleted"

        mock_get_status.side_effect = _get_status
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_delete.side_effect = _delete_statement
        mock_drop.side_effect = _drop_table
        mock_post.side_effect = _post_flink_statement
        # Avoid remote call via statement_mgr.get_statement_list() inside build_and_deploy_flink_statement_from_sql_content
        mock_get_statement_list.return_value = {}
        mock_build_simple_report.return_value = "mock_build_simple_report"
        summary, report = dm.build_deploy_pipeline_from_table(table_name="d",
                                    inventory_path=self.inventory_path,
                                    compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
                                    dml_only=False,
                                    execute_plan=True,
                                    may_start_descendants=False,
                                    force_ancestors=False)

        assert len(report.tables) == 6
        assert report.tables[0].table_name in ["src_x", "src_y"]
        assert report.tables[2].table_name in ["x", "y"]
        print(f"summary: {summary}")
        print(f"report: {report.model_dump_json(indent=3)}")

    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.deployment_mgr.statement_mgr.drop_table')
    @patch('shift_left.core.deployment_mgr.statement_mgr.delete_statement_if_exists')
    def test_deploy_product_using_parallel(self,
                                           mock_delete,
                                           mock_drop,
                                           mock_post,
                                           mock_assign_compute_pool_id):
        """
        Deploying a product p1 in parallel should deploy all the tables in parallel.
        """
        def _drop_table(table_name: str, compute_pool_id: str) -> str:
            print(f"@@@@ drop_table {table_name} {compute_pool_id}")
            time.sleep(1)
            return "deleted"

        def _post_flink_statement(compute_pool_id: str, statement_name: str, sql_content: str) -> Statement:
            print(f"\n@@@@ post_flink_statement {compute_pool_id} {statement_name} {sql_content}")
            time.sleep(1)
            if "ddl" in statement_name:
                return self._create_mock_statement(name=statement_name, status_phase="COMPLETED")
            return self._create_mock_statement(name=statement_name, status_phase="RUNNING")

        def _delete_statement(statement_name: str):
            print(f"@@@@ delete statement {statement_name}")
            time.sleep(1)
            return "deleted"

        mock_delete.side_effect = _delete_statement
        mock_drop.side_effect = _drop_table
        mock_post.side_effect = _post_flink_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        dm.build_deploy_pipelines_from_product(product_name="p1",
                                                inventory_path=self.inventory_path,
                                                execute_plan=True,
                                                force_ancestors=True,
                                                may_start_descendants=True,
                                                sequential=False,
                                                pool_creation=False)


    @patch('shift_left.core.deployment_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.deployment_mgr.statement_mgr.drop_table')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def test_full_pipeline_undeploy_from_table(
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

        def mock_drop_table(table_name: str, compute_pool_id: str) -> str:
            print(f"drop_table {table_name} {compute_pool_id}")
            self.count += 1
            return "deleted"

        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_delete.return_value = "deleted"
        mock_drop.side_effect = mock_drop_table

        # Execute
        result = dm.full_pipeline_undeploy_from_table(
            table_name="z",
            inventory_path=self.inventory_path
        )
        print(result)
        # Verify
        mock_delete.assert_called()
        assert self.count == 6  # call for all 6 tables (z, d, c, f, p, e)

    @patch('shift_left.core.deployment_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.deployment_mgr.statement_mgr.delete_statement_if_exists')
    def test_prepare_table(self, mock_delete, mock_post):
        """
        Test the prepare table
        """

        def mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            status = Status(
                phase= "COMPLETED",
                detail= ""
            )
            spec = Spec(
                compute_pool_id=get_config().get('flink').get('compute_pool_id'),
                principal="principal_sa",
                statement=sql_content,
                properties={"sql.current-catalog": "default", "sql.current-database": "default"},
                stopped=False
            )
            metadata = Metadata(
                created_at="2025-04-20T10:15:02.853006",
                labels={},
                resource_version="1",
                self="https://test-url",
                uid="test-uid",
                updated_at="2025-04-20T10:15:02.853006"
            )
            return Statement(name= statement_name, status= status, spec=spec, metadata=metadata)


        mock_delete.return_value = "deleted"
        mock_post.side_effect = mock_post_statement
        path_to_sql_file = os.getenv("PIPELINES",TEST_PIPELINES_DIR) + "/alter_table_avro_debezium.sql"
        dm.prepare_tables_from_sql_file(sql_file_name=path_to_sql_file,
                                        compute_pool_id="lfcp-121")


    @patch('shift_left.core.deployment_mgr._drop_node_worker')
    @patch('shift_left.core.deployment_mgr._build_execution_plan_using_sorted_ancestors')
    @patch('shift_left.core.deployment_mgr._build_topological_sorted_graph')
    @patch('shift_left.core.deployment_mgr._build_statement_node_map')
    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    @patch('shift_left.core.deployment_mgr.get_or_build_inventory')
    @patch('shift_left.core.deployment_mgr.get_config')
    def test_full_pipeline_undeploy_from_product_success(
        self,
        mock_get_config,
        mock_get_inventory,
        mock_read_pipeline,
        mock_build_node_map,
        mock_build_sorted_parents,
        mock_build_execution_plan,
        mock_drop_worker
    ):
        """Test successful pipeline undeployment from product.
         Two tables belong to the target product and are running, both should be dropped
        """
        print("test_full_pipeline_undeploy_from_product_success")

        # Setup mocks
        mock_get_config.return_value = {'flink': {'compute_pool_id': self.TEST_COMPUTE_POOL_ID_1}}

        # Mock inventory with tables from target product and other products
        mock_inventory = {
            'table1': {
                'table_name': 'table1',
                'product_name': 'test_product',
                'table_folder_name': 'table1_folder',
                'type': 'source'
            },
            'table2': {
                'table_name': 'table2',
                'product_name': 'test_product',
                'table_folder_name': 'table2_folder',
                'type': 'fact'
            },
            'table3': {
                'table_name': 'table3',
                'product_name': 'other_product',
                'table_folder_name': 'table3_folder',
                'type': 'source'
            }
        }
        mock_get_inventory.return_value = mock_inventory

        # Mock pipeline definitions that return nodes
        mock_pipeline_def = MagicMock()
        mock_node1 = self._create_mock_statement_node(
            table_name='table1',
            product_name='test_product',
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1
        )
        mock_node1.existing_statement_info = self._create_mock_get_statement_info(
            name='table1-dml',
            status_phase='RUNNING'
        )

        mock_node2 = self._create_mock_statement_node(
            table_name='table2',
            product_name='test_product',
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1
        )
        mock_node2.existing_statement_info = self._create_mock_get_statement_info(
            name='table2-dml',
            status_phase='RUNNING'
        )

        mock_pipeline_def.to_node.side_effect = [mock_node1, mock_node2]
        mock_read_pipeline.return_value = mock_pipeline_def

        # Mock node map building
        mock_node_map = {
            'table1': mock_node1,
            'table2': mock_node2
        }
        mock_build_node_map.return_value = mock_node_map

        # Mock topological sorting
        mock_build_sorted_parents.return_value = [mock_node1, mock_node2]

        # Mock execution plan
        mock_execution_plan = MagicMock()
        mock_execution_plan.nodes = [mock_node2, mock_node1]  # Reverse order for undeploy
        mock_build_execution_plan.return_value = mock_execution_plan

        # Mock drop worker to return success messages
        mock_drop_worker.return_value = "Dropped table successfully\n"

        # Execute the function
        result = dm.full_pipeline_undeploy_from_product(
            product_name='test_product',
            inventory_path='/test/inventory/path',
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1
        )

        # Verify the result
        self.assertIsInstance(result, str)
        self.assertIn("Full pipeline delete from product test_product", result)
        self.assertIn("Dropped table successfully", result)

        # Verify mock calls
        mock_get_inventory.assert_called_once_with('/test/inventory/path', '/test/inventory/path', False)
        self.assertEqual(mock_read_pipeline.call_count, 2)  # Called for table1 and table2
        mock_build_node_map.assert_called()
        mock_build_sorted_parents.assert_called_once()
        mock_build_execution_plan.assert_called_once()

        # Verify drop worker was called for both nodes
        self.assertEqual(mock_drop_worker.call_count, 2)

    @patch('shift_left.core.deployment_mgr.get_or_build_inventory')
    @patch('shift_left.core.deployment_mgr.get_config')
    def test_full_pipeline_undeploy_from_product_no_tables_found(
        self,
        mock_get_config,
        mock_get_inventory
    ):
        """Test when no tables are found for the specified product."""
        print("test_full_pipeline_undeploy_from_product_no_tables_found")

        # Setup mocks
        mock_get_config.return_value = {'flink': {'compute_pool_id': self.TEST_COMPUTE_POOL_ID_1}}

        # Mock inventory with no tables for target product
        mock_inventory = {
            'table1': {
                'table_name': 'table1',
                'product_name': 'other_product',
                'table_folder_name': 'table1_folder'
            }
        }
        mock_get_inventory.return_value = mock_inventory

        # Execute the function
        result = dm.full_pipeline_undeploy_from_product(
            product_name='test_product',
            inventory_path='/test/inventory/path',
			compute_pool_id=self.TEST_COMPUTE_POOL_ID_1
        )

        # Verify the result - when no tables found for product, returns empty trace
        self.assertEqual(result, "")
        mock_get_inventory.assert_called_once_with('/test/inventory/path', '/test/inventory/path', False)

    @patch('shift_left.core.deployment_mgr.get_or_build_inventory')
    @patch('shift_left.core.deployment_mgr.get_config')
    def test_full_pipeline_undeploy_from_product_empty_inventory(
        self,
        mock_get_config,
        mock_get_inventory
    ):
        """Test when inventory is empty."""
        print("test_full_pipeline_undeploy_from_product_empty_inventory")

        # Setup mocks
        mock_get_config.return_value = {'flink': {'compute_pool_id': self.TEST_COMPUTE_POOL_ID_1}}
        mock_get_inventory.return_value = {}

        # Execute the function
        result = dm.full_pipeline_undeploy_from_product(
            product_name='test_product',
            inventory_path='/test/inventory/path',
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1
        )

        # Verify the result - empty inventory returns empty trace
        self.assertEqual(result, "")

    @patch('shift_left.core.deployment_mgr._drop_node_worker')
    @patch('shift_left.core.deployment_mgr._build_execution_plan_using_sorted_ancestors')
    @patch('shift_left.core.deployment_mgr._build_topological_sorted_graph')
    @patch('shift_left.core.deployment_mgr._build_statement_node_map')
    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    @patch('shift_left.core.deployment_mgr.get_or_build_inventory')
    @patch('shift_left.core.deployment_mgr.get_config')
    def test_full_pipeline_undeploy_from_product_no_nodes_to_drop(
        self,
        mock_get_config,
        mock_get_inventory,
        mock_read_pipeline,
        mock_build_node_map,
        mock_build_sorted_parents,
        mock_build_execution_plan,
        mock_drop_worker
    ):
        """Test when tables exist for product but none need to be dropped (not running).
        Tables exist but have status "UNKNOWN" (not running), so they don't qualify for deletion
        """

        print("test_full_pipeline_undeploy_from_product_no_nodes_to_drop")

        # Setup mocks
        mock_get_config.return_value = {'flink': {'compute_pool_id': self.TEST_COMPUTE_POOL_ID_1}}

        # Mock inventory with tables for target product
        mock_inventory = {
            'table1': {
                'table_name': 'table1',
                'product_name': 'test_product',
                'table_folder_name': 'table1_folder',
                'type': 'source'
            }
        }
        mock_get_inventory.return_value = mock_inventory

        # Mock pipeline definition that returns a node
        mock_pipeline_def = MagicMock()
        mock_node1 = self._create_mock_statement_node(
            table_name='table1',
            product_name='test_product'
        )
        # Set node status to UNKNOWN so it won't be dropped
        mock_node1.existing_statement_info = self._create_mock_get_statement_info(
            name='table1-dml',
            status_phase='UNKNOWN'
        )

        mock_pipeline_def.to_node.return_value = mock_node1
        mock_read_pipeline.return_value = mock_pipeline_def

        # Mock node map building
        mock_node_map = {'table1': mock_node1}
        mock_build_node_map.return_value = mock_node_map

        # Mock topological sorting
        mock_build_sorted_parents.return_value = [mock_node1]

        # Mock execution plan
        mock_execution_plan = MagicMock()
        mock_execution_plan.nodes = [mock_node1]
        mock_build_execution_plan.return_value = mock_execution_plan

        # Execute the function
        result = dm.full_pipeline_undeploy_from_product(
            product_name='test_product',
            inventory_path='/test/inventory/path',
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1
        )

        # Verify the result - should return the "No table found" message when no nodes to drop
        self.assertEqual(result, "No table found for product test_product in inventory /test/inventory/path")

        # Verify mocks called but drop worker should not be called
        mock_get_inventory.assert_called_once()
        mock_read_pipeline.assert_called_once()
        mock_drop_worker.assert_not_called()

    @patch('shift_left.core.deployment_mgr._drop_node_worker')
    @patch('shift_left.core.deployment_mgr._build_execution_plan_using_sorted_ancestors')
    @patch('shift_left.core.deployment_mgr._build_topological_sorted_graph')
    @patch('shift_left.core.deployment_mgr._build_statement_node_map')
    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    @patch('shift_left.core.deployment_mgr.get_or_build_inventory')
    @patch('shift_left.core.deployment_mgr.get_config')
    def test_full_pipeline_undeploy_from_product_with_errors(
        self,
        mock_get_config,
        mock_get_inventory,
        mock_read_pipeline,
        mock_build_node_map,
        mock_build_sorted_parents,
        mock_build_execution_plan,
        mock_drop_worker
    ):
        """Test pipeline undeployment with some failures."""
        print("test_full_pipeline_undeploy_from_product_with_errors")

        # Setup mocks similar to success test
        mock_get_config.return_value = {'flink': {'compute_pool_id': self.TEST_COMPUTE_POOL_ID_1}}

        mock_inventory = {
            'table1': {
                'table_name': 'table1',
                'product_name': 'test_product',
                'table_folder_name': 'table1_folder',
                'type': 'source'
            }
        }
        mock_get_inventory.return_value = mock_inventory

        mock_pipeline_def = MagicMock()
        mock_node1 = self._create_mock_statement_node(
            table_name='table1',
            product_name='test_product'
        )
        mock_node1.existing_statement_info = self._create_mock_get_statement_info(
            name='table1-dml',
            status_phase='RUNNING'
        )

        mock_pipeline_def.to_node.return_value = mock_node1
        mock_read_pipeline.return_value = mock_pipeline_def

        mock_node_map = {'table1': mock_node1}
        mock_build_node_map.return_value = mock_node_map
        mock_build_sorted_parents.return_value = [mock_node1]

        mock_execution_plan = MagicMock()
        mock_execution_plan.nodes = [mock_node1]
        mock_build_execution_plan.return_value = mock_execution_plan

        # Mock drop worker to raise an exception
        mock_drop_worker.side_effect = Exception("Connection failed")

        # Execute the function
        result = dm.full_pipeline_undeploy_from_product(
            product_name='test_product',
            inventory_path='/test/inventory/path',
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1
        )

        # Verify the result contains error message
        self.assertIsInstance(result, str)
        self.assertIn("Full pipeline delete from product test_product", result)
        self.assertIn("Failed to process table1: Connection failed", result)



if __name__ == '__main__':
    unittest.main()
