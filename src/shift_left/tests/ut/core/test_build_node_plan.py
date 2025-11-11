"""
Copyright 2024-2025 Confluent, Inc.
"""
from pdb import pm
import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
from datetime import datetime

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

from shift_left.core.models.flink_statement_model import FlinkStatementNode
from shift_left.core.utils.file_search import FlinkTablePipelineDefinition
from shift_left.core.deployment_mgr import _build_statement_node_map
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
from shift_left.core.utils.file_search import read_pipeline_definition_from_file
import shift_left.core.pipeline_mgr as pm

class TestBuildNodePlan(unittest.TestCase):
    """Test suite for build node plan functionality."""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up test environment before running tests."""
        pipelines_path = os.getenv("PIPELINES")
        if pipelines_path is None:
            raise ValueError("PIPELINES environment variable is not set")
        pm.build_all_pipeline_definitions(pipelines_path)

    def _create_node(self, table_name: str, type: str) -> FlinkStatementNode:
        """Create a node."""
        return FlinkStatementNode(
            table_name=table_name,
            product_name=self.test_product_name,
            type=type,
            path=f"{self.test_path}/{table_name}",
            created_at=datetime.now(),
            dml_statement_name=f"dml_{table_name}",
            dml_ref=f"dml_{table_name}.sql",
            ddl_statement_name=f"ddl_{table_name}",
            ddl_ref=f"ddl_{table_name}.sql",
            upgrade_mode="Stateful")


    def setUp(self):
        """Set up test fixtures."""
        self.test_product_name = "test_product"
        self.test_path = "/test/path"
        self.inventory_path = os.getenv("PIPELINES")

        # Create reusabletest nodes
        self.source_node = self._create_node(table_name="source_table", type="source")
        self.intermediate_node = self._create_node(table_name="intermediate_table", type="intermediate")
        self.sink_node = self._create_node(table_name="sink_table", type="fact")

        # Set up relationships: source -> intermediate -> sink
        self.intermediate_node.add_parent(self.source_node)
        self.sink_node.add_parent(self.intermediate_node)

    def create_mock_pipeline_definition(self, node: FlinkStatementNode) -> FlinkTablePipelineDefinition:
        """Create a mock pipeline definition from a node."""
        mock_pipeline_def = MagicMock(spec=FlinkTablePipelineDefinition)
        mock_pipeline_def.table_name = node.table_name
        mock_pipeline_def.product_name = node.product_name
        mock_pipeline_def.type = node.type
        mock_pipeline_def.path = node.path
        mock_pipeline_def.dml_ref = node.dml_ref
        mock_pipeline_def.ddl_ref = node.ddl_ref
        mock_pipeline_def.state_form = node.upgrade_mode
        mock_pipeline_def.parents = set()
        mock_pipeline_def.children = set()
        mock_pipeline_def.to_node.return_value = node
        return mock_pipeline_def

    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    def test_build_statement_node_map_simple_chain(self, mock_read_pipeline):
        """Test building node map for a simple chain of nodes: source -> intermediate -> sink."""

        # Create mock pipeline definitions
        source_pipeline = self.create_mock_pipeline_definition(self.source_node)
        intermediate_pipeline = self.create_mock_pipeline_definition(self.intermediate_node)
        sink_pipeline = self.create_mock_pipeline_definition(self.sink_node)

        # Mock the read_pipeline_definition_from_file function
        def mock_read_pipeline_side_effect(path):
            if "source_table" in path:
                return source_pipeline
            elif "intermediate_table" in path:
                return intermediate_pipeline
            elif "sink_table" in path:
                return sink_pipeline
            return None

        mock_read_pipeline.side_effect = mock_read_pipeline_side_effect

        # Test the function
        combined_node_map = {}
        visited_nodes = set()
        result_node_map = _build_statement_node_map(self.sink_node, visited_nodes, combined_node_map)

        # Verify the result
        self.assertEqual(len(result_node_map), 3)
        self.assertIn("source_table", result_node_map)
        self.assertIn("intermediate_table", result_node_map)
        self.assertIn("sink_table", result_node_map)

        # Verify nodes are the correct type
        self.assertIsInstance(result_node_map["source_table"], FlinkStatementNode)
        self.assertIsInstance(result_node_map["intermediate_table"], FlinkStatementNode)
        self.assertIsInstance(result_node_map["sink_table"], FlinkStatementNode)

        # Verify table names
        self.assertEqual(result_node_map["source_table"].table_name, "source_table")
        self.assertEqual(result_node_map["intermediate_table"].table_name, "intermediate_table")
        self.assertEqual(result_node_map["sink_table"].table_name, "sink_table")

    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    def test_build_statement_node_map_with_missing_pipeline_def(self, mock_read_pipeline):
        """Test building node map when pipeline definition is missing."""

        # Mock the function to return None for missing pipeline definition
        def mock_read_pipeline_side_effect(path):
            if "source_table" in path:
                return None  # Simulate missing pipeline definition
            return self.create_mock_pipeline_definition(self.sink_node)

        mock_read_pipeline.side_effect = mock_read_pipeline_side_effect

        # Test the function
        combined_node_map = {}
        visited_nodes = set()
        result_node_map = _build_statement_node_map(self.sink_node, visited_nodes, combined_node_map)

        # Verify that the function handles missing pipeline definitions gracefully
        assert len(result_node_map) == 1
        self.assertIn("sink_table", result_node_map)
        self.assertEqual(result_node_map["sink_table"].table_name, "sink_table")

    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    def test_build_statement_node_map_single_node(self, mock_read_pipeline):
        """Test building node map for a single node with no parents or children."""

        # Create a standalone node
        standalone_node = self._create_node(table_name="standalone_table", type="source")

        # Mock the read_pipeline_definition_from_file function
        mock_read_pipeline.return_value = self.create_mock_pipeline_definition(standalone_node)

        # Test the function
        combined_node_map = {}
        visited_nodes = set()
        result_node_map = _build_statement_node_map(standalone_node, visited_nodes, combined_node_map)

        # Verify the result
        self.assertEqual(len(result_node_map), 1)
        self.assertIn("standalone_table", result_node_map)
        self.assertEqual(result_node_map["standalone_table"].table_name, "standalone_table")

    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    def test_build_statement_node_map_w_shape_graph(self, mock_read_pipeline):
        """Test building node map for a W shape:
        src_a -> child_table_1
        src_b -> child_table_1
        src_b -> child_table_2
        src_c -> child_table_2
        """

        src_a = self._create_node(table_name="src_a", type="source")
        src_b = self._create_node(table_name="src_b", type="source")
        src_c = self._create_node(table_name="src_c", type="source")
        child_table_1 = self._create_node(table_name="child_table_1", type="fact")
        child_table_1.add_parent(src_a)
        child_table_1.add_parent(src_b)
        child_table_2 = self._create_node(table_name="child_table_2", type="fact")
        child_table_2.add_parent(src_c)
        child_table_2.add_parent(src_b)

        src_a_pipeline = self.create_mock_pipeline_definition(src_a)
        src_b_pipeline = self.create_mock_pipeline_definition(src_b)
        src_c_pipeline = self.create_mock_pipeline_definition(src_c)
        child_table_1_pipeline = self.create_mock_pipeline_definition(child_table_1)
        child_table_2_pipeline = self.create_mock_pipeline_definition(child_table_2)
        _mock_pipeline_map = {
            "src_a": src_a_pipeline,
            "src_b": src_b_pipeline,
            "src_c": src_c_pipeline,
            "child_table_1": child_table_1_pipeline,
            "child_table_2": child_table_2_pipeline
        }
        def mock_read_pipeline_side_effect(path):
            table_name = path.split("/")[-2]
            if table_name in _mock_pipeline_map:
                return _mock_pipeline_map[table_name]
            return None

        mock_read_pipeline.side_effect = mock_read_pipeline_side_effect

        # Test the function
        combined_node_map = {}
        visited_nodes = set()
        result_node_map = _build_statement_node_map(src_a, visited_nodes, combined_node_map)
        assert len(result_node_map) == 5
        assert result_node_map["src_a"].parents == set()
        assert result_node_map["src_b"].parents == set()
        assert result_node_map["src_a"].children == set([child_table_1])
        assert result_node_map["src_b"].children == set([child_table_1, child_table_2])
        assert result_node_map["child_table_1"].children == set()
        assert result_node_map["child_table_1"].parents == set([src_a, src_b])
        # same from child_table
        combined_node_map = {}
        visited_nodes = set()
        result_node_map = _build_statement_node_map(child_table_1, visited_nodes, combined_node_map)
        assert len(result_node_map) == 5
        assert result_node_map["src_a"].parents == set()
        assert result_node_map["src_b"].parents == set()
        assert result_node_map["src_a"].children == set([child_table_1])
        assert result_node_map["src_b"].children == set([child_table_1, child_table_2])
        assert result_node_map["child_table_1"].children == set()
        assert result_node_map["child_table_1"].parents == set([src_a, src_b])
        # same from child_table_2
        combined_node_map = {}
        visited_nodes = set()
        result_node_map = _build_statement_node_map(child_table_2, visited_nodes, combined_node_map)
        assert len(result_node_map) == 5


    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    def test_build_statement_node_map_complex_graph(self, mock_read_pipeline):
        """Test building node map for a complex graph with multiple parents one intermediate and children.
        src_a -> join_table_1 -> child_a
        src_b -> join_table_1
        child_a -> child_a_a
        src_b -> join_table_2 -> child_b
        src_c -> join_table_2
        src_c -> child_c
        """

        # Create additional nodes for complex graph
        src_a = self._create_node(table_name="src_a", type="source")
        src_b = self._create_node(table_name="src_b", type="source")
        src_c = self._create_node(table_name="src_c", type="source")
        child_a = self._create_node(table_name="child_a", type="intermediate")
        child_a_a = self._create_node(table_name="child_a_a", type="fact")
        child_a_a.add_parent(child_a)
        join_table_1 = self._create_node(table_name="join_table_1", type="intermediate")
        join_table_1.add_parent(src_a)
        join_table_1.add_parent(src_b)
        child_a.add_parent(join_table_1)
        join_table_2 = self._create_node(table_name="join_table_2", type="intermediate")
        join_table_2.add_parent(src_b)
        join_table_2.add_parent(src_c)
        child_b = self._create_node(table_name="child_b", type="fact")
        child_b.add_parent(join_table_2)
        child_c = self._create_node(table_name="child_c", type="fact")
        child_c.product_name = "common"
        child_c.add_parent(src_c)

        # Create mock pipeline definitions
        src_a_pipeline = self.create_mock_pipeline_definition(src_a)
        src_b_pipeline = self.create_mock_pipeline_definition(src_b)
        src_c_pipeline = self.create_mock_pipeline_definition(src_c)
        join_table_1_pipeline = self.create_mock_pipeline_definition(join_table_1)
        join_table_2_pipeline = self.create_mock_pipeline_definition(join_table_2)
        child_a_pipeline = self.create_mock_pipeline_definition(child_a)
        child_a_a_pipeline = self.create_mock_pipeline_definition(child_a_a)
        child_b_pipeline = self.create_mock_pipeline_definition(child_b)
        child_c_pipeline = self.create_mock_pipeline_definition(child_c)
        _mock_pipeline_map = {
            "src_a": src_a_pipeline,
            "src_b": src_b_pipeline,
            "src_c": src_c_pipeline,
            "join_table_1": join_table_1_pipeline,
            "join_table_2": join_table_2_pipeline,
            "child_a": child_a_pipeline,
            "child_a_a": child_a_a_pipeline,
            "child_b": child_b_pipeline,
            "child_c": child_c_pipeline
        }
        # Mock the read_pipeline_definition_from_file function
        def mock_read_pipeline_side_effect(path):
            table_name = path.split("/")[-2]
            if table_name in _mock_pipeline_map:
                return _mock_pipeline_map[table_name]
            return None

        mock_read_pipeline.side_effect = mock_read_pipeline_side_effect

        # Test the function
        combined_node_map = {}
        visited_nodes = set()
        result_node_map = _build_statement_node_map(child_b, visited_nodes, combined_node_map)

        # Verify the result
        self.assertEqual(len(result_node_map), 9)

    def test_build_node_map(self) -> None:
        """Test building node map from pipeline definition.

        Loading a pipeline definition for an intermediate table should get all reachable
        related tables. Direct descendants and ancestors should be included.
        """
        print("test_build node_map")
        combined_node_map = {}
        visited_nodes = set()
        pipeline_def = read_pipeline_definition_from_file(self.inventory_path + "/intermediates/p2/z/" + PIPELINE_JSON_FILE_NAME)
        if pipeline_def is None:
            raise ValueError("Failed to read pipeline definition")
        sink_node = pipeline_def.to_node()
        node_map = _build_statement_node_map(sink_node, visited_nodes, combined_node_map)

        assert len(node_map) == 14
        for node in node_map.values():
            print(node.table_name, node.upgrade_mode, node.dml_statement_name)

        assert node_map["src_y"].upgrade_mode == "Stateless"
        assert node_map["src_x"].upgrade_mode == "Stateless"
        assert node_map["src_b"].upgrade_mode == "Stateless"
        assert node_map["src_a"].upgrade_mode == "Stateless"
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


if __name__ == '__main__':
    unittest.main()

