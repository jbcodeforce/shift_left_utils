"""
Unit tests for the _filtering_descendant_nodes method in deployment_mgr.py
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
from datetime import datetime
from typing import List

# Set up environment for testing
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

from shift_left.core.deployment_mgr import _filtering_out_descendant_nodes, FlinkStatementNode
from shift_left.core.utils.app_config import get_config
from ut.core.BaseUT import BaseUT


class TestFilteringAncestors(BaseUT):
    """Test suite for the _filtering_descendant_nodes function."""

    def setUp(self) -> None:
        """Set up test case before each test."""
        super().setUp()
        self.product_name = "p1"
        self.config = get_config()

    def _create_flink_statement_node(
        self,
        table_name: str,
        product_name: str = None,
        children: List[FlinkStatementNode] = None
    ) -> FlinkStatementNode:
        """Create a FlinkStatementNode for testing."""
        node = FlinkStatementNode(
            table_name=table_name,
            product_name=product_name or self.product_name,
            created_at=datetime.now()
        )

        # Add children if provided
        if children:
            for child in children:
                node.add_child(child)

        return node

    def test_filtering_descendant_nodes_same_product_name(self):
        """Test that ancestors with the same product name are not filtered out."""
        # Arrange
        ancestor1 = self._create_flink_statement_node("table1", self.product_name)
        ancestor2 = self._create_flink_statement_node("table2", self.product_name)
        ancestor3 = self._create_flink_statement_node("table3", self.product_name)
        ancestors = [ancestor1, ancestor2, ancestor3]

        # Act
        result = _filtering_out_descendant_nodes(
            ancestors=ancestors,
            product_name=self.product_name,
            may_start_descendants=True
        )

        # Assert
        self.assertEqual(len(result), 3)
        self.assertIn(ancestor1, result)
        self.assertIn(ancestor2, result)
        self.assertIn(ancestor3, result)

    def test_filtering_descendant_nodes_different_product_with_children_in_expected_product(self):
        """Test that ancestors with different product name
        but children in expected product are not filtered."""
        # Arrange
        child_in_expected_product = self._create_flink_statement_node("child1", self.product_name)
        ancestor_different_product = self._create_flink_statement_node(
            "ancestor1",
            "different_product",
            children=[child_in_expected_product]
        )
        ancestors = [ancestor_different_product, child_in_expected_product]

        # Act
        result = _filtering_out_descendant_nodes(
            ancestors=ancestors,
            product_name=self.product_name,
            may_start_descendants=False # not relevant for this test
        )

        # Assert
        self.assertEqual(len(result), 2)
        self.assertIn(ancestor_different_product, result)
        self.assertIn(child_in_expected_product, result)

    @patch('shift_left.core.deployment_mgr.get_config')
    def test_filtering_descendant_nodes_different_product_mixed_children_product(self, mock_get_config):
        """Test children with different product name will be filtered out as ancestor with different
        product name will be there but not its children
        """
        # Arrange
        mock_config = {
            'app': {'accepted_common_products': ['common']}
        }
        mock_get_config.return_value = mock_config
        child_same_product = self._create_flink_statement_node("child1", self.product_name)
        child_different_product = self._create_flink_statement_node("child2", "different_product")
        ancestor_different_product = self._create_flink_statement_node(
            "ancestor1",
            "common",
            children=[child_same_product,child_different_product]
        )
        child_different_product = self._create_flink_statement_node("child2", "different_product")
        ancestors = [ancestor_different_product, child_same_product, child_different_product]

        # Act
        result = _filtering_out_descendant_nodes(
            ancestors=ancestors,
            product_name=self.product_name,
            may_start_descendants=True
        )

        # Assert
        self.assertEqual(len(result), 2)
        self.assertIn(ancestor_different_product, result)
        self.assertNotIn(child_different_product, result)

    def test_filtering_descendant_nodes_different_product_no_children(self):
        """Test that ancestors with different product name and no children are filtered."""
        # Arrange
        ancestor_different_product = self._create_flink_statement_node("ancestor1", "different_product")
        ancestors = [ancestor_different_product]

        # Act
        result = _filtering_out_descendant_nodes(
            ancestors=ancestors,
            product_name=self.product_name,
            may_start_descendants=True
        )

        # Assert
        self.assertEqual(len(result), 0)

    def test_filtering_descendant_nodes_accepted_common_products(self):
        """Test that ancestors from accepted_common_products are not filtered."""
        # Arrange
        # From config.yaml: accepted_common_products: ['common', 'seeds']
        ancestor_common = self._create_flink_statement_node("ancestor1", "common")
        ancestor_seeds = self._create_flink_statement_node("ancestor2", "seeds")
        ancestors = [ancestor_common, ancestor_seeds]

        # Act
        result = _filtering_out_descendant_nodes(
            ancestors=ancestors,
            product_name=self.product_name,
            may_start_descendants=True
        )

        # Assert
        self.assertEqual(len(result), 2)
        self.assertIn(ancestor_common, result)
        self.assertIn(ancestor_seeds, result)


    @patch('shift_left.core.deployment_mgr.get_config')
    def test_filtering_descendant_nodes_mixed_scenarios(self, mock_get_config):
        """Test a complex scenario with multiple ancestors of different types."""
        # Arrange
        mock_config = {
            'app': {'accepted_common_products': ['common']}
        }
        mock_get_config.return_value = mock_config
        # Same product - should not be filtered
        ancestor_same_product = self._create_flink_statement_node("table1", self.product_name)

        # Different product with child in expected product - should not be filtered
        child_in_expected_product = self._create_flink_statement_node("child1", self.product_name)
        ancestor_same_product.add_child(child_in_expected_product)
        ancestor_with_expected_child = self._create_flink_statement_node(
            "table2",
            "different_product",
            children=[child_in_expected_product]
        )

        # Different product with no children in expected product - should be filtered
        child_different_product = self._create_flink_statement_node("child2", "different_product")
        ancestor_to_filter = self._create_flink_statement_node(
            "table3",
            "different_product",
            children=[child_different_product]
        )

        # Common product - should not be filtered
        child_4_different_product = self._create_flink_statement_node("child4", "different_product")
        ancestor_common = self._create_flink_statement_node("table4", "common", children=[child_4_different_product])


        ancestors = [
            ancestor_same_product,
            ancestor_with_expected_child,
            ancestor_to_filter,
            ancestor_common,
            child_in_expected_product,
            child_different_product,
            child_4_different_product
        ]

        # Act
        result = _filtering_out_descendant_nodes(
            ancestors=ancestors,
            product_name=self.product_name,
            may_start_descendants=True
        )

        # Assert
        self.assertEqual(len(result), 4)
        self.assertIn(ancestor_same_product, result)
        self.assertIn(ancestor_with_expected_child, result)
        self.assertNotIn(ancestor_to_filter, result)
        self.assertIn(ancestor_common, result)

    def test_filtering_descendant_nodes_empty_list(self):
        """Test that an empty list of ancestors returns an empty list."""
        # Arrange
        ancestors = []

        # Act
        result = _filtering_out_descendant_nodes(
            ancestors=ancestors,
            product_name=self.product_name,
            may_start_descendants=True
        )

        # Assert
        self.assertEqual(len(result), 0)
        self.assertEqual(result, [])

    def test_filtering_descendant_nodes_maintains_original_list_order(self):
        """Test that the order of ancestors is maintained after filtering."""
        # Arrange
        ancestor1 = self._create_flink_statement_node("table1", self.product_name)
        ancestor2 = self._create_flink_statement_node("table2", self.product_name)
        ancestor3 = self._create_flink_statement_node("table3", self.product_name)
        ancestors = [ancestor1, ancestor2, ancestor3]

        # Act
        result = _filtering_out_descendant_nodes(
            ancestors=ancestors,
            product_name=self.product_name,
            may_start_descendants=True
        )

        # Assert
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], ancestor1)
        self.assertEqual(result[1], ancestor2)
        self.assertEqual(result[2], ancestor3)

    @patch('shift_left.core.deployment_mgr.get_config')
    def test_filtering_descendant_nodes_multiple_children_mixed_products(self, mock_get_config):
        """Test ancestor with multiple children where some are in expected product."""
        # Arrange
        child_expected = self._create_flink_statement_node("child1", self.product_name)
        child_different = self._create_flink_statement_node("child2", "different_product")

        ancestor = self._create_flink_statement_node("ancestor", "different_product")
        ancestor.add_child(child_expected)
        ancestor.add_child(child_different)

        ancestors = [ancestor]

        # Act
        result = _filtering_out_descendant_nodes(
            ancestors=ancestors,
            product_name=self.product_name,
            may_start_descendants=True
        )

        # Assert
        self.assertEqual(len(result), 1)
        self.assertIn(ancestor, result)

    @patch('shift_left.core.deployment_mgr.get_config')
    def test_filtering_descendant_nodes_missing_accepted_common_products_config(self, mock_get_config):
        """Test behavior when accepted_common_products is not in config."""
        # Arrange
        mock_config = {
            'app': {}  # Missing accepted_common_products
        }
        mock_get_config.return_value = mock_config

        ancestor_different_product = self._create_flink_statement_node("ancestor1", "different_product")
        ancestors = [ancestor_different_product]

        # Act & Assert
        with self.assertRaises(KeyError):
            _filtering_out_descendant_nodes(
                ancestors=ancestors,
                product_name=self.product_name,
                may_start_descendants=True
            )

    @patch('shift_left.core.deployment_mgr.get_config')
    def test_keeping_common_product_with_children_in_expected_product(self, mock_get_config):
        """Test behavior when ancestor is part od accepted_common_products and has children in expected product."""
        # Arrange
        mock_config = {
            'app': {'accepted_common_products': ['common']}
        }
        mock_get_config.return_value = mock_config
        ancestor_common = self._create_flink_statement_node("ancestor1", "common")
        ancestor_different_product = self._create_flink_statement_node("ancestor2", "other")
        child_in_expected_product = self._create_flink_statement_node("child1", self.product_name)
        ancestor_different_product.add_child(child_in_expected_product)
        ancestor_common.add_child(child_in_expected_product)
        ancestors = [ancestor_different_product, ancestor_common, child_in_expected_product]

        # Act & Assert
        result = _filtering_out_descendant_nodes(
                ancestors=ancestors,
                product_name=self.product_name,
                may_start_descendants=True
            )
        self.assertEqual(len(result), 3)
        self.assertIn(ancestor_common, result)
        self.assertIn(child_in_expected_product, result)

    @patch('shift_left.core.deployment_mgr.get_config')
    def test_keeping_common_product_as_part_of_descendants(self, mock_get_config):
        """Test behavior when ancestor is part od accepted_common_products and has children in different product."""
        # Arrange
        mock_config = {
            'app': {'accepted_common_products': ['common']}
        }
        mock_get_config.return_value = mock_config
        ancestor_common = self._create_flink_statement_node("ancestor1", "common")
        ancestor_different_product = self._create_flink_statement_node("ancestor2", "other")
        child_in_expected_product = self._create_flink_statement_node("child1", self.product_name)
        ancestor_different_product.add_child(child_in_expected_product)
        ancestor_different_product.add_child(ancestor_common)
        ancestors = [ancestor_different_product, ancestor_common, child_in_expected_product]

        # Act & Assert
        result = _filtering_out_descendant_nodes(
                ancestors=ancestors,
                product_name=self.product_name,
                may_start_descendants=True
            )
        self.assertEqual(len(result), 3)
        self.assertIn(ancestor_common, result)
        self.assertIn(child_in_expected_product, result)

    def test_filtering_descendant_nodes_node_without_product_name(self):
        """Test filtering when a node has None as product_name."""
        # Arrange
        ancestor_no_product = self._create_flink_statement_node("ancestor1")
        ancestor_no_product.product_name = None
        ancestors = [ancestor_no_product]

        # Act
        result = _filtering_out_descendant_nodes(
            ancestors=ancestors,
            product_name=self.product_name,
            may_start_descendants=True
        )

        # Assert
        # Since product_name is None (not equal to expected product) and may_start_descendants is False,
        # and assuming None is not in accepted_common_products, it should be filtered
        self.assertEqual(len(result), 0)


if __name__ == '__main__':
    unittest.main()
