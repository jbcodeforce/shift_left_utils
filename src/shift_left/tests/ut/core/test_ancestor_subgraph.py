import unittest

import shift_left.core.deployment_mgr as dm
from shift_left.core.models.flink_statement_model import FlinkStatementNode


class TestAncestorSubgraph(unittest.TestCase):
    def setUp(self):
        self._orig_statement_mgr = dm.statement_mgr

        class _DummyStatementMgr:
            def get_statement_status_with_cache(self, name):
                return None

        dm.statement_mgr = _DummyStatementMgr()

    def tearDown(self):
        dm.statement_mgr = self._orig_statement_mgr

    def test_simple_chain(self):
        node_c = FlinkStatementNode(
            table_name="C",
            dml_statement_name="C",
        )
        node_b = FlinkStatementNode(
            table_name="B",
            dml_statement_name="B",
        )
        node_a = FlinkStatementNode(
            table_name="A",
            dml_statement_name="A",
        )

        node_b.add_parent(node_c)
        node_a.add_parent(node_b)

        node_map = {
            "A": node_a,
            "B": node_b,
            "C": node_c,
        }

        ancestors, dependencies = dm._get_ancestor_subgraph(node_a, node_map)

        self.assertEqual(set(ancestors.keys()), {"A", "B", "C"})
        normalized = {(child, parent.table_name) for (child, parent) in dependencies}
        self.assertEqual(normalized, {("A", "B"), ("B", "C")})

    def test_cycle_detection(self):
        # Create a cycle A <-> B
        node_a = FlinkStatementNode(
            table_name="A",
            dml_statement_name="A",
        )
        node_b = FlinkStatementNode(
            table_name="B",
            dml_statement_name="B",
        )

        node_a.add_parent(node_b)
        node_b.add_parent(node_a)

        node_map = {
            "A": node_a,
            "B": node_b,
        }

        ancestors, dependencies = dm._get_ancestor_subgraph(node_a, node_map)

        # Should not hang; ancestors should include both nodes
        self.assertEqual(set(ancestors.keys()), {"A", "B"})
        # Dependencies should include both directions, but no duplicates
        normalized = {(child, parent.table_name) for (child, parent) in dependencies}
        self.assertEqual(normalized, {("A", "B"), ("B", "A")})

    def test_complex_chain(self):
        node_f = FlinkStatementNode(
            table_name="F",
            dml_statement_name="F",
        )
        node_e = FlinkStatementNode(
            table_name="E",
            dml_statement_name="E",
        )
        node_d = FlinkStatementNode(
            table_name="D",
            dml_statement_name="D",
        )
        node_c = FlinkStatementNode(
            table_name="C",
            dml_statement_name="C",
        )
        node_b = FlinkStatementNode(
            table_name="B",
            dml_statement_name="B",
        )
        node_a = FlinkStatementNode(
            table_name="A",
            dml_statement_name="A",
        )

        node_f.add_parent(node_e)
        node_e.add_parent(node_d)
        node_e.add_parent(node_c)
        node_c.add_parent(node_a)
        node_b.add_parent(node_a)
        node_d.add_parent(node_b)

        node_map = {    
            "A": node_a,
            "B": node_b,
            "C": node_c,
            "D": node_d,
            "E": node_e,
            "F": node_f,
        }

        ancestors, dependencies = dm._get_ancestor_subgraph(node_f, node_map)

        self.assertEqual(set(ancestors.keys()), {"A", "B", "C", "D", "E", "F"})
        for (child, parent) in dependencies:
            print(f"Child: {child}, Parent: {parent.table_name}")
        normalized = {(child, parent.table_name) for (child, parent) in dependencies}
        self.assertEqual(normalized, {("F", "E"), ("E", "D"), ("E", "C"), ("C", "A"), ("D", "B"), ("B", "A")})

    def test_pass_list_of_tables_to_build_ancestor_sorted_graph(self):
        """
        Test to pass a list of table to build the ancestor sorted graph
        see https://github.com/jbcodeforce/shift_left_utils/blob/main/docs/images/flink_pipeline_for_test.drawio.png 
        """
        node_map = {}
        node_map["src_x"] = FlinkStatementNode(table_name="src_x")
        node_map["src_y"] = FlinkStatementNode(table_name="src_y")
        node_map["src_b"] = FlinkStatementNode(table_name="src_b")
        node_map["src_a"] = FlinkStatementNode(table_name="src_a")
        node_map["b"] = FlinkStatementNode(table_name="b", parents=[node_map["src_b"]])
        node_map["a"] = FlinkStatementNode(table_name="a", parents=[node_map["src_x"], node_map["src_a"]])
        node_map["x"] = FlinkStatementNode(table_name="x", parents=[node_map["src_x"]])
        node_map["y"] = FlinkStatementNode(table_name="y", parents=[node_map["src_y"]])
        
        node_map["z"] = FlinkStatementNode(table_name="z", parents=[node_map["x"], node_map["y"]])
        node_map["d"] = FlinkStatementNode(table_name="d", parents=[node_map["z"], node_map['y']])
        node_map["c"] = FlinkStatementNode(table_name="c", parents=[node_map["z"], node_map["b"]])

        node_map["p"] = FlinkStatementNode(table_name="p", parents=[node_map["z"]])
        node_map["e"] = FlinkStatementNode(table_name="e", parents=[node_map["c"]])
        node_map["f"] = FlinkStatementNode(table_name="f", parents=[node_map["d"]])

        table_list = ['x','y','z','a','b']   # intermediate tables of the graph
        merged_ancestors = {}
        merged_dependencies = []
        for node in table_list:
            ancestors, dependencies = dm._get_ancestor_subgraph(node_map[node], node_map)
            normalized = {(child, parent.table_name) for (child, parent) in dependencies}
            print(f"normalized: {normalized}")
            merged_ancestors.update(ancestors)
            for dep in dependencies:
                merged_dependencies.append(dep)
        assert len(merged_ancestors) == 9
        assert len(merged_dependencies) == 9
        sorted_nodes = dm._topological_sort(merged_ancestors, merged_dependencies)
        for node in sorted_nodes:
            print(f"{node.table_name}")
        assert len(sorted_nodes) == 9
        assert sorted_nodes[0].table_name in ["src_x", "src_a", "src_y", "src_b"]
        assert sorted_nodes[4].table_name in ["x", "y", "a", "b"]
        assert sorted_nodes[8].table_name == "z"

if __name__ == "__main__":
    unittest.main()