"""
Tests for RAG corpus loader: load_ksql_flink_corpus and ExamplePair.
"""
import unittest
import pathlib
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
data_dir = pathlib.Path(__file__).parent.parent / "data"

from shift_left.ai.rag.corpus_loader import ExamplePair, load_ksql_flink_corpus


class TestCorpusLoader(unittest.TestCase):
    def test_load_ksql_flink_corpus_returns_list(self):
        corpus_root = data_dir / "ksql-project"
        if not corpus_root.is_dir():
            self.skipTest("ksql-project data not found")
        pairs = load_ksql_flink_corpus(corpus_root)
        self.assertIsInstance(pairs, list)
        self.assertGreater(len(pairs), 0, "Expected at least one example pair from ksql-project")
        for p in pairs:
            self.assertIsInstance(p, ExamplePair)
            self.assertTrue(p.ksql_text.strip(), f"Example {p.name} has empty ksql_text")
            self.assertTrue(p.flink_ddl.strip(), f"Example {p.name} has empty flink_ddl")
            self.assertIsInstance(p.flink_dml, str)

    def test_example_pair_defaults(self):
        p = ExamplePair(name="x", ksql_text="k", flink_ddl="d", flink_dml="")
        self.assertEqual(p.flink_dml, "")
        p2 = ExamplePair(name="y", ksql_text="k", flink_ddl="", flink_dml=None)
        self.assertEqual(p2.flink_ddl, "")
        self.assertEqual(p2.flink_dml, "")
