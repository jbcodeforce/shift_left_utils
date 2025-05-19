"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
from shift_left.core.utils.naming_convention import DmlNameModifier, ComputePoolNameModifier
from shift_left.core.models.flink_statement_model import FlinkStatementNode


class TestNamingConvention(unittest.TestCase):

    def test_get_dml_name(self):
       transformer = DmlNameModifier()
       name = transformer.modify_statement_name(FlinkStatementNode(table_name="table1", product_name="p1"), "dml-table1", "dev")
       assert name == "dev-p1-dml-table1"

    def test_get_compute_pool_name(self):
        transformer = ComputePoolNameModifier()
        name = transformer.build_compute_pool_name_from_table("table1")
        assert name == "dev-table1"

    def test_get_compute_pool_name_from_table(self):
        transformer = ComputePoolNameModifier()
        name = transformer.build_compute_pool_name_from_table("src_p1_table1")
        assert name == "dev-src-p1-table1"

    def test_get_compute_pool_name_from_table_recordconfiguration(self):
        transformer = ComputePoolNameModifier()
        name = transformer.build_compute_pool_name_from_table("src_p1_recordconfiguration")
        assert name == "dev-src-p1-reccfg"

    def test_get_compute_pool_name_from_table_recordexecution(self):
        transformer = ComputePoolNameModifier()
        name = transformer.build_compute_pool_name_from_table("src_p1_recordexecution")
        assert name == "dev-src-p1-recexe"

    def test_get_compute_pool_name_from_table_recordexecution_with_prefix(self):
        transformer = ComputePoolNameModifier()
        name = transformer.build_compute_pool_name_from_table("int_aqem_tag_tag_dummy")
        assert name == "dev-int-aqem-tag-tag-dummy"
        
if __name__ == '__main__':
    unittest.main()