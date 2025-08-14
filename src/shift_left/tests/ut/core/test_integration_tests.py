"""
Copyright 2024-2025 Confluent, Inc.

TDD: Tests for initializing integration tests scaffolding for a pipeline.
"""
import os
import pathlib
import shutil
import unittest

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

import shift_left.core.pipeline_mgr as pm
from shift_left.core.utils.file_search import PIPELINE_JSON_FILE_NAME
from ut.core.BaseUT import BaseUT
from shift_left.core.utils.file_search import get_or_build_inventory

class TestIntegrationTestsInit(BaseUT):
    """
    TDD: validate that init_integration_test_for_pipeline creates a tests folder structure
    with insert statements for source tables and validation SQLs for relevant intermediates
    and the target table.
   """

    @classmethod
    def setUpClass(cls) -> None:
        # Ensure pipeline definitions are (re)built from the test data
        pm.delete_all_metada_files(os.getenv("PIPELINES"))
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES"))
        get_or_build_inventory(os.getenv("PIPELINES"), os.getenv("PIPELINES"), False)

    def setUp(self) -> None:
        super().setUp()
        self.inventory_path = os.getenv("PIPELINES")

    def _cleanup_tests_dir(self, product_name: str, table_name: str) -> None:
        tests_dir = pathlib.Path(self.inventory_path) / "tests" / product_name / table_name
        if tests_dir.exists():
            shutil.rmtree(tests_dir)

   

    def _assert_files_exist(self, base_dir: pathlib.Path, file_names: list[str]) -> None:
        for fn in file_names:
            self.assertTrue((base_dir / fn).exists(), f"Expected file missing: {(base_dir / fn)}")

    def _test_init_for_leaf_table_f_creates_inserts_and_validations(self) -> None:
        """
        For target table 'f' (leaf), expect:
        - inserts for sources feeding the pipeline (src_x, src_y)
        - validations for multi-parent intermediates (x,y,z, d)
        - validation for the target table (f)
        """
        table_name = "f"
        product_name = self._get_product_name(table_name)
        self._cleanup_tests_dir(product_name, table_name)

        # Act
        pm.init_integration_test_for_pipeline(table_name=table_name, pipeline_path=self.inventory_path)

        # Assert expected structure and files
        base_dir = pathlib.Path(self.inventory_path) / "tests" / product_name / table_name
        self.assertTrue(base_dir.exists(), f"Expected tests dir not created: {base_dir}")

        expected_inserts = [
            "insert_src_x.sql",
            "insert_src_y.sql",
        ]
        expected_validations = [
            "validate_x.sql",
            "validate_y.sql",
            "validate_z.sql",
            "validate_d.sql",
            "validate_f.sql",
        ]
        self._assert_files_exist(base_dir, expected_inserts + expected_validations)

    def test_init_for_leaf_table_f_creates_inserts_and_validations(self) -> None:
        table_name = "f"
        pm.init_integration_test_for_pipeline(table_name=table_name, pipeline_path=self.inventory_path)


    def test_init_with_unknown_table_raises(self) -> None:
        """
        Unknown table should raise an exception.
        """
        with self.assertRaises(Exception):
            pm.init_integration_test_for_pipeline(table_name="unknown_table_xyz", pipeline_path=self.inventory_path)


if __name__ == "__main__":
    unittest.main()

