"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import pathlib
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
from shift_left.core.utils.file_search import (
    EXTERNAL_KAFKA_TYPE,
    get_or_build_source_file_inventory,
    build_inventory,
    get_table_ref_from_inventory,
    create_folder_if_not_exist,
    get_ddl_dml_from_folder, 
    from_pipeline_to_absolute,
    from_absolute_to_pipeline,
    update_pipeline_definition_file,
    SCRIPTS_DIR,
    PIPELINE_JSON_FILE_NAME,
    read_pipeline_definition_from_file,
    FlinkStatementNode,
    FlinkTablePipelineDefinition,
    FlinkTableReference,
    extract_product_name,
    get_table_type_from_file_path,
    get_ddl_file_name,
    get_ddl_dml_names_from_table,
    list_src_sql_files,
    derive_table_type_product_name_from_path,
    get_ddl_dml_names_from_pipe_def,
    _apply_statement_naming_convention,
    _get_statement_name_modifier,
    DmlNameModifier
)

from shift_left.core.utils.app_config import get_config, logger

import json
import pathlib
"""
To be successful, the test_file_search.py has to be run from the folder above tests and the 
inventory and pipelines_definition.json files have to be present.
"""
class TestFileSearch(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent.parent / "./data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "spark-project")

    def test_table_ref_equality(self):
        ref1 = FlinkTableReference.model_validate({"table_name": "table1", "product_name": "p1", "type": "fact", "dml_ref": "dml1", "ddl_ref": "ddl1", "table_folder_name": "folder1" })
        ref2 = FlinkTableReference.model_validate({"table_name": "table1", "product_name": "p1", "type": "fact", "dml_ref": "dml1", "ddl_ref": "ddl1", "table_folder_name": "folder1" })
        self.assertEqual(ref1, ref2)
        self.assertEqual(ref1.__hash__(), ref2.__hash__())

    def test_table_ref_inequality(self):
        ref1 = FlinkTableReference.model_validate({"table_name": "table1", "product_name": "p1", "type": "fact", "dml_ref": "dml1", "ddl_ref": "ddl1", "table_folder_name": "folder1" })
        ref2 = FlinkTableReference.model_validate({"table_name": "table2", "product_name": "p1", "type": "fact", "dml_ref": "dml2", "ddl_ref": "ddl2", "table_folder_name": "folder2" })
        self.assertNotEqual(ref1, ref2)
        self.assertNotEqual(ref1.__hash__(), ref2.__hash__())  

    def test_flink_statement_node(self):
        node = FlinkStatementNode(table_name="root_node", product_name="p1")
        assert node.table_name
        assert not node.to_run
        child = FlinkStatementNode(table_name="child_1", product_name="p1")
        node.add_child(child)
        assert len(node.children) == 1
        assert len(child.parents) == 1

    def test_FlinkTablePipelineDefinition(self):
        pipe_def= FlinkTablePipelineDefinition(table_name="src_table",
                                               type="source",
                                               product_name="p1",
                                               path= "src/src_table",
                                               dml_ref="src/src_table/sql_scripts/dml.src_p1_table.sql",
                                               ddl_ref="src/src_table/sql_scripts/ddl.src_p1_table.sql")
        assert pipe_def
        assert pipe_def.path == "src/src_table"
        assert pipe_def.complexity.state_form == "Stateless"
        node = pipe_def.to_node()
        assert node.dml_statement_name == "dev-usw2-p1-dml-src-table"
        assert node.ddl_statement_name == "dev-usw2-p1-ddl-src-table"

    
    def test_read_pipeline_definition_from_file(self):
        result: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(os.getenv("PIPELINES") + "/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME)
        assert result

    def test_table_type(self):
        type= get_table_type_from_file_path( os.environ["PIPELINES"] + "/sources/src_table_1")
        assert type
        assert type == "source"
        type= get_table_type_from_file_path( os.environ["PIPELINES"] + "/facts/p1/fct_order")
        assert type
        assert type == "fact"
        type= get_table_type_from_file_path( os.environ["PIPELINES"] + "/intermediates/p1/int_table_1")
        assert type
        assert type == "intermediate"



    def test_path_transformation(self):
        path = "/user/bill/project/pipelines/dataproduct/sources/sql-scripts/ddl.table.sql"
        assert "pipelines/dataproduct/sources/sql-scripts/ddl.table.sql" == from_absolute_to_pipeline(path)
        path = "pipelines/dataproduct/sources/sql-scripts/ddl.table.sql"
        assert "pipelines/dataproduct/sources/sql-scripts/ddl.table.sql" == from_absolute_to_pipeline(path)

    def test_absolute_to_relative(self):
        path= "/home/bill/Code/shift_left_utils/examples/flink-project/pipelines"
        assert "pipelines" == from_absolute_to_pipeline(path)
    
    def test_relative_to_pipeline(self):
        test_path = "pipelines/facts/p1/fct_order"
        abs_path = from_pipeline_to_absolute(test_path)
        assert os.path.isabs(abs_path)
        assert abs_path.endswith(test_path)

    def test_build_src_inventory(self):
        """ given a source project, build the inventory of all the sql files """
        inventory_path= os.getenv("SRC_FOLDER")
        all_files= get_or_build_source_file_inventory(inventory_path)
        self.assertIsNotNone(all_files)
        self.assertGreater(len(all_files), 0)
        print(json.dumps(all_files, indent=3))


    def test_build_flink_sql_inventory(self):
        """ given a source project, build the inventory of all the sql files """
        inventory_path= os.getenv("PIPELINES")
        all_files= build_inventory(inventory_path)
        self.assertIsNotNone(all_files)
        self.assertGreater(len(all_files), 0)
        print(json.dumps(all_files, indent=3))
        print(all_files["src_table_1"])


    def test_get_table_ref_from_inventory(self):
        inventory_path= os.getenv("PIPELINES")
        i = build_inventory(inventory_path)
        ref = get_table_ref_from_inventory("p1_fct_order", i)
        assert ref
        assert ref.table_name == "p1_fct_order"


    def test_validate_ddl_dml_file_retrieved(self):
        inventory_path= os.getenv("PIPELINES")
        ddl, dml = get_ddl_dml_from_folder(inventory_path + "/facts/p1/fct_order", SCRIPTS_DIR)
        self.assertIsNotNone(ddl)
        self.assertIsNotNone(dml)
        print(ddl)


    def test_get_ddl_dml_names_from_table(self):
        ddl, dml = get_ddl_dml_names_from_table("fct_order")
        assert ddl == "ddl-fct-order"
        assert dml == "dml-fct-order"


    def test_dml_ddl_names(self):
        pipe_def = read_pipeline_definition_from_file( os.getenv("PIPELINES") + "/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME)
        config = get_config()
        ddl, dml = get_ddl_dml_names_from_pipe_def(pipe_def)
        assert ddl == "dev-usw2-p1-ddl-p1-fct-order"
        assert dml == "dev-usw2-p1-dml-p1-fct-order"

    def test_get_ddl_file_name(self):
        fname = get_ddl_file_name(os.getenv("PIPELINES") + "/facts/p1/fct_order/sql-scripts")
        assert fname
        assert "pipelines/facts/p1/fct_order/sql-scripts/ddl.p1_fct_order.sql" == fname
        print(fname)

    def test_extract_product_name(self):
        pname = extract_product_name(os.getenv("PIPELINES") + "/facts/p1/fct_order")
        assert "p1" == pname

    def test_derive_table_type_product_from_path(self):
        path = "pipelines/intermediates/p3/it2"
        table_type, product_name, table_name = derive_table_type_product_name_from_path(path)
        self.assertEqual(table_type, "intermediate")
        self.assertEqual(product_name, "p3")
        self.assertEqual(table_name, "it2")
        path = "pipelines/facts/p3/it2"
        table_type, product_name, table_name = derive_table_type_product_name_from_path(path)
        self.assertEqual(table_type, "fact")
        self.assertEqual(product_name, "p3")
        self.assertEqual(table_name, "it2")
        path = "pipelines/sources/p3/it2"
        table_type, product_name, table_name  = derive_table_type_product_name_from_path(path)
        self.assertEqual(table_type, "source")
        self.assertEqual(product_name, "p3")
        self.assertEqual(table_name, "it2")

    def test_get_ddl_dml_references(self):
        files = list_src_sql_files(os.getenv("PIPELINES")+ "/facts/p1/fct_order")
        assert files["ddl.p1_fct_order"]
        assert files["dml.p1_fct_order"]
        assert ".sql" in files["dml.p1_fct_order"]
        print(files)

    def test_create_folder_if_not_exist(self):
        """Test folder creation functionality"""
        import tempfile
        import shutil
        
        # Create a temporary directory
        temp_dir = tempfile.mkdtemp()
        try:
            # Test creating a new folder
            new_folder = os.path.join(temp_dir, "test_folder")
            result = create_folder_if_not_exist(new_folder)
            self.assertTrue(os.path.exists(new_folder))
            self.assertEqual(result, new_folder)
            
            # Test with existing folder
            result = create_folder_if_not_exist(new_folder)
            self.assertTrue(os.path.exists(new_folder))
            self.assertEqual(result, new_folder)
        finally:
            # Cleanup
            shutil.rmtree(temp_dir)

    def test_update_pipeline_definition_file(self):
        """Test updating pipeline definition file"""
        import tempfile
        import shutil
        
        temp_dir = tempfile.mkdtemp()
        try:
            # Create a test pipeline definition
            test_def = FlinkTablePipelineDefinition(
                table_name="test_table",
                product_name="test_product",
                type="fact",
                path="test/path",
                dml_ref="test/dml.sql",
                ddl_ref="test/ddl.sql"
            )
            
            # Test file creation and update
            file_path = os.path.join(temp_dir, "pipeline_definition.json")
            update_pipeline_definition_file(file_path, test_def)
            
            # Verify file was created and contains correct data
            self.assertTrue(os.path.exists(file_path))
            with open(file_path, 'r') as f:
                content = json.load(f)
                self.assertEqual(content['table_name'], "test_table")
                self.assertEqual(content['product_name'], "test_product")
        finally:
            shutil.rmtree(temp_dir)

    def test_apply_naming_convention(self):
        """Test naming convention application"""
        # Create a test node
        node = FlinkStatementNode(
            table_name="test_table",
            product_name="test_product",
            dml_statement_name="dml-test-table",
            ddl_statement_name="ddl-test-table"
        )
        
        # Apply naming convention
        modified_node = _apply_statement_naming_convention(node)
        
        # Verify naming convention was applied
        self.assertNotEqual(modified_node.dml_statement_name, "dml-test-table")
        self.assertNotEqual(modified_node.ddl_statement_name, "ddl-test-table")

    def test_get_statement_name_modifier(self):
        """Test statement name modifier retrieval"""
        # Test default modifier
        modifier = _get_statement_name_modifier()
        self.assertIsNotNone(modifier)
        self.assertIsInstance(modifier, DmlNameModifier)

    def test_table_type_edge_cases(self):
        """Test edge cases for table type detection"""
        # Test all possible table types
        test_cases = [
            ("/path/to/source/table", "source"),
            ("/path/to/intermediates/table", "intermediate"),
            ("/path/to/facts/table", "fact"),
            ("/path/to/dimensions/table", "dimension"),
            ("/path/to/stage/table", "intermediate"),
            ("/path/to/mv/table", "view"),
            ("/path/to/seed/table", "seed"),
            ("/path/to/dead_letter/table", "dead_letter"),
            ("/path/to/unknown/table", "unknown-type")
        ]
        
        for path, expected_type in test_cases:
            actual_type = get_table_type_from_file_path(path)
            self.assertEqual(actual_type, expected_type)

    def test_ddl_dml_from_folder_errors(self):
        """Test error cases for DDL/DML file retrieval"""
        import tempfile
        import shutil
        
        temp_dir = tempfile.mkdtemp()
        try:
            # Test missing DDL file
            scripts_dir = os.path.join(temp_dir, "sql-scripts")
            os.makedirs(scripts_dir)
            with self.assertRaises(Exception) as context:
                get_ddl_dml_from_folder(temp_dir, "sql-scripts")
            self.assertTrue("No DDL file found" in str(context.exception))
            
            # Test missing DML file - should not raise exception anymore, just return None for DML
            ddl_file = os.path.join(scripts_dir, "ddl.test.sql")
            with open(ddl_file, 'w') as f:
                f.write("CREATE TABLE test;")
            ddl_result, dml_result = get_ddl_dml_from_folder(temp_dir, "sql-scripts")
            # Should return the DDL file path and None for missing DML
            self.assertEqual(ddl_result, ddl_file)
            self.assertIsNone(dml_result)
        finally:
            shutil.rmtree(temp_dir)

    def test_extract_product_name_edge_cases(self):
        """Test edge cases for product name extraction"""
        test_cases = [
            ("/path/to/facts/product1/table", "product1"),
            ("/path/to/intermediates/product2/table", "product2"),
            ("/path/to/sources/product3/table", "product3"),
            ("/path/to/dimensions/product4/table", "product4"),
            ("/path/to/views/product5/table", "product5"),
            ("/path/to/facts/table", 'None'),  # No product name
            ("/path/to/unknown/table", "unknown")  # Unknown structure
        ]
        
        for path, expected_product in test_cases:
            actual_product = extract_product_name(path)
            self.assertEqual(actual_product, expected_product)

    def test_merge_seeds_external_tables_into_inventory(self):
        import tempfile
        import shutil

        from shift_left.core.utils.file_search import merge_seeds_external_tables_into_inventory

        td = tempfile.mkdtemp()
        try:
            seeds = os.path.join(td, "pipelines", "seeds")
            os.makedirs(seeds)
            with open(os.path.join(seeds, "external_tables.json"), "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "tables": [
                            {
                                "table_name": "ext_dim",
                                "kafka_topic": "topic.a",
                                "product_name": "ldg",
                            }
                        ]
                    },
                    f,
                )
            inv = {
                "local_t": {
                    "table_name": "local_t",
                    "type": "source",
                    "dml_ref": "x",
                    "ddl_ref": "y",
                    "table_folder_name": "f",
                    "product_name": "p",
                }
            }
            merge_seeds_external_tables_into_inventory(inv, os.path.join(td, "pipelines"))
            self.assertIn("ext_dim", inv)
            self.assertEqual(inv["ext_dim"]["type"], EXTERNAL_KAFKA_TYPE)
            self.assertEqual(inv["ext_dim"]["kafka_topic"], "topic.a")
            self.assertEqual(inv["ext_dim"]["dml_ref"], "")
        finally:
            shutil.rmtree(td)

    def test_get_pipeline_definition_for_external_kafka_table(self):
        from unittest.mock import patch

        from shift_left.core import pipeline_mgr

        ref = FlinkTableReference.model_validate(
            {
                "table_name": "ext_only",
                "type": "external_kafka",
                "product_name": "common",
                "dml_ref": "",
                "ddl_ref": "",
                "table_folder_name": "pipelines/seeds/ext_only",
                "kafka_topic": "t.ext",
            }
        )
        inv = {"ext_only": ref.model_dump()}
        with patch(
            "shift_left.core.pipeline_mgr.get_or_build_inventory", return_value=inv
        ), patch(
            "shift_left.core.pipeline_mgr.get_table_ref_from_inventory", return_value=ref
        ):
            pd = pipeline_mgr.get_pipeline_definition_for_table("ext_only", "/fake")
        self.assertEqual(pd.table_name, "ext_only")
        self.assertEqual(pd.type, "external_kafka")
        self.assertEqual(pd.kafka_topic, "t.ext")
        self.assertEqual(pd.dml_ref, "")

    def test_deploy_one_node_skips_external_kafka(self):
        from shift_left.core.deployment_mgr import _deploy_one_node
        from shift_left.core.models.flink_statement_model import FlinkStatementNode, StatementError

        node = FlinkStatementNode(
            table_name="ext_topic",
            type=EXTERNAL_KAFKA_TYPE,
            path="pipelines/seeds/ext_topic",
            compute_pool_id="pool-1",
        )
        result = _deploy_one_node(node, accept_exceptions=True, compute_pool_id="pool-1")
        self.assertIsInstance(result, StatementError)

if __name__ == '__main__':
    unittest.main()