"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
from shift_left.core.utils.file_search import (
    get_or_build_source_file_inventory, 
    build_inventory,
    get_table_ref_from_inventory,
    get_ddl_dml_from_folder, 
    from_pipeline_to_absolute,
    from_absolute_to_pipeline,
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
    get_ddl_dml_names_from_pipe_def)
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
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")

    def test_table_ref_equality(self):
        ref1 = FlinkTableReference.model_validate({"table_name": "table1", "type": "fact", "dml_ref": "dml1", "ddl_ref": "ddl1", "table_folder_name": "folder1" })
        ref2 = FlinkTableReference.model_validate({"table_name": "table1", "type": "fact", "dml_ref": "dml1", "ddl_ref": "ddl1", "table_folder_name": "folder1" })
        self.assertEqual(ref1, ref2)
        self.assertEqual(ref1.__hash__(), ref2.__hash__())

    def test_table_ref_inequality(self):
        ref1 = FlinkTableReference.model_validate({"table_name": "table1", "type": "fact", "dml_ref": "dml1", "ddl_ref": "ddl1", "table_folder_name": "folder1" })
        ref2 = FlinkTableReference.model_validate({"table_name": "table2", "type": "fact", "dml_ref": "dml2", "ddl_ref": "ddl2", "table_folder_name": "folder2" })
        self.assertNotEqual(ref1, ref2)
        self.assertNotEqual(ref1.__hash__(), ref2.__hash__())  

    def test_flink_statement_node(self):
        node = FlinkStatementNode(table_name="root_node")
        assert node.table_name
        assert not node.to_run
        child = FlinkStatementNode(table_name="child_1")
        node.add_child(child)
        assert len(node.children) == 1
        assert len(child.parents) == 1

    def test_FlinkTablePipelineDefinition(self):
        pipe_def= FlinkTablePipelineDefinition(table_name="src_table",
                                               type="source",
                                               path= "src/src_table",
                                               dml_ref="src/src_table/sql_scripts/dml.file.sql",
                                               ddl_ref="src/src_table/sql_scripts/ddl.file.sql")
        assert pipe_def
        assert pipe_def.path == "src/src_table"
        assert pipe_def.state_form == "Stateful"
    
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
        assert "flink-project/pipelines/facts/p1/fct_order"  in from_pipeline_to_absolute(test_path)

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
        ddl, dml = get_ddl_dml_names_from_table("fct_order", "dev")
        assert ddl == "dev-ddl-fct-order"
        assert dml == "dev-dml-fct-order"

    def test_dml_ddl_names(self):
        pipe_def = read_pipeline_definition_from_file( os.getenv("PIPELINES") + "/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME)
        config = get_config()
        ddl, dml = get_ddl_dml_names_from_pipe_def(pipe_def,  config['kafka']['cluster_type'])
        assert ddl == "dev-ddl-p1-fct-order"
        assert dml == "dev-dml-p1-fct-order"

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

if __name__ == '__main__':
    unittest.main()