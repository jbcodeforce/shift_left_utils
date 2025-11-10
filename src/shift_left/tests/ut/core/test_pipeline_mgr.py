"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")

import shift_left.core.pipeline_mgr as pm
import shift_left.core.table_mgr as tm
from shift_left.core.utils.file_search import (
    PIPELINE_JSON_FILE_NAME,
    FlinkTablePipelineDefinition
)
from shift_left.core.pipeline_mgr import (
    PipelineReport
)


class TestPipelineManager(unittest.TestCase):


    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent.parent / "data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
        pm.delete_all_metada_files(os.getenv("PIPELINES"))
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES"))

    def test_PipelineReport(self):
        report: PipelineReport = PipelineReport(table_name="fct_order",
                                                path = "facts/p1/fct_order")
        assert report
        assert report.table_name == "fct_order"

    def test_build_a_src_pipeline_def(self):
        print("test_build_a_src_pipelinedef")
        path= os.getenv("PIPELINES")
        pm.delete_all_metada_files(path)
        dml_table_path=path + "/sources/c360/src_groups/sql-scripts/dml.src_c360_groups.sql"
        ddl_table_path=path + "/sources/c360/src_groups/sql-scripts/ddl.src_c360_groups.sql"
        result = pm.build_pipeline_definition_from_ddl_dml_content(dml_table_path, ddl_table_path, path)
        assert result
        assert result.table_name == "src_c360_groups"
        assert len(result.parents) == 0
        assert len(result.children) == 0
        assert "source" == result.type
        print(result.model_dump_json(indent=3))

    def test_1_build_pipeline_def_for_fact_table(self):
        """ Need to run this one first"""
        print("test_1_build_pipeline_def_for_fact_table")
        path= os.getenv("PIPELINES")
        dml_table_path=path + "/facts/p1/fct_order/sql-scripts/dml.p1_fct_order.sql"
        ddl_table_path=path + "/facts/p1/fct_order/sql-scripts/ddl.p1_fct_order.sql"
        result = pm.build_pipeline_definition_from_ddl_dml_content(dml_table_path, ddl_table_path, path)
        assert result
        assert len(result.children) == 0
        assert len(result.parents) == 2
        print(result.model_dump_json(indent=3))
        print("Verify intermediate tables updated")
        pipe_def: FlinkTablePipelineDefinition = pm.read_pipeline_definition_from_file(path + "/intermediates/p1/int_table_1/" + PIPELINE_JSON_FILE_NAME)
        assert pipe_def
        assert len(pipe_def.children) == 1
        assert len(pipe_def.parents) == 2
        pipe_def: FlinkTablePipelineDefinition = pm.read_pipeline_definition_from_file(path + "/intermediates/p1/int_table_2/" + PIPELINE_JSON_FILE_NAME)
        assert pipe_def
        assert len(pipe_def.children) == 1
        assert len(pipe_def.parents) == 1

    def test_all_pipeline_def(self):
        pm.delete_all_metada_files(os.getenv("PIPELINES"))
        pm.build_all_pipeline_definitions( os.getenv("PIPELINES"))
        assert os.path.exists(os.getenv("PIPELINES") + "/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME)
        assert os.path.exists(os.getenv("PIPELINES") + "/facts/c360/fct_user_per_group/" + PIPELINE_JSON_FILE_NAME)

    def test_visit_parents(self):
        print("test_visit_parents")
        path= os.getenv("PIPELINES")
        table_path=path + "/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME
        pipeline_def: FlinkTablePipelineDefinition = pm.read_pipeline_definition_from_file(table_path)
        assert pipeline_def
        pm._visit_parents(pipeline_def)
        print(pipeline_def.model_dump_json(indent=3))
        assert len(pipeline_def.parents) == 2
        assert len(pipeline_def.children) == 0

    def test_build_pipeline_report_from_table(self):
        print("test_walk_the_hierarchy_for_report_from_table")
        path= os.getenv("PIPELINES")
        dml_table_path=path + "/facts/p1/fct_order/sql-scripts/dml.p1_fct_order.sql"
        ddl_table_path=path + "/facts/p1/fct_order/sql-scripts/ddl.p1_fct_order.sql"
        pm.build_pipeline_definition_from_ddl_dml_content(dml_table_path, ddl_table_path, path)
        result = pm.get_static_pipeline_report_from_table("p1_fct_order", os.getenv("PIPELINES"))
        assert result
        print(result.model_dump_json(indent=3))

    def test_walk_the_hierarchy_for_report_from_intermediate_table(self):
        print("test_walk_the_hierarchy_for_report_from_table")
        path= os.getenv("PIPELINES")
        dml_table_path=path + "/facts/p1/fct_order/sql-scripts/dml.p1_fct_order.sql"
        ddl_table_path=path + "/facts/p1/fct_order/sql-scripts/ddl.p1_fct_order.sql"
        pm.build_pipeline_definition_from_ddl_dml_content(dml_table_path, ddl_table_path, path)
        result = pm.get_static_pipeline_report_from_table("int_p1_table_1", os.getenv("PIPELINES"))
        assert result
        print(result.model_dump_json(indent=3))


if __name__ == '__main__':
    unittest.main()
