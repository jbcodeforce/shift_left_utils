import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json

from shift_left.core.utils.app_config import get_config
from shift_left.core.models.flink_statement_model import Statement, StatementInfo, StatementListCache, Spec, Status
import  shift_left.core.pipeline_mgr as pipeline_mgr
from shift_left.core.models.flink_statement_model import Statement, StatementResult, Data, OpRow
import shift_left.core.deployment_mgr as deployment_mgr
class TestDebugUnitTests(unittest.TestCase):

    def _test_build_pipeline_definition_from_dml_content(self):
        pipe_path ="/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"
        os.environ["PIPELINES"]=pipe_path
        dml_path = pipe_path + "/facts/aqem/fct_event_step_element/sql-scripts/dml.aqem_fct_event_step_element.sql"
        pipeline_def = pipeline_mgr.build_pipeline_definition_from_dml_content(
            dml_path,
            pipeline_path=pipe_path
        )
        print(pipeline_def.model_dump_json())

    def test_deploy_pipeline_from_product(self):
        pipe_path ="/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"
        os.environ["PIPELINES"]=pipe_path
        product_name = "aqem"
        dml_only = False
        may_start_children = True
        force_sources = False   
        result, summary =  deployment_mgr.deploy_pipeline_from_product(
            product_name=product_name,
            inventory_path=pipe_path,
            dml_only=dml_only,
            may_start_children=may_start_children,
            force_sources=force_sources)
        print(result)
        print(summary)

if __name__ == '__main__':
    unittest.main()