
"""
Copyright 2024-2025 Confluent, Inc.
"""
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any, Set
from shift_left.core.models.flink_statement_model import Statement, StatementResult

class SLTestData(BaseModel):
    table_name: str
    file_name: str
    file_type: str = "sql"

class SLTestCase(BaseModel):
    name: str
    inputs: List[SLTestData]
    outputs: List[SLTestData]

class Foundation(BaseModel):
    """
    represent the table to test and the ddl for the input tables to be created during tests.
    Those tables will be deleted after the tests are run.
    """
    table_name: str
    ddl_for_test: str

class SLTestDefinition(BaseModel):
    foundations: List[Foundation]
    test_suite: List[SLTestCase]

class TestResult(BaseModel):
    test_case_name: str
    result: str
    validation_result: StatementResult = None
    foundation_statements: List[Statement] = []
    statements: List[Statement] = []

class TestSuiteResult(BaseModel):
    foundation_statements: List[Statement] = []
    test_results: Dict[str, TestResult] = {}