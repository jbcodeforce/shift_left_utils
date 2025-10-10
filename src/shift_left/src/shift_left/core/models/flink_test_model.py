
"""
Copyright 2024-2025 Confluent, Inc.
"""
from datetime import datetime, timezone
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
    foundation_statements: Set[Statement] = set()
    statements: Set[Statement] = set()
    status: str = "pending"  # pending, completed, error

class TestSuiteResult(BaseModel):
    foundation_statements: List[Statement] = []
    test_results: Dict[str, TestResult] = {}
    cleanup_errors: Optional[str] = None


class IntegrationTestData(BaseModel):
    """Data specification for integration tests"""
    table_name: str
    file_name: str
    unique_id: Optional[str] = None  # For tracking data through pipeline


class IntegrationTestScenario(BaseModel):
    """Integration test scenario definition"""
    name: str
    source_data: List[IntegrationTestData]  # Raw data to inject
    validation_queries: List[IntegrationTestData]  # Validation queries for each step
    intermediate_validations: List[IntegrationTestData] = []  # Optional intermediate checks

class IntegrationTestSuite(BaseModel):
    """Complete integration test suite definition"""
    product_name: str
    sink_table: str
    description: Optional[str] = None
    sink_test_path: str
    measure_latency: bool = True
    foundations: List[Foundation]
    scenarios: List[IntegrationTestScenario]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class IntegrationTestLatencyResult(BaseModel):
    """Latency measurement result"""
    unique_id: str
    start_time: datetime
    end_time: datetime
    latency_ms: float
    source_table: str
    sink_table: str


class IntegrationTestResult(BaseModel):
    """Result of a single integration test scenario"""
    scenario_name: str
    status: str  # PASS, FAIL, ERROR
    start_time: datetime
    end_time: datetime
    duration_ms: float
    latency_results: List[IntegrationTestLatencyResult] = []
    validation_results: List[StatementResult] = []
    error_message: Optional[str] = None


class IntegrationTestSuiteResult(BaseModel):
    """Result of complete integration test suite execution"""
    suite_name: str
    product_name: str
    sink_table: str
    test_results: List[IntegrationTestResult]
    overall_status: str  # PASS, FAIL, ERROR
    total_duration_ms: float
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

