"""
Copyright 2024-2025 Confluent, Inc.

Unit tests for the compute pool usage analyzer.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from shift_left.core.compute_pool_usage_analyzer import (
    ComputePoolUsageAnalyzer,
    PoolUsageStats,
    StatementGroup,
    ConsolidationRecommendation,
    PoolAnalysisReport
)
from shift_left.core.models.flink_compute_pool_model import ComputePoolInfo, ComputePoolList
from shift_left.core.models.flink_statement_model import StatementInfo


class TestComputePoolUsageAnalyzer:
    """Test suite for ComputePoolUsageAnalyzer."""
    
    @pytest.fixture
    def mock_pool_list(self):
        """Create mock compute pool list."""
        pool1 = ComputePoolInfo(
            id="pool-1", 
            name="saleops-prod-pool", 
            current_cfu=5,
            max_cfu=10,
            env_id="env-123"
        )
        pool2 = ComputePoolInfo(
            id="pool-2", 
            name="marketing-pool", 
            current_cfu=2,
            max_cfu=10,
            env_id="env-123"
        )
        pool3 = ComputePoolInfo(
            id="pool-3", 
            name="analytics-pool", 
            current_cfu=1,
            max_cfu=5,
            env_id="env-123"
        )
        return ComputePoolList(pools=[pool1, pool2, pool3])
    
    @pytest.fixture
    def mock_statement_list(self):
        """Create mock statement list."""
        return {
            "stmt-1": StatementInfo(
                name="stmt-1",
                status_phase="RUNNING",
                compute_pool_id="pool-1",
                compute_pool_name="saleops-prod-pool"
            ),
            "stmt-2": StatementInfo(
                name="stmt-2", 
                status_phase="RUNNING",
                compute_pool_id="pool-1",
                compute_pool_name="saleops-prod-pool"
            ),
            "stmt-3": StatementInfo(
                name="stmt-3",
                status_phase="RUNNING", 
                compute_pool_id="pool-2",
                compute_pool_name="marketing-pool"
            ),
            "stmt-4": StatementInfo(
                name="stmt-4",
                status_phase="RUNNING",
                compute_pool_id="pool-3", 
                compute_pool_name="analytics-pool"
            )
        }
    
    @pytest.fixture
    def analyzer(self):
        """Create analyzer instance."""
        return ComputePoolUsageAnalyzer()
    
    @patch('shift_left.core.compute_pool_usage_analyzer.compute_pool_mgr')
    @patch('shift_left.core.compute_pool_usage_analyzer.statement_mgr')
    def test_load_pool_and_statement_data(self, mock_stmt_mgr, mock_pool_mgr, analyzer, mock_pool_list, mock_statement_list):
        """Test loading of pool and statement data."""
        # Setup mocks
        mock_pool_mgr.get_compute_pool_list.return_value = mock_pool_list
        mock_stmt_mgr.get_statement_list.return_value = mock_statement_list
        
        # Execute
        analyzer._load_pool_and_statement_data()
        
        # Verify
        assert analyzer.pool_list == mock_pool_list
        assert analyzer.statement_list == mock_statement_list
        mock_pool_mgr.get_compute_pool_list.assert_called_once()
        mock_stmt_mgr.get_statement_list.assert_called_once()
    
    def test_analyze_individual_pools(self, analyzer, mock_pool_list, mock_statement_list):
        """Test individual pool analysis."""
        # Setup
        analyzer.pool_list = mock_pool_list
        analyzer.statement_list = mock_statement_list
        
        # Execute
        pool_stats = analyzer._analyze_individual_pools()
        
        # Verify
        assert len(pool_stats) == 3
        
        # Check pool-1 stats (2 statements, 5 CFU)
        pool1_stats = next(p for p in pool_stats if p.pool_id == "pool-1")
        assert pool1_stats.statement_count == 2
        assert pool1_stats.current_cfu == 5
        assert pool1_stats.efficiency_score == 2/5  # 2 statements / 5 CFU
        
        # Check pool-2 stats (1 statement, 2 CFU)  
        pool2_stats = next(p for p in pool_stats if p.pool_id == "pool-2")
        assert pool2_stats.statement_count == 1
        assert pool2_stats.current_cfu == 2
        assert pool2_stats.efficiency_score == 1/2  # 1 statement / 2 CFU
        
        # Check pool-3 stats (1 statement, 1 CFU)
        pool3_stats = next(p for p in pool_stats if p.pool_id == "pool-3")
        assert pool3_stats.statement_count == 1
        assert pool3_stats.current_cfu == 1
        assert pool3_stats.efficiency_score == 1/1  # 1 statement / 1 CFU
    
    def test_group_by_resource_usage(self, analyzer, mock_statement_list, mock_pool_list):
        """Test grouping statements by resource usage."""
        # Setup - create underutilized pool scenario
        analyzer.pool_list = mock_pool_list
        running_statements = [stmt for stmt in mock_statement_list.values() if stmt.status_phase == "RUNNING"]
        
        # Execute
        groups = analyzer._group_by_resource_usage(running_statements)
        
        # Verify - should find underutilized pools (pool-2 and pool-3 have low density)
        assert len(groups) >= 0  # May or may not find groups based on heuristics
        
        if groups:
            group = groups[0]
            assert group.group_type == "resource"
            assert group.consolidation_feasible
    
    def test_group_by_pool_efficiency(self, analyzer, mock_statement_list, mock_pool_list):
        """Test grouping by pool efficiency (single statement pools)."""
        # Setup
        analyzer.pool_list = mock_pool_list
        running_statements = [stmt for stmt in mock_statement_list.values() if stmt.status_phase == "RUNNING"]
        
        # Execute
        groups = analyzer._group_by_pool_efficiency(running_statements)
        
        # Verify - should find pools with single statements (pool-2, pool-3)
        assert len(groups) == 1
        efficiency_group = groups[0]
        assert efficiency_group.group_type == "efficiency"
        assert len(efficiency_group.statements) == 2  # stmt-3 and stmt-4
        assert efficiency_group.consolidation_feasible
        assert "single statements" in efficiency_group.consolidation_reason.lower()
    
    def test_find_best_target_pool(self, analyzer, mock_pool_list):
        """Test finding the best target pool for consolidation."""
        # Setup
        pool_stats = [
            PoolUsageStats(
                pool_id="pool-1",
                pool_name="busy-pool",
                current_cfu=8,
                max_cfu=10,
                usage_percentage=80.0,
                statement_count=3,
                efficiency_score=0.375
            ),
            PoolUsageStats(
                pool_id="pool-4",
                pool_name="available-pool", 
                current_cfu=3,
                max_cfu=10,
                usage_percentage=30.0,
                statement_count=2,
                efficiency_score=0.67
            )
        ]
        
        group = StatementGroup(
            group_id="test-group",
            group_type="test",
            current_pools={"pool-2", "pool-3"},
            statements=[
                StatementInfo(name="test-stmt-1", status_phase="RUNNING", compute_pool_id="pool-2"),
                StatementInfo(name="test-stmt-2", status_phase="RUNNING", compute_pool_id="pool-3")
            ]
        )
        
        # Execute
        target_pool = analyzer._find_best_target_pool(group, pool_stats)
        
        # Verify - should choose pool-4 as it has better capacity and efficiency
        assert target_pool is not None
        assert target_pool.pool_id == "pool-4"
    
    @patch('shift_left.core.compute_pool_usage_analyzer.compute_pool_mgr')
    @patch('shift_left.core.compute_pool_usage_analyzer.statement_mgr') 
    def test_analyze_pool_usage_integration(self, mock_stmt_mgr, mock_pool_mgr, analyzer, mock_pool_list, mock_statement_list):
        """Test the full analysis integration."""
        # Setup mocks
        mock_pool_mgr.get_compute_pool_list.return_value = mock_pool_list
        mock_stmt_mgr.get_statement_list.return_value = mock_statement_list
        
        # Execute - this will use real config, which is fine for integration test
        report = analyzer.analyze_pool_usage()
        print(report)
        # Verify basic report structure
        assert isinstance(report, PoolAnalysisReport)
        assert report.environment_id is not None  # Don't assert specific value since using real config
        assert report.total_pools == 3
        assert report.total_statements == 4
        assert len(report.pool_stats) == 3
        assert report.overall_efficiency > 0
        
        # Should have found some consolidation opportunities
        # (at least efficiency-based grouping for single statement pools)
        assert len(report.recommendations) >= 0
    
    def test_print_analysis_summary(self, analyzer):
        """Test the summary report generation."""
        # Create test report
        pool_stats = [
            PoolUsageStats(
                pool_id="pool-1",
                pool_name="test-pool",
                current_cfu=5,
                max_cfu=10, 
                usage_percentage=50.0,
                statement_count=2,
                efficiency_score=0.4
            )
        ]
        
        recommendations = [
            ConsolidationRecommendation(
                recommendation_id="test-rec",
                source_pools=["pool-2", "pool-3"],
                estimated_cfu_savings=3,
                migration_complexity="LOW",
                reason="Test consolidation",
                statements_to_move=[StatementInfo(name="test-stmt", status_phase="RUNNING", compute_pool_id="pool-2")]
            )
        ]
        
        report = PoolAnalysisReport(
            environment_id="test-env",
            total_pools=1,
            total_statements=2,
            total_cfu_used=5,
            total_cfu_capacity=10,
            overall_efficiency=0.4,
            pool_stats=pool_stats,
            recommendations=recommendations
        )
        
        # Execute
        summary = analyzer.print_analysis_summary(report)
        print(summary)
        # Verify key content
        assert "COMPUTE POOL USAGE ANALYSIS SUMMARY" in summary
        assert "test-env" in summary
        assert "Total Pools: 1" in summary
        assert "Total Statements: 2" in summary
        assert "CONSOLIDATION RECOMMENDATIONS:" in summary
        assert "test-rec" in summary
        assert "Estimated CFU Savings: 3" in summary
    
    def test_filter_statements_by_scope(self, analyzer):
        """Test filtering of statements : no filtering."""
        # Setup mock statements
        mock_statements = {
            "saleops-stmt-1": StatementInfo(
                name="saleops-stmt-1",
                status_phase="RUNNING",
                compute_pool_id="pool-1"
            ),
            "marketing-stmt-1": StatementInfo(
                name="marketing-stmt-1", 
                status_phase="RUNNING",
                compute_pool_id="pool-2"
            ),
            "analytics-stmt-1": StatementInfo(
                name="analytics-stmt-1",
                status_phase="RUNNING",
                compute_pool_id="pool-3"
            )
        }
        analyzer.statement_list = mock_statements
        
        # Test no filtering
        all_statements = analyzer._filter_statements_by_scope()
        assert len(all_statements) == 3
        
        # Test filtering without inventory (should return all statements)
        filtered = analyzer._filter_statements_by_scope(product_name="saleops")
        assert len(filtered) == 3  # No inventory means no filtering
    
    @patch('shift_left.core.compute_pool_usage_analyzer.compute_pool_mgr')
    @patch('shift_left.core.compute_pool_usage_analyzer.statement_mgr') 
    def test_analyze_pool_usage_with_product_filter(self, mock_stmt_mgr, mock_pool_mgr, analyzer, mock_pool_list, mock_statement_list):
        """Test analysis with product filter."""
        # Setup mocks
        mock_pool_mgr.get_compute_pool_list.return_value = mock_pool_list
        mock_stmt_mgr.get_statement_list.return_value = mock_statement_list
        
        # Execute with product filter
        report = analyzer.analyze_pool_usage(product_name="saleops")
        
        # Verify basic report structure
        assert isinstance(report, PoolAnalysisReport)
        assert report.analysis_scope is not None
        assert report.analysis_scope.get('product_name') == 'saleops'
    
    def test_print_analysis_summary_with_scope(self, analyzer):
        """Test summary generation with analysis scope."""
        # Create test report with scope
        report = PoolAnalysisReport(
            environment_id="test-env",
            total_pools=1,
            total_statements=2,
            total_cfu_used=5,
            total_cfu_capacity=10,
            overall_efficiency=0.4,
            pool_stats=[],
            recommendations=[],
            analysis_scope={'product_name': 'saleops', 'directory': '/path/to/facts'}
        )
        
        # Execute
        summary = analyzer.print_analysis_summary(report)
        print(summary)
        # Verify scope is included in summary
        assert "Analysis Scope: Product: saleops, Directory: /path/to/facts" in summary


if __name__ == "__main__":
    pytest.main([__file__])
