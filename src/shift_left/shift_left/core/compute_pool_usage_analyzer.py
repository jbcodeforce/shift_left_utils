"""
Copyright 2024-2025 Confluent, Inc.

Compute Pool Usage Analyzer - Analyzes current compute pool usage and assesses 
statement consolidation opportunities for resource optimization.

This module provides functionality to:
- Analyze current compute pool usage across all pools
- Identify running statements in each compute pool
- Assess consolidation opportunities using simple heuristics
- Generate optimization recommendations
"""
from typing import List, Dict, Optional, Tuple, Set, Final
from datetime import datetime
from pydantic import BaseModel, Field
import time

from shift_left.core import compute_pool_mgr
from shift_left.core import statement_mgr
from shift_left.core.models.flink_compute_pool_model import ComputePoolInfo, ComputePoolList
from shift_left.core.models.flink_statement_model import StatementInfo, FlinkStatementExecutionPlan
from shift_left.core.utils.app_config import get_config, logger
from shift_left.core.utils.file_search import get_or_build_inventory, FlinkTableReference

NB_STATEMENTS_PER_POOL: int = 8

class PoolUsageStats(BaseModel):
    """Statistics for compute pool usage analysis."""
    pool_id: str
    pool_name: str
    current_cfu: int
    max_cfu: int  
    usage_percentage: float
    statement_count: int
    statements: List[StatementInfo] = Field(default_factory=list)
    efficiency_score: float = Field(default=0.0, description="Efficiency score based on usage vs statements")


class StatementGroup(BaseModel):
    """Group of statements that could potentially be consolidated."""
    group_id: str
    group_type: str  # "product", "resource", "dependency"
    statements: List[StatementInfo] = Field(default_factory=list)
    estimated_cfu_requirement: int = Field(default=0)
    current_pools: Set[str] = Field(default_factory=set)
    product_names: Set[str] = Field(default_factory=set)
    consolidation_feasible: bool = True
    consolidation_reason: str = ""


class ConsolidationRecommendation(BaseModel):
    """Recommendation for pool consolidation."""
    recommendation_id: str
    source_pools: List[str]
    target_pool: Optional[str] = None
    create_new_pool: bool = False
    statements_to_move: List[StatementInfo] = Field(default_factory=list)
    estimated_cfu_savings: int = 0
    estimated_cost_savings: float = 0.0
    migration_complexity: str = "LOW"  # LOW, MEDIUM, HIGH
    reason: str = ""


class PoolAnalysisReport(BaseModel):
    """Complete analysis report for compute pool usage."""
    created_at: datetime = Field(default_factory=datetime.now)
    environment_id: str
    total_pools: int
    total_statements: int
    total_cfu_used: int
    total_cfu_capacity: int
    overall_efficiency: float
    pool_stats: List[PoolUsageStats] = Field(default_factory=list)
    statement_groups: List[StatementGroup] = Field(default_factory=list)
    recommendations: List[ConsolidationRecommendation] = Field(default_factory=list)
    analysis_scope: Optional[Dict[str, str]] = Field(default=None, description="Scope of analysis (product/directory filters)")


class ComputePoolUsageAnalyzer:
    """
    Main analyzer class for compute pool usage optimization.
    
    Provides methods to:
    - Analyze current pool usage
    - Discover and group running statements  
    - Generate consolidation recommendations
    """
    
    def __init__(self):
        self.config = get_config()
        self.pool_list: Optional[ComputePoolList] = None
        self.statement_list: Dict[str, StatementInfo] = {}
        
    def analyze_pool_usage(self, inventory_path: str = None, 
                    product_name: str = None, 
                    directory: str = None) -> PoolAnalysisReport:
        """
        Perform comprehensive analysis of compute pool usage.
        
        Args:
            inventory_path: Path to pipeline inventory for context
            product_name: Optional product name to filter analysis to specific product
            directory: Optional directory path to filter analysis to specific pipeline directory
            
        Returns:
            Complete analysis report with recommendations
        """
        logger.info(f"Starting compute pool usage analysis (product: {product_name}, directory: {directory})...")
        start_time = time.perf_counter()
        
        # Get current pool and statement data
        self._load_pool_and_statement_data()
        
        # Filter statements based on product and/or directory if specified
        filtered_statements = self._filter_statements_by_scope(inventory_path, product_name, directory)
        
        # Analyze individual pool usage (only pools with filtered statements)
        pool_stats = self._analyze_individual_pools(filtered_statements)
        
        # Group statements by various heuristics
        statement_groups = self._group_statements_for_consolidation(inventory_path, filtered_statements)
        
        # Generate consolidation recommendations
        recommendations = self._generate_consolidation_recommendations(pool_stats, statement_groups)
        
        # Build final report
        report = self._build_analysis_report(pool_stats, statement_groups, recommendations, product_name, directory)
        
        execution_time = time.perf_counter() - start_time
        logger.info(f"Pool usage analysis completed in {execution_time:.2f} seconds")
        
        return report
    
    def _load_pool_and_statement_data(self):
        """Load current compute pool and statement information."""
        logger.info("Loading compute pool and statement data...")
        
        # Get compute pool list
        self.pool_list = compute_pool_mgr.get_compute_pool_list()
        logger.info(f"Found {len(self.pool_list.pools)} compute pools")
        
        # Get running statements
        self.statement_list = statement_mgr.get_statement_list()
        running_statements = {k: v for k, v in self.statement_list.items() 
                            if v.status_phase in ["RUNNING", "PENDING"]}
        logger.info(f"Found {len(running_statements)} running statements")
    
    def _filter_statements_by_scope(self, inventory_path: str = None, product_name: str = None, directory: str = None) -> List[StatementInfo]:
        """Filter statements based on product and/or directory scope."""
        running_statements = [stmt for stmt in self.statement_list.values() if stmt.status_phase in ["RUNNING", "PENDING"]]
        
        if not product_name and not directory:
            return running_statements
            
        filtered_statements = []
        
        try:
            if inventory_path:
                inventory = get_or_build_inventory(inventory_path, inventory_path, False)
            else:
                inventory = None
        except Exception as e:
            logger.warning(f"Could not load inventory for filtering: {e}")
            inventory = None
            
        for stmt in running_statements:
            include_statement = True
            
            # Filter by product if specified
            if product_name and inventory and not self._is_statement_for_product(product_name, stmt, inventory):
                include_statement = False
                    
            # Filter by directory if specified
            if directory and inventory and include_statement:
                statement_directory = self._get_statement_directory(stmt, inventory)
                if statement_directory and directory not in statement_directory:
                    include_statement = False
                    
            if include_statement:
                filtered_statements.append(stmt)
                
        logger.info(f"Filtered to {len(filtered_statements)} statements based on scope (product: {product_name}, directory: {directory})")
        return filtered_statements
    
    def _is_statement_for_product(self, product_name: str, stmt: StatementInfo, inventory: dict) -> bool:
        """Assess if statement is part of the product"""
        pname = product_name.replace('_', '-')
        if pname in stmt.name:
            return True
        return False
    
    def _get_statement_directory(self, stmt: StatementInfo, inventory: dict) -> Optional[str]:
        """Extract directory path for a statement using inventory lookup."""
        potential_table_name = stmt.name.replace('-dml', '').replace('-ddl', '').replace('staging-', '').replace('local-', '')
        
        for table_name, table_ref_dict in inventory.items():
            if potential_table_name in table_name or table_name in potential_table_name:
                table_ref = FlinkTableReference(**table_ref_dict)
                return table_ref.table_folder_name
        return None
        
    def _analyze_individual_pools(self, filtered_statements: List[StatementInfo] = None) -> List[PoolUsageStats]:
        """Analyze usage statistics for each compute pool."""
        logger.info("Analyzing individual pool usage...")
        pool_stats = []
        
        # Use filtered statements if provided, otherwise use all running statements
        if filtered_statements is None:
            filtered_statements = [stmt for stmt in self.statement_list.values() 
                                 if stmt.status_phase in ["RUNNING", "PENDING"]]
        
        # Only analyze pools that have statements in our filtered set
        pools_with_filtered_statements = set(stmt.compute_pool_id for stmt in filtered_statements)
        
        for pool in self.pool_list.pools:
            if pool.id not in pools_with_filtered_statements:
                continue  # Skip pools with no statements in our filtered scope
                
            # Find filtered statements running in this pool
            pool_statements = [stmt for stmt in filtered_statements 
                             if stmt.compute_pool_id == pool.id]
            
            usage_pct = compute_pool_mgr.get_pool_usage_from_pool_info(pool)
            
            # Calculate efficiency score (statements per CFU)
            efficiency_score = len(pool_statements) / max(pool.current_cfu, 1) if pool.current_cfu > 0 else 0
            
            stats = PoolUsageStats(
                pool_id=pool.id,
                pool_name=pool.name,
                current_cfu=pool.current_cfu,
                max_cfu=pool.max_cfu,
                usage_percentage=usage_pct * 100,
                statement_count=len(pool_statements),
                statements=pool_statements,
                efficiency_score=efficiency_score
            )
            pool_stats.append(stats)
            
        return pool_stats
    
    def _group_statements_for_consolidation(self, inventory_path: str = None, filtered_statements: List[StatementInfo] = None) -> List[StatementGroup]:
        """Group statements using various heuristics for potential consolidation."""
        logger.info("Grouping statements for consolidation analysis...")
        
        # Use filtered statements if provided, otherwise use all running statements
        if filtered_statements is None:
            running_statements = [stmt for stmt in self.statement_list.values() 
                                if stmt.status_phase in ["RUNNING", "PENDING"]]
        else:
            running_statements = filtered_statements
        
        statement_groups = []
        
        # Group by product (using inventory if available)
        if inventory_path:
            product_groups = self._group_by_product(running_statements, inventory_path)
            statement_groups.extend(product_groups)
        
        # Group by resource usage patterns
        resource_groups = self._group_by_resource_usage(running_statements)
        statement_groups.extend(resource_groups)
        
        # Group by pool efficiency
        efficiency_groups = self._group_by_pool_efficiency(running_statements)
        statement_groups.extend(efficiency_groups)
        
        return statement_groups
    
    def _group_by_product(self, statements: List[StatementInfo], inventory_path: str) -> List[StatementGroup]:
        """Group statements by data product."""
        logger.info("Grouping statements by product...")
        
        try:
            inventory = get_or_build_inventory(inventory_path, inventory_path, False)
            product_map = {}
            
            # Map statement names to products via table names
            for stmt in statements:
                # Extract table name from dml statement name: everything after '-dml-'
                if '-dml-' in stmt.name:
                    potential_table_name = stmt.name.split('-dml-', 1)[1]
                else:
                    potential_table_name = stmt.name
                potential_table_name = potential_table_name.replace('-', '_')
                for table_name, table_ref_dict in inventory.items():
                    if potential_table_name in table_name or table_name in potential_table_name:
                        table_ref = FlinkTableReference(**table_ref_dict)
                        product = table_ref.product_name
                        if product not in product_map:
                            product_map[product] = []
                        product_map[product].append(stmt)
                        break
            
            # Create statement groups for each product with multiple statements
            groups = []
            for product, product_statements in product_map.items():
                if len(product_statements) > 1:
                    pools_used = set(stmt.compute_pool_id for stmt in product_statements)
                    
                    group = StatementGroup(
                        group_id=f"product_{product}",
                        group_type="product",
                        statements=product_statements,
                        current_pools=pools_used,
                        product_names={product},
                        consolidation_feasible=len(pools_used) > 1,
                        consolidation_reason=f"Product {product} statements scattered across {len(pools_used)} pools"
                    )
                    groups.append(group)
                    
            return groups
            
        except Exception as e:
            logger.warning(f"Could not group by product: {e}")
            return []
    
    def _group_by_resource_usage(self, statements: List[StatementInfo]) -> List[StatementGroup]:
        """Group statements by resource usage patterns."""
        logger.info("Grouping statements by resource usage...")
        
        # Simple heuristic: group statements in underutilized pools
        pool_utilization = {}
        for stmt in statements:
            pool_id = stmt.compute_pool_id
            if pool_id not in pool_utilization:
                pool_utilization[pool_id] = []
            pool_utilization[pool_id].append(stmt)
        
        groups = []
        underutilized_pools = []
        
        # Find pools with low statement density
        for pool in self.pool_list.pools:
            pool_statements = pool_utilization.get(pool.id, [])
            if pool.current_cfu > 0:
                density = len(pool_statements) / pool.current_cfu
                if density < 0.5 and len(pool_statements) > 0:  # Less than 0.5 statements per CFU
                    underutilized_pools.extend(pool_statements)
        
        if len(underutilized_pools) > 1:
            # Group all underutilized statements
            pools_used = set(stmt.compute_pool_id for stmt in underutilized_pools)
            group = StatementGroup(
                group_id="resource_underutilized",
                group_type="resource",
                statements=underutilized_pools,
                current_pools=pools_used,
                consolidation_feasible=True,
                consolidation_reason=f"Statements from {len(pools_used)} underutilized pools can be consolidated"
            )
            groups.append(group)
            
        return groups
    
    def _group_by_pool_efficiency(self, statements: List[StatementInfo]) -> List[StatementGroup]:
        """Group statements from pools with poor efficiency."""
        logger.info("Grouping statements by pool efficiency...")
        
        # Find pools with single statements
        single_statement_pools = []
        
        for pool in self.pool_list.pools:
            pool_statements = [stmt for stmt in statements if stmt.compute_pool_id == pool.id]
            if len(pool_statements) == 1 and pool.current_cfu > 0:
                single_statement_pools.extend(pool_statements)
        
        groups = []
        if len(single_statement_pools) > 1:
            pools_used = set(stmt.compute_pool_id for stmt in single_statement_pools)
            group = StatementGroup(
                group_id="efficiency_single_statement",
                group_type="efficiency",
                statements=single_statement_pools,
                current_pools=pools_used,
                consolidation_feasible=True,
                consolidation_reason=f"{len(pools_used)} pools each running single statements can be consolidated"
            )
            groups.append(group)
            
        return groups
    
    def _generate_consolidation_recommendations(self, 
                                              pool_stats: List[PoolUsageStats],
                                              statement_groups: List[StatementGroup]) -> List[ConsolidationRecommendation]:
        """Generate consolidation recommendations based on analysis."""
        logger.info("Generating consolidation recommendations...")
        
        recommendations = []
        
        for group in statement_groups:
            if not group.consolidation_feasible or len(group.statements) < NB_STATEMENTS_PER_POOL:
                continue
                
            # Find best target pool or recommend new pool creation
            target_pool = self._find_best_target_pool(group, pool_stats)
            
            # Calculate potential savings
            current_pools_cfu = sum(next((p.current_cfu for p in pool_stats if p.pool_id in group.current_pools), 0) 
                                  for pool_id in group.current_pools)
            
            if target_pool:
                estimated_savings = max(0, current_pools_cfu - target_pool.current_cfu - len(group.statements))
                
                recommendation = ConsolidationRecommendation(
                    recommendation_id=f"consolidate_{group.group_id}",
                    source_pools=list(group.current_pools),
                    target_pool=target_pool.pool_id,
                    create_new_pool=False,
                    statements_to_move=group.statements,
                    estimated_cfu_savings=estimated_savings,
                    migration_complexity="LOW" if len(group.statements) <= 3 else "MEDIUM",
                    reason=f"{group.consolidation_reason}. Consolidate to {target_pool.pool_name}"
                )
            else:
                # Recommend new pool creation
                estimated_cfu_needed = len(group.statements) + 2  # Simple heuristic
                estimated_savings = max(0, current_pools_cfu - estimated_cfu_needed)
                
                recommendation = ConsolidationRecommendation(
                    recommendation_id=f"new_pool_{group.group_id}",
                    source_pools=list(group.current_pools),
                    target_pool=None,
                    create_new_pool=True,
                    statements_to_move=group.statements,
                    estimated_cfu_savings=estimated_savings,
                    migration_complexity="MEDIUM",
                    reason=f"{group.consolidation_reason}. Create new optimized pool"
                )
            
            recommendations.append(recommendation)
            
        return recommendations
    
    def _find_best_target_pool(self, group: StatementGroup, pool_stats: List[PoolUsageStats]) -> Optional[PoolUsageStats]:
        """Find the best existing pool to consolidate statements into.
        This is a simple heuristic, need to be improved.
        """
        
        # Exclude pools already in the group
        candidate_pools = [p for p in pool_stats if p.pool_id not in group.current_pools]
        
        if not candidate_pools:
            return None
            
        # Simple heuristic: find pool with capacity and good efficiency
        best_pool = None
        best_score = 0
        
        for pool in candidate_pools:
            available_capacity = pool.max_cfu - pool.current_cfu
            statements_needed = min(len(group.statements), NB_STATEMENTS_PER_POOL)
            
            # Check if pool has enough capacity (rough estimate)
            if available_capacity >= statements_needed:
                # Score based on efficiency and available capacity
                score = pool.efficiency_score + (available_capacity / pool.max_cfu) * 0.5
                if score > best_score:
                    best_score = score
                    best_pool = pool
                    
        return best_pool
    
    def _build_analysis_report(self, 
                              pool_stats: List[PoolUsageStats],
                              statement_groups: List[StatementGroup], 
                              recommendations: List[ConsolidationRecommendation],
                              product_name: str = None,
                              directory: str = None) -> PoolAnalysisReport:
        """Build the final analysis report."""
        
        total_cfu_used = sum(p.current_cfu for p in pool_stats)
        total_cfu_capacity = sum(p.max_cfu for p in pool_stats)
        total_statements = sum(p.statement_count for p in pool_stats)
        overall_efficiency = total_statements / max(total_cfu_used, 1)
        
        report = PoolAnalysisReport(
            environment_id=self.config['confluent_cloud']['environment_id'],
            total_pools=len(pool_stats),
            total_statements=total_statements,
            total_cfu_used=total_cfu_used,
            total_cfu_capacity=total_cfu_capacity,
            overall_efficiency=overall_efficiency,
            pool_stats=pool_stats,
            statement_groups=statement_groups,
            recommendations=recommendations
        )
        
        # Add filtering context to report if applicable
        scope_info = {}
        if product_name:
            scope_info['product_name'] = product_name
        if directory:
            scope_info['directory'] = directory
        if scope_info:
            report.analysis_scope = scope_info
            
        return report

    def print_analysis_summary(self, report: PoolAnalysisReport) -> str:
        """Generate a human-readable summary of the analysis."""
        
        summary = []
        summary.append("="*60)
        summary.append("COMPUTE POOL USAGE ANALYSIS SUMMARY")
        summary.append("="*60)
        summary.append(f"Analysis Date: {report.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        summary.append(f"Environment: {report.environment_id}")
        
        # Add scope information if present
        if report.analysis_scope:
            scope_desc = []
            if 'product_name' in report.analysis_scope:
                scope_desc.append(f"Product: {report.analysis_scope['product_name']}")
            if 'directory' in report.analysis_scope:
                scope_desc.append(f"Directory: {report.analysis_scope['directory']}")
            if scope_desc:
                summary.append(f"Analysis Scope: {', '.join(scope_desc)}")
        
        summary.append("")
        
        # Overall statistics
        summary.append("OVERALL STATISTICS:")
        summary.append(f"  Total Pools: {report.total_pools}")
        summary.append(f"  Total Statements: {report.total_statements}")
        summary.append(f"  Total CFU Used: {report.total_cfu_used}")
        summary.append(f"  Total CFU Capacity: {report.total_cfu_capacity}")
        summary.append(f"  Overall Utilization: {(report.total_cfu_used/max(report.total_cfu_capacity,1)*100):.1f}%")
        summary.append(f"  Overall Efficiency: {report.overall_efficiency:.2f} statements/CFU")
        summary.append("")
        
        # Pool details
        summary.append("POOL USAGE DETAILS:")
        for pool in report.pool_stats:
            summary.append(f"  {pool.pool_name} ({pool.pool_id}):")
            summary.append(f"    Usage: {pool.current_cfu}/{pool.max_cfu} CFU ({pool.usage_percentage:.1f}%)")
            summary.append(f"    Statements: {pool.statement_count}")
            summary.append(f"    Efficiency: {pool.efficiency_score:.2f} statements/CFU")
            summary.append("")
        
        # Consolidation opportunities
        if report.recommendations:
            summary.append("CONSOLIDATION RECOMMENDATIONS:")
            for rec in report.recommendations:
                summary.append(f"  {rec.recommendation_id}:")
                summary.append(f"    Complexity: {rec.migration_complexity}")
                summary.append(f"    Estimated CFU Savings: {rec.estimated_cfu_savings}")
                summary.append(f"    Statements to Move: {len(rec.statements_to_move)}")
                summary.append(f"    Reason: {rec.reason}")
                summary.append("")
        else:
            summary.append("No consolidation opportunities identified.")
            
        summary.append("="*60)
        
        return "\n".join(summary)
