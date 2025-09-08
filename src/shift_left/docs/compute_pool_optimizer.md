# Compute Pool Usage Analyzer

## Overview

The Compute Pool Usage Analyzer is a new feature designed to analyze current compute pool usage across your Flink environment and assess opportunities for statement consolidation. This tool can help optimize resource allocation and reduce costs by identifying underutilized pools and providing consolidation recommendations.

## Features

### Analysis

- Analyzes CFU usage across all compute pools
- Identifies all running statements and their pool assignments
- *Calculates statements-per-CFU ratios and efficiency scores

### Consolidation Heuristics
- Groups statements from the same data product
- Identifies underutilized pools for consolidation
- Finds pools with single statements that could be combined

### Detailed Reporting

- Per-pool CFU usage and statement counts
- Specific suggestions for optimization
- Potential CFU savings from consolidation

## Usage

### Command Line Interface

```bash
# Analyze compute pool usage with pipeline inventory context
shift-left pipeline analyze-pool-usage /path/to/pipelines

# Analyze without inventory context (basic analysis)
shift-left pipeline analyze-pool-usage
```

### Programmatic Usage

```python
from shift_left.core.compute_pool_usage_analyzer import ComputePoolUsageAnalyzer

# Create analyzer instance
analyzer = ComputePoolUsageAnalyzer()

# Run analysis
report = analyzer.analyze_pool_usage(inventory_path="/path/to/pipelines")

# Print summary
summary = analyzer.print_analysis_summary(report)
print(summary)

# Access detailed data
for pool in report.pool_stats:
    print(f"Pool {pool.pool_name}: {pool.usage_percentage:.1f}% used, {pool.statement_count} statements")

for rec in report.recommendations:
    print(f"Recommendation: {rec.reason}")
    print(f"Potential savings: {rec.estimated_cfu_savings} CFUs")
```

## Analysis Output

### Console Summary
The analyzer provides a comprehensive console summary including:

```
============================================================
COMPUTE POOL USAGE ANALYSIS SUMMARY  
============================================================
Analysis Date: 2024-12-19 10:30:15
Environment: env-abc123

OVERALL STATISTICS:
  Total Pools: 5
  Total Statements: 12
  Total CFU Used: 25
  Total CFU Capacity: 50
  Overall Utilization: 50.0%
  Overall Efficiency: 0.48 statements/CFU

POOL USAGE DETAILS:
  saleops-prod-pool (pool-123):
    Usage: 10/20 CFU (50.0%)
    Statements: 5
    Efficiency: 0.50 statements/CFU

CONSOLIDATION RECOMMENDATIONS:
  consolidate_efficiency_single_statement:
    Complexity: LOW
    Estimated CFU Savings: 8
    Statements to Move: 3
    Reason: 3 pools each running single statements can be consolidated
============================================================
```

### Detailed JSON Report
A detailed JSON report is saved with complete analysis data:

```json
{
  "created_at": "2024-12-19T10:30:15",
  "environment_id": "env-abc123",
  "total_pools": 5,
  "total_statements": 12,
  "pool_stats": [...],
  "statement_groups": [...],
  "recommendations": [...]
}
```

## Consolidation Heuristics

### 1. Product-based Grouping
- Groups statements belonging to the same data product
- Identifies when product statements are scattered across multiple pools
- Recommends consolidation to improve resource utilization

### 2. Resource-based Grouping  
- Identifies pools with low statement density (< 0.5 statements per CFU)
- Groups underutilized statements for potential consolidation
- Focuses on improving overall resource efficiency

### 3. Efficiency-based Grouping
- Finds pools running only single statements
- Calculates consolidation potential for better CFU utilization
- Prioritizes pools with poor efficiency ratios

## Recommendations

The analyzer generates specific recommendations with:

- **Migration Complexity**: LOW/MEDIUM/HIGH based on number of statements
- **CFU Savings Estimates**: Potential resource reduction
- **Target Pool Suggestions**: Best pools for consolidation
- **New Pool Recommendations**: When creating new pools is more efficient

## Integration Points

### Existing Architecture Integration
- Uses existing `compute_pool_mgr` for pool data
- Leverages `statement_mgr` for statement information  
- Integrates with pipeline inventory system
- Follows established error handling patterns

### CLI Integration
- Added to `pipeline` command group
- Follows existing CLI patterns and styling
- Provides both summary and detailed output options

## Benefits

### Cost Optimization
- **Reduced CFU Usage**: Consolidate underutilized pools
- **Improved Efficiency**: Better statements-per-CFU ratios
- **Resource Planning**: Data-driven pool sizing decisions

### Operational Benefits
- **Simplified Management**: Fewer pools to monitor
- **Better Utilization**: More efficient resource usage
- **Visibility**: Clear insight into current usage patterns

## Future Enhancements

While the current implementation uses simple heuristics, it provides a foundation for more sophisticated optimization approaches:

1. **O/R Problem Integration**: Full operations research optimization
2. **Machine Learning**: Pattern recognition for usage optimization
3. **Cost Modeling**: Detailed cost analysis and projections
4. **Automated Migration**: Seamless statement movement capabilities
5. **Capacity Planning**: Predictive resource requirement modeling

## Technical Details

### Models
- `PoolUsageStats`: Per-pool usage statistics
- `StatementGroup`: Groups of statements for consolidation
- `ConsolidationRecommendation`: Specific optimization suggestions
- `PoolAnalysisReport`: Complete analysis results

### Dependencies
- Pydantic models for data validation
- Existing compute pool and statement management systems
- Pipeline inventory integration for product context

This feature provides immediate value for resource optimization while establishing the foundation for more advanced optimization capabilities in the future.
