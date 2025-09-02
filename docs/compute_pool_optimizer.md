# Compute Pool Usage Analyzer

## Overview

The Compute Pool Usage Analyzer is a new feature designed to analyze current compute pool usage across your Flink environment and assess opportunities for statement consolidation. This tool can help optimize resource allocation and reduce costs by identifying underutilized pools and providing consolidation recommendations.

## Features

### 🔍 Comprehensive Analysis
- **Pool Usage Assessment**: Analyzes CFU usage across all compute pools
- **Statement Discovery**: Identifies all running statements and their pool assignments
- **Efficiency Metrics**: Calculates statements-per-CFU ratios and efficiency scores
- **Scoped Analysis**: Filter analysis by product or directory for targeted optimization

### 🎯 Smart Consolidation Heuristics
- **Product-based Grouping**: Groups statements from the same data product
- **Resource-based Grouping**: Identifies underutilized pools for consolidation
- **Efficiency-based Grouping**: Finds pools with single statements that could be combined

### 📊 Detailed Reporting
- **Usage Statistics**: Per-pool CFU usage and statement counts
- **Consolidation Recommendations**: Specific suggestions for optimization
- **Cost Savings Estimates**: Potential CFU savings from consolidation

## Usage

### Command Line Interface

The analyzer supports flexible filtering options to focus analysis on specific areas:

```bash
# Analyze all pools (global view)
shift-left pipeline analyze-pool-usage /path/to/pipelines

# Analyze for specific product (recommended for focused optimization)
shift-left pipeline analyze-pool-usage --product-name saleops

# Analyze for specific directory 
shift-left pipeline analyze-pool-usage --directory /path/to/facts/saleops

# Combine product and directory filters for precision targeting
shift-left pipeline analyze-pool-usage --product-name saleops --directory /path/to/facts

# Short form options
shift-left pipeline analyze-pool-usage -p saleops -d /path/to/facts
```

### Programmatic Usage

```python
from shift_left.core.compute_pool_usage_analyzer import ComputePoolUsageAnalyzer

# Create analyzer instance
analyzer = ComputePoolUsageAnalyzer()

# Global analysis
report = analyzer.analyze_pool_usage(inventory_path="/path/to/pipelines")

# Product-specific analysis
report = analyzer.analyze_pool_usage(
    inventory_path="/path/to/pipelines",
    product_name="saleops"
)

# Directory-specific analysis  
report = analyzer.analyze_pool_usage(
    inventory_path="/path/to/pipelines",
    directory="/path/to/facts/saleops"
)

# Combined filtering
report = analyzer.analyze_pool_usage(
    inventory_path="/path/to/pipelines",
    product_name="saleops", 
    directory="/path/to/facts"
)

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
Analysis Scope: Product: saleops, Directory: /path/to/facts

OVERALL STATISTICS:
  Total Pools: 3 (filtered from 8 total pools)
  Total Statements: 7 (filtered from 25 total statements)
  Total CFU Used: 15
  Total CFU Capacity: 30
  Overall Utilization: 50.0%
  Overall Efficiency: 0.47 statements/CFU

POOL USAGE DETAILS:
  saleops-facts-pool (pool-123):
    Usage: 8/15 CFU (53.3%)
    Statements: 4
    Efficiency: 0.50 statements/CFU

  saleops-staging-pool (pool-456):
    Usage: 7/15 CFU (46.7%)
    Statements: 3
    Efficiency: 0.43 statements/CFU

CONSOLIDATION RECOMMENDATIONS:
  consolidate_product_saleops:
    Complexity: LOW
    Estimated CFU Savings: 5
    Statements to Move: 3
    Reason: Product saleops statements scattered across 2 pools can be consolidated
============================================================
```

### Detailed JSON Report
A detailed JSON report is saved with complete analysis data including scope information:

```json
{
  "created_at": "2024-12-19T10:30:15",
  "environment_id": "env-abc123", 
  "analysis_scope": {
    "product_name": "saleops",
    "directory": "/path/to/facts"
  },
  "total_pools": 3,
  "total_statements": 7,
  "pool_stats": [...],
  "statement_groups": [...],
  "recommendations": [...]
}
```

## Analysis Scoping Strategy

### 🎯 Recommended Analysis Approach

**1. Product-Level Analysis (Most Common)**
```bash
shift-left pipeline analyze-pool-usage --product-name saleops
```
- **Best for**: Day-to-day optimization decisions
- **Benefits**: Clear ownership boundaries, manageable scope
- **Use case**: "How can we optimize the saleops product pools?"

**2. Directory-Level Analysis**
```bash
shift-left pipeline analyze-pool-usage --directory /path/to/facts
```
- **Best for**: Layer-specific optimization (facts, dimensions, sources)
- **Benefits**: Focuses on similar workload patterns
- **Use case**: "Are our fact tables efficiently distributed across pools?"

**3. Combined Product + Directory**
```bash
shift-left pipeline analyze-pool-usage --product-name saleops --directory /path/to/facts
```
- **Best for**: Precision targeting of specific areas
- **Benefits**: Maximum focus, actionable recommendations
- **Use case**: "Optimize just the saleops fact tables"

**4. Global Analysis**
```bash
shift-left pipeline analyze-pool-usage
```
- **Best for**: Strategic overview, capacity planning
- **Benefits**: Complete picture, cross-product insights
- **Use case**: "What's our overall pool efficiency across all products?"

## Consolidation Heuristics

### 1. Product-based Grouping
- Groups statements belonging to the same data product
- **Enhanced with scoping**: When analyzing by product, focuses on cross-pool consolidation within that product
- Recommends consolidation to improve resource utilization

### 2. Resource-based Grouping  
- Identifies pools with low statement density (< 0.5 statements per CFU)
- **Enhanced with scoping**: Only considers pools within the filtered scope
- Focuses on improving overall resource efficiency

### 3. Efficiency-based Grouping
- Finds pools running only single statements
- **Enhanced with scoping**: Prioritizes single-statement pools within the analysis scope
- Calculates consolidation potential for better CFU utilization

## Recommendations

The analyzer generates specific recommendations with:

- **Migration Complexity**: LOW/MEDIUM/HIGH based on number of statements
- **CFU Savings Estimates**: Potential resource reduction within scope
- **Target Pool Suggestions**: Best pools for consolidation
- **New Pool Recommendations**: When creating new pools is more efficient

## Integration Points

### Existing Architecture Integration
- Uses existing `compute_pool_mgr` for pool data
- Leverages `statement_mgr` for statement information  
- Integrates with pipeline inventory system for product/directory filtering
- Follows established error handling patterns

### CLI Integration
- Added to `pipeline` command group with intuitive options
- Follows existing CLI patterns and styling
- Provides both summary and detailed output options

## Benefits

### Cost Optimization
- **Focused Savings**: Target specific products or directories for immediate impact
- **Reduced CFU Usage**: Consolidate underutilized pools within scope
- **Improved Efficiency**: Better statements-per-CFU ratios where it matters most
- **Strategic Planning**: Different scopes for different optimization goals

### Operational Benefits
- **Incremental Approach**: Start with one product, expand to others
- **Team Alignment**: Product-based analysis aligns with team boundaries
- **Reduced Risk**: Smaller scope reduces migration complexity
- **Better Utilization**: More efficient resource usage in target areas

## Example Workflows

### Product Team Workflow
```bash
# Weekly optimization check for saleops team
shift-left pipeline analyze-pool-usage --product-name saleops

# Focus on facts if issues found
shift-left pipeline analyze-pool-usage --product-name saleops --directory /path/to/facts
```

### Platform Team Workflow
```bash
# Monthly strategic review
shift-left pipeline analyze-pool-usage

# Deep dive into underutilized areas
shift-left pipeline analyze-pool-usage --directory /path/to/sources
```

### Cost Optimization Workflow
```bash
# Identify biggest opportunities by product
for product in saleops marketing analytics; do
  shift-left pipeline analyze-pool-usage --product-name $product
done
```

## Future Enhancements

While the current implementation uses simple heuristics with powerful scoping, it provides a foundation for more sophisticated optimization approaches:

1. **O/R Problem Integration**: Full operations research optimization within scoped boundaries
2. **Machine Learning**: Pattern recognition for usage optimization per product/directory
3. **Cost Modeling**: Detailed cost analysis and projections with scope awareness  
4. **Automated Migration**: Seamless statement movement within filtered scope
5. **Capacity Planning**: Predictive resource requirement modeling per product
6. **Cross-Product Analysis**: Identify shared resources and consolidation opportunities across products

## Technical Details

### Models
- `PoolUsageStats`: Per-pool usage statistics (filtered by scope)
- `StatementGroup`: Groups of statements for consolidation (within scope)
- `ConsolidationRecommendation`: Specific optimization suggestions (scope-aware)
- `PoolAnalysisReport`: Complete analysis results with scope metadata

### Filtering Logic
- **Product Filtering**: Uses pipeline inventory to map statements to products
- **Directory Filtering**: Uses inventory table folder paths for directory matching
- **Combined Filtering**: Applies both filters for precision targeting
- **Fallback Behavior**: Without inventory, performs global analysis

### Dependencies
- Pydantic models for data validation
- Existing compute pool and statement management systems
- Pipeline inventory integration for product/directory context
- FlinkTableReference model for metadata extraction

This enhanced feature provides immediate value for targeted resource optimization while establishing the foundation for more advanced, scope-aware optimization capabilities in the future.

## Quick Start Examples

```bash
# Start with your main product
shift-left pipeline analyze-pool-usage --product-name your-product

# If recommendations found, drill down to specific layers
shift-left pipeline analyze-pool-usage --product-name your-product --directory facts

# For platform teams - strategic overview
shift-left pipeline analyze-pool-usage

# Focus on efficiency issues
shift-left pipeline analyze-pool-usage --directory sources  # Often has single-statement pools
```