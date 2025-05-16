# Shift Left Utils - Flink SQL Pipeline Management System

## Overview
Shift Left Utils is a comprehensive pipeline management system designed to handle the deployment and management of Flink SQL pipelines. The system provides tools for building, deploying, and managing complex data processing pipelines using Flink SQL statements.

## Core Components

### 1. Pipeline Management

- **Pipeline Definition**: Each pipeline is defined through a JSON configuration file that specifies:
  - Table relationships (parents and children)
  - DDL and DML statements
  - Table metadata and properties

### 2. Deployment Management
- **Execution Planning**: The system builds execution plans that determine the order of statement deployment
- **Dependency Management**: Handles complex dependencies between tables and statements
- **State Management**: Manages stateful and stateless processing requirements
- **Compute Pool Management**: Handles compute pool allocation and management

### 3. Table Management
- **Table Structure**: Supports different table types:
  - Source tables (`src_*`)
  - Intermediate tables (`int_*`)
  - Fact tables (`*_fct_*`)
  - Dimension tables (`*_dim_*`)
  - View tables (`*_mv_*`)
- **Table Operations**: Provides operations for:
  - Table creation
  - Table dropping
  - Table structure inspection
  - Table validation

## Key Features

### 1. Smart Deployment
- **Execution Planning**: Automatically determines the correct order of statement execution
- **Dependency Resolution**: Handles complex parent-child relationships between tables
- **State Management**: Properly manages stateful operations and their dependencies

### 2. Validation and Compliance
- **Naming Conventions**: Enforces consistent naming patterns for tables and files
- **Schema Validation**: Validates table structures and properties
- **Pipeline Validation**: Ensures pipeline definitions are complete and valid

### 3. Compute Pool Management
- **Pool Allocation**: Manages compute pool resources
- **Statement Deployment**: Handles statement deployment to appropriate compute pools
- **Resource Optimization**: Optimizes resource usage across the pipeline

## Architecture

### 1. Core Components
- **Deployment Manager**: Handles pipeline deployment and execution planning
- **Table Manager**: Manages table operations and metadata
- **Pipeline Manager**: Handles pipeline definitions and relationships
- **Statement Manager**: Manages Flink SQL statement lifecycle

### 2. Data Flow
1. Pipeline Definition → Execution Plan Generation
2. Execution Plan → Statement Deployment
3. Statement Deployment → Table Creation/Modification
4. Table Operations → Pipeline Validation

## Usage Patterns

### 1. Pipeline Deployment
```python
deploy_pipeline_from_table(
    table_name: str,
    inventory_path: str,
    compute_pool_id: str,
    dml_only: bool = False,
    force_children: bool = False
)
```

### 2. Pipeline Undeployment
```python
full_pipeline_undeploy_from_table(
    table_name: str,
    inventory_path: str
)
```

### 3. Table Management
```python
create_table(table_name: str, ddl_content: str, compute_pool_id: Optional[str] = None)
drop_table(table_name: str, compute_pool_id: Optional[str] = None)
get_table_structure(table_name: str, compute_pool_id: Optional[str] = None)
```

## Future Considerations

### 1. Enhanced Monitoring
- Add comprehensive monitoring of pipeline execution
- Implement alerting for pipeline failures
- Add performance metrics collection

### 2. Advanced Features
- Support for dynamic pipeline modification
- Enhanced error recovery mechanisms
- Improved state management for complex pipelines

### 3. Integration Capabilities
- Support for additional data sources
- Integration with other data processing frameworks
- Enhanced metadata management

## Best Practices

1. **Naming Conventions**
   - Follow the established naming patterns for tables and files
   - Use consistent prefixes for different table types
   - Maintain clear and descriptive names

2. **Pipeline Design**
   - Keep pipelines modular and maintainable
   - Document dependencies clearly
   - Consider state management requirements

3. **Resource Management**
   - Optimize compute pool usage
   - Monitor resource consumption
   - Plan for scalability

## Conclusion
Shift Left Utils provides a robust framework for managing Flink SQL pipelines, with a focus on reliability, maintainability, and scalability. The system's architecture allows for complex pipeline management while maintaining simplicity in deployment and operation. 