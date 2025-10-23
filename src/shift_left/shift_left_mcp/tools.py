"""
Tool definitions for shift_left MCP server.
Defines all available shift_left CLI commands as MCP tools.
"""

TOOLS = [
    {
        "name": "shift_left_project_init",
        "description": "Initialize a new Flink project structure with specified name, path, and project type. Creates directory structure for Kimball or Data Product architectures.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_name": {
                    "type": "string",
                    "description": "Name of project to create"
                },
                "project_path": {
                    "type": "string", 
                    "description": "Target path where project will be created"
                },
                "project_type": {
                    "type": "string",
                    "description": "Project structure type (kimball or data_product)",
                    "enum": ["kimball", "data_product"]
                }
            },
            "required": ["project_name", "project_path"]
        }
    },
    {
        "name": "shift_left_project_validate_config",
        "description": "Validate the shift_left configuration file for required sections and proper formatting.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "shift_left_project_list_topics",
        "description": "List all Kafka topics in the configured cluster.",
        "inputSchema": {
            "type": "object", 
            "properties": {
                "project_path": {
                    "type": "string",
                    "description": "Path to the project directory"
                }
            },
            "required": ["project_path"]
        }
    },
    {
        "name": "shift_left_project_list_compute_pools",
        "description": "List available Flink compute pools in Confluent Cloud.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "environment_id": {
                    "type": "string",
                    "description": "Confluent Cloud environment ID (optional)"
                }
            },
            "required": []
        }
    },
    {
        "name": "shift_left_project_list_modified_files",
        "description": "Track modified Flink SQL files between git branches for blue-green deployment. Lists files changed between a branch and HEAD.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "branch_name": {
                    "type": "string",
                    "description": "Base branch name to compare against (e.g., 'main', 'develop')"
                },
                "output_file": {
                    "type": "string", 
                    "description": "Output file to save the list of modified files (optional)"
                },
                "file_filter": {
                    "type": "string",
                    "description": "File extension filter (e.g., '.sql')"
                },
                "project_path": {
                    "type": "string",
                    "description": "Path to the project directory (optional)"
                }
            },
            "required": ["branch_name"]
        }
    },
    {
        "name": "shift_left_table_init",
        "description": "Build a new table structure under the specified path. Creates folder structure for Flink table definitions.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to create"
                },
                "table_path": {
                    "type": "string",
                    "description": "Folder path where table structure will be created"
                },
                "product_name": {
                    "type": "string",
                    "description": "Product name for the table (optional)"
                }
            },
            "required": ["table_name", "table_path"]
        }
    },
    {
        "name": "shift_left_table_build_inventory",
        "description": "Build an inventory of all tables in the project with basic metadata.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "pipeline_path": {
                    "type": "string",
                    "description": "Pipeline folder path containing table definitions"
                }
            },
            "required": ["pipeline_path"]
        }
    },
    {
        "name": "shift_left_table_migrate",
        "description": "Migrate SQL code from various dialects (KSQL, Spark SQL, DBT) to Flink SQL using AI agents.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string", 
                    "description": "Name of the table after migration"
                },
                "sql_src_file_name": {
                    "type": "string",
                    "description": "Source SQL file path to migrate"
                },
                "target_path": {
                    "type": "string",
                    "description": "Target path for migrated content"
                },
                "source_type": {
                    "type": "string",
                    "description": "Type of SQL source to migrate (ksql, dbt, or spark)",
                    "enum": ["ksql", "dbt", "spark"]
                },
                "validate": {
                    "type": "boolean",
                    "description": "Validate migrated SQL using Confluent Cloud"
                },
                "recursive": {
                    "type": "boolean", 
                    "description": "Process recursively up to sources"
                }
            },
            "required": ["table_name", "sql_src_file_name", "target_path"]
        }
    },
    {
        "name": "shift_left_pipeline_deploy",
        "description": "Deploy Flink SQL pipelines from table names, product names, or directories. Supports blue-green deployment with dependency management.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "inventory_path": {
                    "type": "string",
                    "description": "Path to inventory folder containing pipeline definitions"
                },
                "table_name": {
                    "type": "string",
                    "description": "Specific table name to deploy (optional)"
                },
                "product_name": {
                    "type": "string", 
                    "description": "Product name to deploy all tables from (optional)"
                },
                "table_list_file_name": {
                    "type": "string",
                    "description": "File containing list of tables to deploy for blue-green (optional)"
                },
                "compute_pool_id": {
                    "type": "string",
                    "description": "Flink compute pool ID (optional)"
                },
                "dml_only": {
                    "type": "boolean",
                    "description": "Deploy only DML (not DDL)"
                },
                "parallel": {
                    "type": "boolean",
                    "description": "Deploy in parallel"
                }
            },
            "required": ["inventory_path"]
        }
    },
    {
        "name": "shift_left_pipeline_build_metadata",
        "description": "Build pipeline metadata from DML files for Flink table definitions.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dml_file_name": {
                    "type": "string",
                    "description": "Path to the DML file"
                },
                "pipeline_path": {
                    "type": "string", 
                    "description": "Pipeline base path"
                }
            },
            "required": ["dml_file_name", "pipeline_path"]
        }
    },
    {
        "name": "shift_left_table_init_unit_tests",
        "description": "Create test file to unit test the specified table.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to create unit tests for"
                }
            },
            "required": ["table_name"]
        }
    },
    {
        "name": "shift_left_table_run_unit_tests",
        "description": "Run the test suite of the given table.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to run unit tests for"
                }
            },
            "required": ["table_name"]
        }
    },
       {
        "name": "shift_left_table_validate_unit_tests",
        "description": "Run the Validation querie of the given table and a test case name.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to run unit tests for"
                },
                "test_case_name": {
                    "type": "string",
                    "description": "Name of the test case to validate unit tests for"
                }
            },
            "required": ["table_name"]
        }
    },
    {
        "name": "shift_left_table_delete_unit_tests",
        "description": "Remove/undeploy the unit test artifacts on Confluent Cloud.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to delete unit tests for"
                }
            },
            "required": ["table_name"]
        }
    },
    {
        "name": "shift_left_version",
        "description": "Display the current version of shift-left CLI.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
]

