"""
Command builder for shift_left MCP server.
Converts tool calls into shift_left CLI commands.
"""


def build_command(tool_name: str, arguments: dict) -> list[str]:
    """
    Build the shift_left CLI command from tool name and arguments.
    
    Args:
        tool_name: Name of the MCP tool being called
        arguments: Dictionary of arguments for the tool
    
    Returns:
        List of command arguments suitable for subprocess.run()
    
    Raises:
        ValueError: If tool_name is not recognized
    """
    
    # Map tool names to CLI commands
    command_map = {
        "shift_left_project_init": ["shift_left", "project", "init"],
        "shift_left_project_validate_config": ["shift_left", "project", "validate-config"],
        "shift_left_project_list_topics": ["shift_left", "project", "list-topics"],
        "shift_left_project_list_compute_pools": ["shift_left", "project", "list-compute-pools"],
        "shift_left_project_list_modified_files": ["shift_left", "project", "list-modified-files"],
        "shift_left_table_init": ["shift_left", "table", "init"],
        "shift_left_table_build_inventory": ["shift_left", "table", "build-inventory"],
        "shift_left_table_migrate": ["shift_left", "table", "migrate"],
        "shift_left_table_init_unit_tests": ["shift_left", "table", "init-unit-tests"],
        "shift_left_table_run_unit_tests": ["shift_left", "table", "run-unit-tests"],
        "shift_left_table_validate_unit_tests": ["shift_left", "table", "validate-unit-tests"],
        "shift_left_table_delete_unit_tests": ["shift_left", "table", "delete-unit-tests"],
        "shift_left_pipeline_deploy": ["shift_left", "pipeline", "deploy"],
        "shift_left_pipeline_build_metadata": ["shift_left", "pipeline", "build-metadata"],
        "shift_left_version": ["shift_left", "version"]
    }
    
    cmd = command_map.get(tool_name, [])
    if not cmd:
        raise ValueError(f"Unknown tool: {tool_name}")
    
    # Add positional arguments based on command
    if tool_name == "shift_left_project_init":
        cmd.extend([arguments["project_name"], arguments["project_path"]])
        if "project_type" in arguments:
            cmd.extend(["--project-type", arguments["project_type"]])
    
    elif tool_name == "shift_left_project_list_topics":
        cmd.append(arguments["project_path"])
    
    elif tool_name == "shift_left_project_list_compute_pools":
        if "environment_id" in arguments:
            cmd.extend(["--environment-id", arguments["environment_id"]])
    
    elif tool_name == "shift_left_project_list_modified_files":
        cmd.append(arguments["branch_name"])
        if "output_file" in arguments:
            cmd.extend(["--output-file", arguments["output_file"]])
        if "file_filter" in arguments:
            cmd.extend(["--file-filter", arguments["file_filter"]])
        if "project_path" in arguments:
            cmd.extend(["--project-path", arguments["project_path"]])
    
    elif tool_name == "shift_left_table_init":
        cmd.extend([arguments["table_name"], arguments["table_path"]])
        if "product_name" in arguments:
            cmd.extend(["--product-name", arguments["product_name"]])
    
    elif tool_name == "shift_left_table_build_inventory":
        cmd.append(arguments["pipeline_path"])
    
    elif tool_name == "shift_left_table_migrate":
        cmd.extend([arguments["table_name"], arguments["sql_src_file_name"], arguments["target_path"]])
        if "source_type" in arguments:
            cmd.extend(["--source-type", arguments["source_type"]])
        if arguments.get("validate"):
            cmd.append("--validate")
        if arguments.get("recursive"):
            cmd.append("--recursive")
    
    elif tool_name in ["shift_left_table_init_unit_tests", "shift_left_table_run_unit_tests", "shift_left_validate_unit_tests","shift_left_table_delete_unit_tests"]:
        cmd.append(arguments["table_name","testc_case_name"])
    
    elif tool_name == "shift_left_pipeline_deploy":
        cmd.append(arguments["inventory_path"])
        if "table_name" in arguments:
            cmd.extend(["--table-name", arguments["table_name"]])
        if "product_name" in arguments:
            cmd.extend(["--product-name", arguments["product_name"]])
        if "table_list_file_name" in arguments:
            cmd.extend(["--table-list-file-name", arguments["table_list_file_name"]])
        if "compute_pool_id" in arguments:
            cmd.extend(["--compute-pool-id", arguments["compute_pool_id"]])
        if arguments.get("dml_only"):
            cmd.append("--dml-only")
        if arguments.get("parallel"):
            cmd.append("--parallel")
    
    elif tool_name == "shift_left_pipeline_build_metadata":
        cmd.extend([arguments["dml_file_name"], arguments["pipeline_path"]])
    
    return cmd

