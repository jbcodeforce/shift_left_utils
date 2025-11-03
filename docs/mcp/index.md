# Shift Left MCP Server Integration

The `src/shift_left/shift_left_mcp` directory contains the MCP (Model Context Protocol) server integration for the `shift_left` CLI tool, enabling direct integration with any MCP client like Cursor AI .

## What is MCP?

MCP (Model Context Protocol) is a protocol that allows AI assistants like Cursor to call external tools and services. This integration exposes all shift_left CLI commands as callable tools within Cursor.

## Quick Start

### Install Dependencies

Using `uv` (recommended):

```bash
cd shift_left_utils/src/shift_left
uv add mcp
```

Or using pip:

```bash
pip install mcp
```

### Test the MCP Server

This is optional but helps to verify the MCP server will work in your environment:

```bash
in shift_left_utils/src/shift_left
uv run python -m shift_left_mcp.test_server
```
### Set environment variables

Environment variables must be set **before** starting Cursor.

### Configure Cursor

1. Modify the Cursor mcp configuration: **Cursor > Settings > Cursor Settings,** then select `Tools & MCP`. Add the mcp configuration but change the cwd path below:
    ```json
      "mcpServers": {
            "shift-left-tools": {
                "command": "uv",
                "args": [
                    "run",
                    "python",
                    "-m",
                    "shift_left_mcp"
                ],
                "cwd": "/Users/jerome/Documents/Code/shift_left_utils/src/shift_left",
                "env": {
                    "PIPELINES": "${env:PIPELINES}",
                    "CONFIG_FILE": "${env:CONFIG_FILE}",
                    "STAGING": "${env:STAGING}",
                    "SL_CONFLUENT_CLOUD_API_KEY": "${env:SL_CONFLUENT_CLOUD_API_KEY}",
                    "SL_CONFLUENT_CLOUD_API_SECRET": "${env:SL_CONFLUENT_CLOUD_API_SECRET}",
                    "SL_FLINK_API_KEY": "${env:SL_FLINK_API_KEY}",
                    "SL_FLINK_API_SECRET": "${env:SL_FLINK_API_SECRET}",
                    "SL_KAFKA_API_KEY": "${env:SL_KAFKA_API_KEY}",
                    "SL_KAFKA_API_SECRET": "${env:SL_KAFKA_API_SECRET}"
                }
            }
        }
    ```

1. Be sure to define the environment variables before re-starting Cursor. Include the `SL_*` variables to connect to Confluent Cloud. Use a shell script to export those variables and source this script before starting Cursor, or VScode.
1. **Restart Cursor**: Press **Cmd+Q** (don't just close the window)
1. Start Cursor from this terminal
    ```sh
    open -a Cursor
    ```
1. In Cursor: "What is the version of shift_left?"

### Potential Problem

* When using prompts like "what is the version of shift left" or "list my kafka topics" in Cursor, the LLM tries to answer directly instead of calling the MCP tools.

Check **View > Output > MCP** for connection status.

This may come from:

1. MCP server not configured in Cursor settings
2. Environment variables not set before Cursor starts
3. Cursor not restarted after configuration changes, and started from a Terminal with environment variables set.


* Tools Not Being Called

The MCP server registered but LLM not recognizing tool calling opportunity. Verify output, try to restart Cursor

### Example Prompts

**Project Management:**
- "Initialize a new Kimball project called 'analytics' in ./workspace"
- "List all Kafka topics in my project at ./my_project"
- "Show me the available Flink compute pools"
- "What files changed between main and my branch?"

**Table Management:**
- "Create a new table called 'customer_events' in ./pipelines/facts"
- "Build the table inventory for ./pipelines"
- "Migrate this Spark SQL file to Flink SQL: ./src/customer.sql"
- "Create unit tests for the fact_orders table"

**Pipeline Deployment:**
- "Deploy the customer_orders table from ./pipelines inventory"
- "Deploy all tables in the sales product"
- "Build metadata for the DML file at ./pipelines/facts/fact_sales/dml.sql"

Cursor will:
1. Understand your intent
2. Automatically select the right shift_left tool
3. Fill in the parameters
4. Execute the command
5. Show you the results

## Architecture

The MCP server is organized as a Python package under `src/shift_left/shift_left_mcp/`:

```
shift_left_mcp/
├── __init__.py          # Package initialization
├── __main__.py          # Entry point (python -m shift_left_mcp)
├── server.py            # MCP server implementation
├── tools.py             # Tool definitions
├── command_builder.py   # CLI command builder
└── test_server.py       # Test suite
```

## Available Tools

The MCP server exposes the following shift_left commands:

### Project Management
- `shift_left_project_init` - Initialize new Flink project
- `shift_left_project_validate_config` - Validate configuration
- `shift_left_project_list_topics` - List Kafka topics
- `shift_left_project_list_compute_pools` - List Flink compute pools
- `shift_left_project_list_modified_files` - Track git changes

### Table Management
- `shift_left_table_init` - Create table structure
- `shift_left_table_build_inventory` - Build table inventory
- `shift_left_table_migrate` - Migrate SQL with AI
- `shift_left_table_init_unit_tests` - Initialize unit tests
- `shift_left_table_run_unit_tests` - Run unit tests
- `shift_left_table_delete_unit_tests` - Delete unit tests

### Pipeline Management
- `shift_left_pipeline_deploy` - Deploy Flink pipelines
- `shift_left_pipeline_build_metadata` - Build pipeline metadata

### Utility
- `shift_left_version` - Show CLI version

## Running the Server

The server can be run in several ways:

### As a Python Module (Recommended)

```bash
uv run python -m shift_left_mcp
```

### Directly

```bash
uv run python src/shift_left/shift_left_mcp/server.py
```

### Via Cursor (Automatic)

Once configured in Cursor, the server runs automatically when needed.

## Usage in Cursor

Once configured, use natural language in Cursor:

**Examples:**
- "Initialize a new Kimball project called 'analytics' in ./workspace"
- "List all Kafka topics in the current project"
- "Deploy the customer_orders table"
- "Migrate this Spark SQL file to Flink SQL"
- "Create unit tests for the fact_sales table"

Cursor automatically:
1. Detects when shift_left is needed
2. Calls the appropriate MCP tool
3. Displays results
4. Handles errors gracefully

## Configuration

### Environment Variables

The MCP server respects standard shift_left environment variables:
- `PIPELINES` - Main pipeline directory
- `SRC_FOLDER` - Source SQL files directory
- `STAGING` - Staging area for migrations
- `CONFIG_FILE` - Configuration file path

Set these in your shell before starting Cursor.

## Development

### Adding New Tools

1. Add tool definition to `tools.py`
2. Add command mapping to `command_builder.py`
3. Add test case to `test_server.py`
4. Run tests to verify
5. Update documentation

### Debugging

Enable verbose logging by modifying `server.py`:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```
View logs in Cursor's Developer Tools (View → Toggle Developer Tools).

## Troubleshooting

### shift_left Not Found

Ensure the working directory is set correctly in Cursor configuration:
```json
"cwd": "/Users/jerome/Documents/Code/shift_left_utils/src/shift_left"
```

### Command Failures

1. Test shift_left CLI directly: `shift_left version`
2. Check environment variables are set
3. Verify credentials for Confluent Cloud
4. Review error messages in Cursor output

## Version

MCP Server Version: 0.1.0
Compatible with: shift_left CLI 0.1.41+

# MCP Usage Examples in Cursor

Once you've configured the shift_left MCP server in Cursor, you can use natural language to execute shift_left commands. Here are practical examples:

## Project Management

### Initialize a New Project
**You say:**
> "Create a new Flink project called 'customer_analytics' in my workspace folder using the Kimball structure"

**Cursor will:**
- Call `shift_left_project_init`
- Pass: project_name="customer_analytics", project_path="./workspace", project_type="kimball"
- Show the created project structure

### List Kafka Topics
**You say:**
> "Show me all the Kafka topics in my current project"

**Cursor will:**
- Call `shift_left_project_list_topics`
- Pass: project_path="."
- Display all available topics

### Check Compute Pools
**You say:**
> "What Flink compute pools are available in environment env-abc123?"

**Cursor will:**
- Call `shift_left_project_list_compute_pools`
- Pass: environment_id="env-abc123"
- List all compute pools with details

### Track Changes for Blue-Green Deployment
**You say:**
> "What SQL files changed between main branch and my current branch?"

**Cursor will:**
- Call `shift_left_project_list_modified_files`
- Pass: branch_name="main", file_filter=".sql"
- Show modified files for targeted deployment

## Table Management

### Create a New Table
**You say:**
> "Create a new table called 'customer_orders' in the facts folder under the sales product"

**Cursor will:**
- Call `shift_left_table_init`
- Pass: table_name="customer_orders", table_path="./pipelines/facts", product_name="sales"
- Create the complete table structure

### Build Table Inventory
**You say:**
> "Build an inventory of all tables in my pipelines directory"

**Cursor will:**
- Call `shift_left_table_build_inventory`
- Pass: pipeline_path="./pipelines"
- Generate inventory with metadata for all tables

### Migrate SQL Code
**You say:**
> "Migrate this Spark SQL file to Flink SQL: /path/to/customer_transform.sql, name it customer_dim, and put it in ./staging. Also validate the result."

**Cursor will:**
- Call `shift_left_table_migrate`
- Pass: table_name="customer_dim", sql_src_file_name="/path/to/customer_transform.sql", target_path="./staging", source_type="spark", validate=true
- Perform AI-powered migration and validation

## Unit Testing

### Initialize Unit Tests
**You say:**
> "Create unit tests for the fact_sales table"

**Cursor will:**
- Call `shift_left_table_init_unit_tests`
- Pass: table_name="fact_sales"
- Generate test structure and files

### Run Unit Tests
**You say:**
> "Run the unit tests for customer_dim"

**Cursor will:**
- Call `shift_left_table_run_unit_tests`
- Pass: table_name="customer_dim"
- Execute tests and show results

### Clean Up Tests
**You say:**
> "Remove the unit test artifacts for fact_orders from Confluent Cloud"

**Cursor will:**
- Call `shift_left_table_delete_unit_tests`
- Pass: table_name="fact_orders"
- Clean up test resources

## Pipeline Deployment

### Deploy a Single Table
**You say:**
> "Deploy the customer_orders table from the pipelines inventory"

**Cursor will:**
- Call `shift_left_pipeline_deploy`
- Pass: inventory_path="./pipelines", table_name="customer_orders"
- Deploy DDL and DML with dependency management

### Deploy by Product
**You say:**
> "Deploy all tables in the sales product to compute pool lfcp-abc123"

**Cursor will:**
- Call `shift_left_pipeline_deploy`
- Pass: inventory_path="./pipelines", product_name="sales", compute_pool_id="lfcp-abc123"
- Deploy all tables in the product

### Deploy DML Only
**You say:**
> "Redeploy just the DML for fact_sales, not the DDL"

**Cursor will:**
- Call `shift_left_pipeline_deploy`
- Pass: inventory_path="./pipelines", table_name="fact_sales", dml_only=true
- Deploy only the INSERT/SELECT statement

### Blue-Green Deployment
**You say:**
> "Deploy only the tables that changed, listed in modified_tables.txt"

**Cursor will:**
- Call `shift_left_pipeline_deploy`
- Pass: inventory_path="./pipelines", table_list_file_name="modified_tables.txt"
- Deploy only changed tables in dependency order

### Parallel Deployment
**You say:**
> "Deploy all tables in the analytics product in parallel"

**Cursor will:**
- Call `shift_left_pipeline_deploy`
- Pass: inventory_path="./pipelines", product_name="analytics", parallel=true
- Deploy tables concurrently where dependencies allow

### Build Pipeline Metadata
**You say:**
> "Generate metadata for the DML file at ./pipelines/facts/fact_sales/dml.fact_sales.sql"

**Cursor will:**
- Call `shift_left_pipeline_build_metadata`
- Pass: dml_file_name="./pipelines/facts/fact_sales/dml.fact_sales.sql", pipeline_path="./pipelines"
- Extract and save pipeline metadata

## Utility Commands

### Check Version
**You say:**
> "What version of shift_left is installed?"

**Cursor will:**
- Call `shift_left_version`
- Display version information

### Validate Configuration
**You say:**
> "Check if my shift_left configuration is valid"

**Cursor will:**
- Call `shift_left_project_validate_config`
- Validate and report any configuration issues

## Complex Workflows

### Complete Migration Workflow
**You say:**
> "I need to migrate all Spark SQL files in ./src/spark/ to Flink, validate them, and create the table structures in ./pipelines/staging/"

**Cursor might:**
1. List files in ./src/spark/
2. For each file, call `shift_left_table_migrate` with validation
3. Show results and any errors
4. Suggest next steps

### Blue-Green Deployment Workflow
**You say:**
> "Show me what changed since main branch, then deploy only those tables"

**Cursor will:**
1. Call `shift_left_project_list_modified_files` with branch_name="main"
2. Save results to a file
3. Call `shift_left_pipeline_deploy` with table_list_file_name pointing to that file
4. Show deployment progress and results

### Full Table Lifecycle
**You say:**
> "Create a new table called 'order_events' in facts, create unit tests for it, then deploy it"

**Cursor will:**
1. Call `shift_left_table_init`
2. Call `shift_left_table_init_unit_tests`
3. Call `shift_left_pipeline_deploy`
4. Report success at each stage

## Context-Aware Assistance

Cursor can use file context too:

**Scenario:** You have a Spark SQL file open

**You say:**
> "Migrate this file to Flink SQL"

**Cursor will:**
- Detect the file path from context
- Infer source_type="spark"
- Ask for table name and target path if needed
- Execute the migration

## Tips for Best Results

1. **Be specific about paths**: Use absolute or clear relative paths
2. **Mention product names**: Helps organize multi-product projects
3. **Specify validation**: Say "validate" when migrating to catch issues early
4. **Use environment variables**: Reference $PIPELINES when configured
5. **Ask for explanations**: "Explain what this command will do before running it"

## Error Handling

If something fails, Cursor will:
- Show the error message from shift_left
- Suggest fixes based on common issues
- Help you retry with corrected parameters

**Example:**
> "Deploy fact_sales table"

**If it fails:**
- Cursor shows: "Configuration missing compute_pool_id"
- Suggests: "Try: Deploy to compute pool lfcp-xyz123"
- Or: "Set COMPUTE_POOL_ID environment variable"

## Getting Help

**You can ask:**
- "What shift_left commands are available?"
- "Show me examples of deploying a table"
- "How do I set up unit tests?"
- "What's the syntax for migrating from Spark to Flink?"

Cursor will explain and show examples!

## Advanced: Combining with File Operations

**You say:**
> "Read the DML file in ./pipelines/facts/fact_orders/, show me the dependencies, then deploy it"

**Cursor will:**
1. Read the file
2. Parse it to show table dependencies
3. Call `shift_left_pipeline_deploy`
4. Monitor the deployment
