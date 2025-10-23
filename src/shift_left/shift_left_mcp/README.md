# Shift Left MCP Module

This module provides MCP (Model Context Protocol) server integration for the shift_left CLI, enabling seamless integration with AI assistants like Cursor.

**Note:** This module is named `shift_left_mcp` to avoid naming conflicts with the installed `mcp` package (MCP SDK).

## Structure

```
shift_left_mcp/
├── __init__.py          # Package initialization
├── __main__.py          # Entry point (python -m shift_left_mcp)
├── server.py            # MCP server implementation
├── tools.py             # Tool definitions for all shift_left commands
├── command_builder.py   # Converts MCP calls to CLI commands
└── test_server.py       # Test suite for the MCP server
```

## Running the Server

### As a Python Module (Recommended)

```bash
uv run python -m shift_left_mcp
```

### Directly

```bash
uv run python -m shift_left_mcp.server
```

### Testing

```bash
uv run python -m shift_left_mcp.test_server
```

## Usage in Cursor

Once configured (see [docs/mcp/index.md](../../../../docs/mcp/index.md)), the server runs automatically when Cursor needs to execute shift_left commands.

## Architecture

### server.py
Main MCP server that:
- Implements MCP protocol handlers
- Lists available tools
- Executes shift_left CLI commands via subprocess
- Returns results to the AI assistant

### tools.py
Defines all available shift_left commands as MCP tool schemas with:
- Tool names
- Descriptions
- Input schemas (parameters, types, requirements)

### command_builder.py
Converts MCP tool calls into shift_left CLI commands:
- Maps tool names to CLI command structure
- Handles positional and optional arguments
- Builds proper command arrays for subprocess execution

### test_server.py
Validates the MCP server:
- Tests tool listing
- Validates command building
- Executes sample commands
- Verifies proper operation

## Adding New Tools

1. Add tool definition to `tools.py`:
```python
{
    "name": "shift_left_new_command",
    "description": "What the command does",
    "inputSchema": {
        "type": "object",
        "properties": {
            "param_name": {
                "type": "string",
                "description": "Parameter description"
            }
        },
        "required": ["param_name"]
    }
}
```

2. Add command mapping to `command_builder.py`:
```python
command_map = {
    ...
    "shift_left_new_command": ["shift_left", "category", "command"],
}

# Add argument handling
elif tool_name == "shift_left_new_command":
    cmd.extend([arguments["param_name"]])
```

3. Add test case to `test_server.py`:
```python
{
    "tool": "shift_left_new_command",
    "args": {"param_name": "value"},
    "expected": ["shift_left", "category", "command", "value"]
}
```

4. Run tests to verify:
```bash
uv run python -m shift_left.mcp.test_server
```

## Dependencies

- `mcp>=0.9.0` - Model Context Protocol SDK
- Standard library modules (subprocess, asyncio)

## Environment

The MCP server inherits the environment from where it's launched, including:
- PATH (for shift_left CLI access)
- Environment variables (PIPELINES, CONFIG_FILE, etc.)
- Confluent Cloud credentials

## Error Handling

The server handles:
- Invalid tool names
- Missing required parameters
- Command execution failures
- Timeouts (5 minute default)
- Subprocess errors

All errors are returned as text content to the AI assistant.

## Security

- Commands run with the user's permissions
- No privilege escalation

## Version

Current version: 0.1.0

Compatible with:
- shift_left CLI 0.1.41+
- MCP SDK 0.9.0+
- Python 3.10+

## Related Documentation

- [MCP Documentation](../../../../docs/mcp/index.md)

