#!/usr/bin/env python3
"""
MCP Server for shift_left CLI tools
Exposes shift_left commands as MCP tools for Cursor integration
"""
import subprocess
from typing import Any
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio

from .tools import TOOLS
from .command_builder import build_command

# Create the MCP server instance
server = Server("shift-left-tools")


@server.list_prompts()
async def handle_list_prompts() -> list[types.Prompt]:
    """List available prompts (instructions for Cursor)."""
    return [
        types.Prompt(
            name="instructions",
            description="Instructions for using shift_left CLI tools via MCP",
            arguments=[],
        )
    ]


@server.get_prompt()
async def handle_get_prompt(
    name: str, arguments: dict[str, str] | None
) -> types.GetPromptResult:
    """Return instructions prompt so Cursor can discover server info."""
    if name != "instructions":
        raise ValueError(f"Unknown prompt: {name}")

    return types.GetPromptResult(
        description="Instructions for using shift_left MCP tools",
        messages=[
            types.PromptMessage(
                role="user",
                content=types.TextContent(
                    type="text",
                    text=(
                        "Use the shift_left MCP tools to run shift_left CLI commands. "
                        "Available tools mirror the CLI: project init/validate-config, "
                        "table init/migrate, pipeline deploy, etc. "
                        "Call the appropriate tool with the required arguments; "
                        "optional args can be omitted. Use PIPELINES, CONFIG_FILE, STAGING "
                        "env vars when relevant (configured in MCP server env)."
                    ),
                ),
            )
        ],
    )


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List available shift_left tools."""
    tools = [
        types.Tool(
            name=tool["name"],
            description=tool["description"],
            inputSchema=tool["inputSchema"]
        )
        for tool in TOOLS
    ]
    return tools


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """Execute a shift_left CLI command."""

    if arguments is None:
        arguments = {}

    try:
        # Build the command
        cmd = build_command(name, arguments)

        # Execute the command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )

        # Prepare output
        output = []
        if result.stdout:
            output.append(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            output.append(f"STDERR:\n{result.stderr}")

        output_text = "\n\n".join(output) if output else "Command completed successfully (no output)"

        if result.returncode != 0:
            output_text = f"Command failed with exit code {result.returncode}\n\n{output_text}"

        return [types.TextContent(type="text", text=output_text)]

    except subprocess.TimeoutExpired:
        return [types.TextContent(
            type="text",
            text="Error: Command timed out after 5 minutes"
        )]
    except Exception as e:
        return [types.TextContent(
            type="text",
            text=f"Error executing command: {str(e)}"
        )]


async def main():
    """Run the MCP server."""
    import sys
    sys.stderr.write("shift_left MCP server starting\n")
    sys.stderr.flush()
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="shift-left-tools",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
                instructions=(
                    "Use the shift_left MCP tools to run shift_left CLI commands. "
                    "Available tools mirror the CLI: project init/validate-config, "
                    "table init/migrate, pipeline deploy, etc. Call the appropriate "
                    "tool with the required arguments; optional args can be omitted. "
                    "Use PIPELINES, CONFIG_FILE, STAGING env vars when relevant."
                ),
            )
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

