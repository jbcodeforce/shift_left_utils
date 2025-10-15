"""
Entry point for running the shift_left MCP server.
Usage: python -m shift_left_mcp
"""
import asyncio
from .server import main

if __name__ == "__main__":
    asyncio.run(main())

