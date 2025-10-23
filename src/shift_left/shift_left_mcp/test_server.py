
"""
Test script for shift_left MCP server
Validates that the MCP server can list and execute tools correctly
"""
import asyncio
from shift_left_mcp.server import handle_list_tools, handle_call_tool
from shift_left_mcp.command_builder import build_command


async def test_list_tools():
    """Test listing all available tools."""
    print("=" * 60)
    print("Testing list_tools...")
    print("=" * 60)
    
    tools = await handle_list_tools()
    print(f"\nFound {len(tools)} tools:")
    for tool in tools:
        print(f"  - {tool.name}: {tool.description[:60]}...")
    
    assert len(tools) > 0, "Should have at least one tool"
    print("\n✓ list_tools passed\n")
    return tools


async def test_version_command():
    """Test the version command."""
    print("=" * 60)
    print("Testing shift_left version command...")
    print("=" * 60)
    
    result = await handle_call_tool("shift_left_version", {})
    print(f"\nResult:")
    for content in result:
        print(content.text)
    
    assert len(result) > 0, "Should return content"
    print("\n✓ version command passed\n")


def test_command_building():
    """Test command building logic."""
    print("=" * 60)
    print("Testing command building...")
    print("=" * 60)
    
    test_cases = [
        {
            "tool": "shift_left_project_init",
            "args": {
                "project_name": "test_project",
                "project_path": "./tmp",
                "project_type": "kimball"
            },
            "expected": ["shift_left", "project", "init", "test_project", "./tmp", "--project-type", "kimball"]
        },
        {
            "tool": "shift_left_version",
            "args": {},
            "expected": ["shift_left", "version"]
        },
        {
            "tool": "shift_left_table_init",
            "args": {
                "table_name": "test_table",
                "table_path": "./tables"
            },
            "expected": ["shift_left", "table", "init", "test_table", "./tables"]
        },
        {
            "tool": "shift_left_pipeline_deploy",
            "args": {
                "inventory_path": "./pipelines",
                "table_name": "fact_sales",
                "dml_only": True
            },
            "expected": ["shift_left", "pipeline", "deploy", "./pipelines", "--table-name", "fact_sales", "--dml-only"]
        }
    ]
    
    for i, test in enumerate(test_cases, 1):
        cmd = build_command(test["tool"], test["args"])
        print(f"\nTest {i}: {test['tool']}")
        print(f"  Args: {test['args']}")
        print(f"  Built: {' '.join(cmd)}")
        print(f"  Expected: {' '.join(test['expected'])}")
        
        assert cmd == test["expected"], f"Command mismatch for {test['tool']}"
        print(f"  ✓ Passed")
    
    print("\n✓ All command building tests passed\n")


async def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("SHIFT LEFT MCP SERVER TEST SUITE")
    print("=" * 60 + "\n")
    
    try:
        # Test command building (synchronous)
        test_command_building()
        
        # Test listing tools
        tools = await test_list_tools()
        
        # Test version command
        await test_version_command()
        
        print("=" * 60)
        print("ALL TESTS PASSED ✓")
        print("=" * 60)
        print(f"\nThe MCP server is working correctly with {len(tools)} available tools.")
        print("\nNext steps:")
        print("1. Configure Cursor (see docs/mcp/index.md)")
        print("2. Restart Cursor")
        print("3. Start using shift_left tools in Cursor!\n")
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)

