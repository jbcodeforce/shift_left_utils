#!/bin/bash
# Diagnostic script to check shift_left MCP setup for Cursor

echo "================================================"
echo "SHIFT LEFT MCP DIAGNOSTIC TOOL"
echo "================================================"
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

passed=0
failed=0

check_pass() {
    echo -e "${GREEN}✓${NC} $1"
    ((passed++))
}

check_fail() {
    echo -e "${RED}✗${NC} $1"
    ((failed++))
}

check_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

echo "1. Checking Python and uv installation..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    check_pass "Python installed: $PYTHON_VERSION"
else
    check_fail "Python not found"
fi

if command -v uv &> /dev/null; then
    UV_VERSION=$(uv --version)
    check_pass "uv installed: $UV_VERSION"
else
    check_fail "uv not found (install with: curl -LsSf https://astral.sh/uv/install.sh | sh)"
fi
echo ""

echo "2. Checking shift_left CLI installation..."
if command -v shift_left &> /dev/null; then
    check_pass "shift_left CLI found in PATH"
else
    check_warn "shift_left CLI not in PATH (this is OK if using uv run)"
fi
echo ""

echo "3. Checking MCP server files..."
MCP_DIR="$HOME/Documents/Code/shift_left_utils/src/shift_left/shift_left_mcp"
if [ -f "$MCP_DIR/server.py" ]; then
    check_pass "MCP server.py exists"
else
    check_fail "MCP server.py not found at $MCP_DIR"
fi

if [ -f "$MCP_DIR/__main__.py" ]; then
    check_pass "MCP __main__.py exists"
else
    check_fail "MCP __main__.py not found"
fi

if [ -f "$MCP_DIR/tools.py" ]; then
    check_pass "MCP tools.py exists"
else
    check_fail "MCP tools.py not found"
fi
echo ""

echo "4. Checking environment variables..."
if [ -n "$PIPELINES" ]; then
    check_pass "PIPELINES is set: $PIPELINES"
else
    check_fail "PIPELINES not set"
fi

if [ -n "$CONFIG_FILE" ]; then
    check_pass "CONFIG_FILE is set: $CONFIG_FILE"
else
    check_fail "CONFIG_FILE not set"
fi

if [ -n "$STAGING" ]; then
    check_pass "STAGING is set: $STAGING"
else
    check_fail "STAGING not set"
fi

if [ -n "$SL_CONFLUENT_CLOUD_API_KEY" ]; then
    check_pass "SL_CONFLUENT_CLOUD_API_KEY is set"
else
    check_fail "SL_CONFLUENT_CLOUD_API_KEY not set"
fi

if [ -n "$SL_FLINK_API_KEY" ]; then
    check_pass "SL_FLINK_API_KEY is set"
else
    check_fail "SL_FLINK_API_KEY not set"
fi

if [ -n "$SL_KAFKA_API_KEY" ]; then
    check_pass "SL_KAFKA_API_KEY is set"
else
    check_fail "SL_KAFKA_API_KEY not set"
fi
echo ""

echo "5. Testing MCP server..."
cd "$HOME/Documents/Code/shift_left_utils/src/shift_left"
if uv run python -m shift_left_mcp.test_server > /tmp/mcp_test.log 2>&1; then
    check_pass "MCP server test passed"
else
    check_fail "MCP server test failed (see /tmp/mcp_test.log)"
fi
echo ""

echo "6. Checking MCP dependencies..."
cd "$HOME/Documents/Code/shift_left_utils/src/shift_left"
if uv pip list 2>/dev/null | grep -q "mcp"; then
    MCP_VERSION=$(uv pip list 2>/dev/null | grep "^mcp " | awk '{print $2}')
    check_pass "MCP package installed: version $MCP_VERSION"
else
    check_fail "MCP package not installed"
fi
echo ""

echo "================================================"
echo "SUMMARY"
echo "================================================"
echo -e "${GREEN}Passed: $passed${NC}"
echo -e "${RED}Failed: $failed${NC}"
echo ""

if [ $failed -eq 0 ]; then
    echo -e "${GREEN}✓ All checks passed!${NC}"
    echo ""
    echo "Your MCP server is ready. Next steps:"
    echo "  1. Make sure the MCP configuration is in Cursor settings"
    echo "  2. Quit Cursor completely (Cmd+Q)"
    echo "  3. Run: source ~/Documents/Code/shift_left_utils/set_cursor_env.sh"
    echo "  4. Run: open -a Cursor"
    echo "  5. Test with: 'What is the version of shift_left?'"
else
    echo -e "${RED}✗ Some checks failed${NC}"
    echo ""
    echo "Common fixes:"
    if [ -z "$PIPELINES" ] || [ -z "$CONFIG_FILE" ]; then
        echo "  • Set environment variables:"
        echo "    source ~/Documents/Code/shift_left_utils/set_j9r_env"
    fi
    echo ""
    echo "For detailed help, see:"
    echo "  • MCP_QUICK_FIX.md"
    echo "  • CURSOR_MCP_SETUP.md"
fi
echo ""

