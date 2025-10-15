source set_test_env
uv run pytest tests/ut/utils -v --tb=short
uv run pytest tests/ut/core -v --tb=short
uv run pytest tests/ut/cli -v --tb=short