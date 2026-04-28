source set_test_env
# dev extra includes pytest; without it `uv run pytest` can pick up a system pytest (wrong interpreter)
uv sync --extra dev
uv run python -m pytest tests/ut/utils -v --tb=short
uv run python -m pytest tests/ut/core -v --tb=short
uv run python -m pytest tests/ut/cli -v --tb=short
