"""Pytest configuration for integration tests. All tests in tests/it/ are marked as integration."""
import pytest

def pytest_collection_modifyitems(items):
    """Mark all tests in this directory as integration tests."""
    for item in items:
        item.add_marker(pytest.mark.integration)
