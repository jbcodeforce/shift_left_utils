import os


def _run_integration_tests() -> bool:
    """True when cloud IT is requested and API key is not the conftest placeholder."""
    cloud = os.environ.get("SHIFT_LEFT_RUN_CLOUD_IT", "").lower() in (
        "1",
        "true",
        "yes",
    )
    key = os.environ.get("SL_CONFLUENT_CLOUD_API_KEY", "")
    return bool(cloud and key not in ("", "test"))

_SKIP_MSG = (
    "Set SHIFT_LEFT_RUN_CLOUD_IT=1 and use real Confluent credentials "
    "(e.g. source set_demo_env from src/shift_left). "
    "conftest's SL_CONFLUENT_CLOUD_API_KEY=test yields empty API results."
)
