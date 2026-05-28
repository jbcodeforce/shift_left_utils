
import os
import pathlib
import unittest

from shift_left.core.utils.app_config import get_config
from typer.testing import CliRunner


_TESTS_ROOT = pathlib.Path(__file__).resolve().parent.parent
_CONFIG_CCLOUD = _TESTS_ROOT / "config-ccloud.yaml"
_PIPELINES = _TESTS_ROOT / "data" / "flink-project" / "pipelines"
_SRC_FOLDER = _TESTS_ROOT / "data" / "spark-project"



class SLUnitTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SL_CONFIG_FILE", str(_CONFIG_CCLOUD))
        os.environ.setdefault("PIPELINES", str(_PIPELINES))
        os.environ.setdefault("SL_SRC_FOLDER", str(_SRC_FOLDER))
        cls._config = get_config()
        cls.pipelines = os.environ["PIPELINES"]
        cls.runner = CliRunner()
