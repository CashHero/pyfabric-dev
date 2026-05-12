"""Shared fixtures: Spark, logger, project path setup, mock notebookutils."""
import logging
import sys
from pathlib import Path

import pytest

EXAMPLE_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(EXAMPLE_ROOT))

# Bootstrap the local environment (mocks notebookutils, ensures dev dirs).
import pyfabric_dev.local_env  # noqa: E402, F401
from pyfabric_dev.spark import create_spark_session  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    return create_spark_session()


@pytest.fixture(scope="session")
def logger():
    log = logging.getLogger("minimal-medallion")
    log.setLevel(logging.INFO)
    return log
