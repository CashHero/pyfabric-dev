"""Common helper functions that the generated common_functions notebook inlines.

This file is the source of truth for the `cf_*` helpers used by bronze /
silver / gold modules. The notebook generator extracts everything below
(imports + function bodies) into ``common/common_functions.Notebook``,
which downstream notebooks load via ``%run common_functions``.

In local development the same helpers are available because tests import
them directly from this module via the ``src.common.functions`` namespace.
"""
import logging
import os
from logging import Logger
from pathlib import Path

from pyspark.sql import SparkSession


def cf_get_lakehouse_path(path: str) -> str:
    """Return an absolute path inside the default lakehouse.

    In Fabric this resolves to ``/lakehouse/default/<path>``. Locally it
    routes under ``DEV_BASE_DIR/lakehouse/default/<path>`` so tests share
    one consistent Files/ tree.
    """
    if "DEV_BASE_DIR" in os.environ:
        base = Path(os.environ["DEV_BASE_DIR"]) / "lakehouse" / "default"
    else:
        base = Path("/lakehouse/default")
    return str(base / path.lstrip("/"))


def cf_create_logger(name: str) -> Logger:
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    return logging.getLogger(name)


def cf_create_spark_session() -> SparkSession:
    """Return a SparkSession.

    Fabric provides a session at notebook start; ``getOrCreate()``
    returns it. Locally, ``pyfabric_dev.local_env`` has already wired
    Delta into the builder by the time tests import this module, so the
    same call yields a Delta-enabled session.
    """
    return SparkSession.builder.getOrCreate()
