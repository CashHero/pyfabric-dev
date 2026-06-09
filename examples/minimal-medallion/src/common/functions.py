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
from logging import Logger  # noqa: F401  — re-exported to notebooks via %run common_functions
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: F401  — re-exported to notebooks via %run common_functions


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


def cf_create_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    return logging.getLogger(name)


def cf_create_spark_session() -> SparkSession:
    """Return a SparkSession.

    Fabric provides a session at notebook start; ``getOrCreate()``
    returns it. Locally the runner creates a Delta-enabled session before any
    cell executes, so this same call yields that session.
    """
    return SparkSession.builder.getOrCreate()


def cf_overwrite_table(spark: SparkSession, df, table_name: str) -> None:
    """Idempotently (re)create a managed Delta table from ``df``.

    Uses ``DROP TABLE`` + ``saveAsTable`` rather than
    ``mode("overwrite").saveAsTable`` so the same code runs on the open-source
    Delta build used in local dev — which doesn't support the catalog truncate
    path a managed overwrite triggers — as well as in Fabric. The effect (a full
    replace of the table's contents) is identical in both.
    """
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    df.write.format("delta").saveAsTable(table_name)
