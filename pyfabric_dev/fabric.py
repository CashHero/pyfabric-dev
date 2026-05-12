"""
Microsoft Fabric-specific utilities.

Functions here call into ``notebookutils`` / ``mssparkutils`` and are
intended for Fabric runtime. For environment-aware versions that work
both locally and in Fabric, see :mod:`pyfabric_dev.spark`.

``notebookutils`` is imported lazily inside each function so that
``import pyfabric_dev.fabric`` succeeds in a local Python without
needing to register the mock first. The mock is registered by importing
:mod:`pyfabric_dev.local_env` (typically from your test conftest).
"""

from pyspark.sql import SparkSession

from pyfabric_dev.defs import DEFAULT_LAKEHOUSE_PATH


def cf_get_lakehouse_path(path: str) -> str:
    """Return the full path to a lakehouse resource in Fabric."""
    return f"{DEFAULT_LAKEHOUSE_PATH}/{path}"


def cf_get_current_workspace_id() -> str:
    """Return the current Fabric workspace ID via ``notebookutils.runtime.context``."""
    from notebookutils import mssparkutils

    return mssparkutils.runtime.context.get("currentWorkspaceId")


def cf_get_lakehouse_id(lakehouse_name: str) -> str:
    """Return the Fabric lakehouse ID for a lakehouse looked up by name."""
    from notebookutils import mssparkutils

    lakehouse = mssparkutils.lakehouse.get(lakehouse_name)
    return lakehouse["id"]


def cf_create_spark_session() -> SparkSession:
    """Get or create the Spark session in Fabric."""
    return SparkSession.builder.getOrCreate()
