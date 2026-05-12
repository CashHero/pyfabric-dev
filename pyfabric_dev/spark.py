"""
Environment-aware Spark session and logging utilities.

This module provides a unified API for creating Spark sessions that work
in both Microsoft Fabric and local development environments.

Usage:
    from pyfabric_dev.spark import create_spark_session, create_logger

    spark = create_spark_session()
    logger = create_logger("my_pipeline")
"""

import logging
import os
import sys
from logging import Logger
from pathlib import Path
from typing import Literal

from pyspark.sql import SparkSession


# Environment detection
_environment: Literal["fabric", "local"] | None = None


def get_environment() -> Literal["fabric", "local"]:
    """
    Detect whether we're running in Microsoft Fabric or locally.

    Detection is based on the presence of the real notebookutils module.
    In Fabric, notebookutils is a real module. Locally, it's either not
    present or is our MockNotebookUtils instance.

    Returns:
        "fabric" if running in Microsoft Fabric, "local" otherwise
    """
    global _environment

    if _environment is not None:
        return _environment

    try:
        import notebookutils
        notebookutils_type = str(type(notebookutils))

        if 'MockNotebookUtils' in notebookutils_type or 'mock' in notebookutils_type.lower():
            _environment = "local"
        elif hasattr(notebookutils, '__file__') and notebookutils.__file__ is None:
            # Real Fabric module typically has __file__ = None
            _environment = "fabric"
        else:
            # Additional check: module vs instance
            import types
            if isinstance(notebookutils, types.ModuleType):
                _environment = "fabric"
            else:
                _environment = "local"
    except (ImportError, ModuleNotFoundError, AttributeError):
        _environment = "local"

    return _environment


def _setup_local_environment():
    """Set up mock modules for local development."""
    # Only import and set up mocks if we're running locally
    try:
        from pyfabric_dev.mock_notebookutils import get_mock_notebookutils
        mock_utils = get_mock_notebookutils()
        sys.modules['notebookutils'] = mock_utils
        sys.modules['notebookutils.mssparkutils'] = mock_utils.mssparkutils
        sys.modules['notebookutils.runtime'] = mock_utils.runtime
    except ImportError:
        # dev module not available, create minimal mocks
        pass


def get_lakehouse_path(path: str) -> str:
    """
    Get the full path to a lakehouse resource.

    In Fabric, this returns /lakehouse/default/{path}.
    In local development, this returns ~/<base>/lakehouse/default/{path}
    (the local base dir is configurable via the DEV_BASE_DIR /
    FABRIC_DEV_BASE_DIR_NAME env vars; see pyfabric_dev.local_config).

    Args:
        path: Relative path within the lakehouse (e.g., "Tables/my_table")

    Returns:
        Full path to the resource
    """
    if get_environment() == "local":
        from pyfabric_dev.local_config import LOCAL_LAKEHOUSE_PATH
        return str(LOCAL_LAKEHOUSE_PATH / path.lstrip('/'))
    else:
        from pyfabric_dev.fabric import cf_get_lakehouse_path
        return cf_get_lakehouse_path(path)


def get_current_workspace_id() -> str:
    """Get the current Fabric workspace ID."""
    if get_environment() == "local":
        return os.getenv("FABRIC_WORKSPACE_ID", "local-dev-workspace-id")
    else:
        from pyfabric_dev.fabric import cf_get_current_workspace_id
        return cf_get_current_workspace_id()


def get_lakehouse_id(lakehouse_name: str) -> str:
    """Get the ID of a lakehouse by name."""
    if get_environment() == "local":
        return f"local-{lakehouse_name}-id"
    else:
        from pyfabric_dev.fabric import cf_get_lakehouse_id
        return cf_get_lakehouse_id(lakehouse_name)


def create_logger(name: str) -> Logger:
    """
    Create a logger with standard formatting.

    This function is now defined in src.common.functions and imported here
    for backward compatibility.

    Args:
        name: Name for the logger (typically the notebook/module name)

    Returns:
        Configured Logger instance
    """
    from pyfabric_dev.functions import cf_create_logger
    return cf_create_logger(name)


def create_spark_session() -> SparkSession:
    """
    Create a Spark session configured for the current environment.

    In Fabric, returns the existing session or creates a minimal one.
    In local development, creates a fully configured session with Delta Lake support.

    Returns:
        SparkSession configured for the current environment
    """
    if get_environment() == "fabric":
        from pyfabric_dev.fabric import cf_create_spark_session
        return cf_create_spark_session()
    else:
        return _create_local_spark_session()


def _create_local_spark_session() -> SparkSession:
    """Create a Spark session configured for local development."""
    from pyfabric_dev.local_config import SPARK_MASTER, SPARK_WAREHOUSE_DIR, SPARK_METASTORE_DIR

    try:
        import delta
        import importlib.metadata
        from pyspark import __version__ as spark_version

        print("Creating Spark Session with versions:")
        print(f"   PySpark: {spark_version}")
        try:
            delta_version = importlib.metadata.version("delta-spark")
            print(f"   Delta Spark: {delta_version}")
        except Exception:
            delta_version = None
            print("   Delta Spark: (version unknown)")
        print()

    except ImportError:
        spark_version = "unknown"
        delta_version = None

    # Derby metastore connection URL - persists catalog across sessions
    metastore_url = f"jdbc:derby:;databaseName={SPARK_METASTORE_DIR}/metastore_db;create=true"

    builder = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(os.getenv("FABRIC_DEV_APP_NAME", "fabric-local-dev"))
        .config("spark.sql.warehouse.dir", str(SPARK_WAREHOUSE_DIR))
        .config("javax.jdo.option.ConnectionURL", metastore_url)
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .enableHiveSupport()

        # Memory configuration for local development. Tunable via env vars
        # so resource-constrained environments (GitHub-hosted runners,
        # laptops with <16GB RAM) can dial the JVM heap down without
        # editing the package. Defaults match a developer workstation.
        .config("spark.driver.memory", os.getenv("FABRIC_DEV_DRIVER_MEMORY", "8g"))
        .config("spark.driver.maxResultSize", os.getenv("FABRIC_DEV_DRIVER_MAX_RESULT_SIZE", "4g"))
        .config("spark.executor.memory", os.getenv("FABRIC_DEV_EXECUTOR_MEMORY", "8g"))
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")

        # Delta Lake configuration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.default", "delta")

        # Delta Lake optimizations
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

        # Compression optimizations
        .config("spark.io.compression.lz4.blockSize", "128kb")
        .config("spark.rdd.compress", "true")

        # Serialization optimizations
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "128m")

        # SQL optimizations
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.sql.legacy.createHiveTableByDefault", "false")
    )

    # Configure Delta Lake JARs
    if delta_version:
        try:
            delta_package_path = Path(delta.__file__).parent
            delta_jars = list(delta_package_path.glob("**/delta-*.jar"))

            if delta_jars:
                jar_paths = ",".join([str(jar.absolute()) for jar in delta_jars])
                builder = builder.config("spark.jars", jar_paths)
            else:
                # Use Maven coordinates
                spark_major_minor = ".".join(spark_version.split(".")[:2])
                scala_version = "2.12" if spark_major_minor.startswith("3.") else "2.13"
                delta_packages = f"io.delta:delta-spark_{scala_version}:{delta_version}"
                builder = builder.config("spark.jars.packages", delta_packages)
        except Exception:
            pass

    return builder.getOrCreate()


# Aliases for backward compatibility with existing notebook code
cf_get_lakehouse_path = get_lakehouse_path
cf_get_current_workspace_id = get_current_workspace_id
cf_get_lakehouse_id = get_lakehouse_id
cf_create_logger = create_logger
cf_create_spark_session = create_spark_session
