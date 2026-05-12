"""
Initialize a local development environment that emulates Fabric.

Importing this module:
- Injects `pyfabric_dev.mock_notebookutils` into `sys.modules` under the
  Fabric module names (`notebookutils`, `notebookutils.mssparkutils`,
  `notebookutils.runtime`) so notebooks that `from notebookutils import ...`
  succeed off-cloud.
- Calls `ensure_dev_directories()` to create the local Spark warehouse,
  Derby metastore, and lakehouse `Files/` tree.

Typical use at the top of a test or local-runner script:

    import pyfabric_dev.local_env  # noqa: F401  -- side effects on import
"""
import sys

import pyfabric_dev.mock_notebookutils as _mock_utils
from pyfabric_dev.local_config import ensure_dev_directories

_mock = _mock_utils.get_mock_notebookutils()
sys.modules["notebookutils"] = _mock
sys.modules["notebookutils.mssparkutils"] = _mock.mssparkutils
sys.modules["notebookutils.runtime"] = _mock.runtime

ensure_dev_directories()


def print_package_versions() -> None:
    """Print versions of key packages for diagnostics."""
    import importlib.metadata

    packages = ["pyspark", "delta-spark", "pandas", "numpy"]
    print("\nPackage versions:")
    print("-" * 60)
    for name in packages:
        try:
            version = importlib.metadata.version(name)
            print(f"   {name:20s} = {version}")
        except importlib.metadata.PackageNotFoundError:
            print(f"   {name:20s} = not installed")
    print(f"   {'Python':20s} = {sys.version.split()[0]}")
    print("-" * 60)
