"""
Local development configuration for emulating Fabric off-cloud.

All values are environment-variable driven so consumer projects can
override defaults without editing the package. Drop a ``.env`` file at
the consumer project root (CWD) and python-dotenv will load it on import.

Key knobs:

- ``DEV_BASE_DIR`` — full path to the local dev base dir. If unset,
  defaults to ``~/<FABRIC_DEV_BASE_DIR_NAME>/<worktree-hash>``.
- ``FABRIC_DEV_BASE_DIR_NAME`` — name of the base dir under ``~``.
  Defaults to ``.fabric_dev``.
- ``SPARK_MASTER`` — defaults to ``local[*]``.
- ``KEY_VAULT_URL`` — Azure Key Vault URL for secret resolution; if
  unset, ``mock_notebookutils`` falls through to environment variables.
- ``FABRIC_WORKSPACE_ID`` — workspace ID returned by the mocked
  ``notebookutils.runtime.context``.

The worktree hash isolates parallel checkouts of the same project so
their lakehouses and Derby metastores don't collide. It's derived from
the current working directory at the time the module is imported.
"""
import hashlib
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    _env_file = Path.cwd() / ".env"
    if _env_file.exists():
        load_dotenv(_env_file)
except ImportError:
    pass

# Medallion layers — used by dev tools that scan a project's source tree.
# Override with FABRIC_MEDALLION_LAYERS as a comma-separated list.
MEDALLION_LAYERS = tuple(
    layer.strip()
    for layer in os.getenv("FABRIC_MEDALLION_LAYERS", "bronze,silver,gold").split(",")
    if layer.strip()
)

# Base directory for local data storage. Per-CWD hash so parallel
# worktrees / branches don't collide.
_worktree_hash = hashlib.md5(str(Path.cwd().resolve()).encode()).hexdigest()[:8]
_default_base_dir_name = os.getenv("FABRIC_DEV_BASE_DIR_NAME", ".fabric_dev")
DEV_BASE_DIR = Path(
    os.getenv("DEV_BASE_DIR", Path.home() / _default_base_dir_name / _worktree_hash)
)

# Local lakehouse paths (stand in for /lakehouse/default in Fabric).
LOCAL_LAKEHOUSE_PATH = DEV_BASE_DIR / "lakehouse" / "default"
LOCAL_TABLES_PATH = LOCAL_LAKEHOUSE_PATH / "Tables"
LOCAL_FILES_PATH = LOCAL_LAKEHOUSE_PATH / "Files"

# Spark configuration.
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_WAREHOUSE_DIR = LOCAL_TABLES_PATH
SPARK_METASTORE_DIR = DEV_BASE_DIR / "metastore"

# Key Vault URL — not used locally, but kept for compatibility with code
# that calls into Fabric secret resolution paths.
KEY_VAULT_URL = os.getenv("KEY_VAULT_URL", "https://local-dev.vault.azure.net/")

# Workspace ID returned by the mocked notebookutils.runtime.context.
WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID", "local-dev-workspace-id")


def ensure_dev_directories() -> None:
    """Create the local dev directory tree if it doesn't exist yet."""
    LOCAL_TABLES_PATH.mkdir(parents=True, exist_ok=True)
    LOCAL_FILES_PATH.mkdir(parents=True, exist_ok=True)
    (LOCAL_FILES_PATH / "config").mkdir(parents=True, exist_ok=True)
    SPARK_METASTORE_DIR.mkdir(parents=True, exist_ok=True)
    DEV_BASE_DIR.mkdir(parents=True, exist_ok=True)
