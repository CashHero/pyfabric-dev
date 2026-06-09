#!/usr/bin/env python3
"""Stage the sample CSV into the local lakehouse Files tree.

In Fabric you upload ``data/sales.csv`` to the bronze lakehouse's ``Files/``
area. Locally the equivalent is copying it under
``DEV_BASE_DIR/lakehouse/default/Files/``, which is where the bronze notebook's
``cf_get_lakehouse_path("Files/data/sales.csv")`` resolves.

Run this once (from this directory) before ``pyfabric-run-notebook`` or
``pyfabric-run-pipeline``:

    python stage_data.py
"""
import shutil
from pathlib import Path

# Importing local_env bootstraps the dev dirs and exports DEV_BASE_DIR so the
# lakehouse paths below resolve the same way the runners and notebooks see them.
import pyfabric_dev.local_env  # noqa: F401
from pyfabric_dev.local_config import LOCAL_FILES_PATH

EXAMPLE_ROOT = Path(__file__).resolve().parent


def main() -> None:
    src = EXAMPLE_ROOT / "data"
    dst = Path(LOCAL_FILES_PATH) / "data"
    dst.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, dst, dirs_exist_ok=True)
    print(f"Staged {src} -> {dst}")


if __name__ == "__main__":
    main()
