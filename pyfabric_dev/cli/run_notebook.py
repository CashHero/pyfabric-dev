#!/usr/bin/env python3
"""CLI entry point: run a single Fabric notebook locally.

For project-specific behavior (helper-function overrides, per-notebook
globals injection), import :class:`pyfabric_dev.runners.NotebookRunner`
directly and pass a :class:`RunnerHooks` instance — that surface gives
you full control over what the runner does. This CLI is the
hook-less default.
"""
import sys
from pathlib import Path

import pyfabric_dev.local_env  # noqa: F401 — side effects: mock notebookutils, dev dirs
from pyfabric_dev.runners import NotebookRunner


def main():
    if len(sys.argv) < 2:
        print("Usage: pyfabric-run-notebook <notebook_path> [--project-root <dir>]")
        print("\nExamples:")
        print("  pyfabric-run-notebook bronze/10_bronze_ingest.Notebook/notebook-content.py")
        print("  pyfabric-run-notebook common/common_functions.Notebook")
        sys.exit(1)

    args = sys.argv[1:]
    project_root = Path.cwd()
    if "--project-root" in args:
        i = args.index("--project-root")
        project_root = Path(args[i + 1]).resolve()
        args = args[:i] + args[i + 2:]

    notebook_arg = args[0]
    notebook_path = Path(notebook_arg)
    if not notebook_path.is_absolute():
        notebook_path = project_root / notebook_path
    if notebook_path.is_dir():
        notebook_path = notebook_path / "notebook-content.py"

    runner = NotebookRunner(notebook_path, project_root)
    runner.run()


if __name__ == "__main__":
    main()
