#!/usr/bin/env python3
"""CLI entry point: run a Fabric pipeline locally.

For project-specific behavior, import
:class:`pyfabric_dev.runners.PipelineRunner` directly and pass a
:class:`RunnerHooks` instance.
"""
import sys
from pathlib import Path

import pyfabric_dev.local_env  # noqa: F401
from pyfabric_dev.local_config import MEDALLION_LAYERS
from pyfabric_dev.runners.pipeline import (
    FabricIdResolver,
    PipelineRunner,
    find_pipeline,
    list_pipelines,
)


USAGE = """\
Usage:
    pyfabric-run-pipeline <pipeline_name_or_path> [--dry-run] [--project-root <dir>]
    pyfabric-run-pipeline --list [--project-root <dir>]
"""


def main():
    args = sys.argv[1:]
    if not args or args[0] in ("--help", "-h"):
        print(USAGE)
        sys.exit(0 if args else 1)

    project_root = Path.cwd()
    if "--project-root" in args:
        i = args.index("--project-root")
        project_root = Path(args[i + 1]).resolve()
        args = args[:i] + args[i + 2:]

    if args and args[0] == "--list":
        list_pipelines(project_root)
        sys.exit(0)

    dry_run = "--dry-run" in args
    pipeline_args = [a for a in args if a != "--dry-run"]
    if not pipeline_args:
        print("Error: pipeline name required")
        sys.exit(1)
    pipeline_arg = pipeline_args[0]

    resolver = FabricIdResolver(project_root)
    pipeline_path = find_pipeline(project_root, pipeline_arg, resolver, MEDALLION_LAYERS)
    if not pipeline_path or not pipeline_path.exists():
        print(f"❌ Could not find pipeline: {pipeline_arg}")
        print("   Use --list to see available pipelines")
        sys.exit(1)

    runner = PipelineRunner(project_root, resolver, dry_run=dry_run)

    print("🚀 Fabric Pipeline Local Runner")
    print("=" * 60)
    print(f"📁 Project root: {project_root}")
    print(f"📄 Pipeline: {pipeline_path.parent.name}")
    if dry_run:
        print("🔍 DRY RUN — showing execution plan only")

    success = runner.run_pipeline(pipeline_path)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
