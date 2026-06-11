#!/usr/bin/env python3
"""
Generate Microsoft Fabric notebooks from Python modules.

This script generates Fabric-compatible .Notebook directories from the
Python modules in src/. Generated notebooks import and execute the
corresponding module functions.

Usage:
    python dev/generate_notebooks.py

    # Generate only specific notebooks
    python dev/generate_notebooks.py --only bronze

    # Regenerate all notebooks (including common)
    python dev/generate_notebooks.py --all

    # Dry run (show what would be generated)
    python dev/generate_notebooks.py --dry-run
"""

import argparse
import ast
import importlib.util
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


# Project root is resolved at CLI invocation (via --project-root, or
# Path.cwd() as a fallback). All source/output path computations route
# through this global so the generator works for any consumer, not just
# the package install location.
PROJECT_ROOT: Path = Path.cwd()

# Lakehouse config is loaded lazily after PROJECT_ROOT is set in main().
LAKEHOUSE_CONFIGS: dict = {}

# Notebook-generation config: declares consumer-specific generator behavior
# (extra modules to inline, helper notebooks, per-notebook %run deps) without
# hardcoding any one project's module names here. Loaded in main().
NOTEBOOK_GEN_CONFIG: dict = {}

# Defaults when no config/notebook_generation.json is present. These reproduce
# the generator's baseline behavior: inline the framework runtime modules that
# the consumer vendors in src/common, no extra domain modules, no helper
# notebooks, no extra %run deps.
NOTEBOOK_GEN_DEFAULTS = {
    # When True, the defs/functions/fabric slots are inlined from the installed
    # pyfabric_dev package rather than from src/common — so consumers keep only
    # their own code in src/common and let pip own the framework runtime.
    "inline_framework_modules": False,
    # Extra src/common modules inlined into common_functions after the primary
    # functions module (e.g. ["domain_helpers.py"]).
    "common_functions_extra_modules": [],
    "helper_notebooks": [],
    "notebook_run_dependencies": {},
}


def framework_module_path(module_name: str) -> Path:
    """Resolve the source path of a framework runtime module in the installed package.

    e.g. framework_module_path("functions") -> .../pyfabric_dev/functions.py.
    Used when inline_framework_modules is set, so generated notebooks inline the
    framework's own code straight from the installed package instead of from a
    vendored copy under src/common.
    """
    # find_spec locates the module's source file without importing/executing it,
    # so generation doesn't require the framework's runtime deps (pyspark, etc.).
    spec = importlib.util.find_spec(f"pyfabric_dev.{module_name}")
    if spec is None or not spec.origin:
        raise ImportError(
            f"Could not locate pyfabric_dev.{module_name}. inline_framework_modules "
            "inlines framework code from the installed pyfabric-dev package; "
            "ensure it is installed (pip install pyfabric-dev)."
        )
    path = Path(spec.origin)
    if not path.exists():
        raise FileNotFoundError(
            f"pyfabric_dev.{module_name} resolved to {path} but the file is missing. "
            "Reinstall pyfabric-dev."
        )
    return path


def load_lakehouse_config(project_root: Path | None = None) -> dict:
    """Load lakehouse configuration from <project_root>/config/lakehouse_config.json."""
    root = project_root if project_root is not None else PROJECT_ROOT
    config_path = root / "config" / "lakehouse_config.json"

    if not config_path.exists():
        print(f"Warning: Lakehouse config not found at {config_path}")
        print("Using empty config. Create config/lakehouse_config.json from template.")
        return {"bronze": {}, "silver": {}, "gold": {}, "tests": {}, "common": {}}

    with open(config_path) as f:
        return json.load(f)


def load_notebook_generation_config(project_root: Path | None = None) -> dict:
    """Load generator config from <project_root>/config/notebook_generation.json.

    Optional. When absent, NOTEBOOK_GEN_DEFAULTS are used, which reproduce the
    generator's baseline behavior (inline functions.py only). Consumers with
    extra common modules, standalone helper notebooks, or per-notebook ``%run``
    dependencies declare them here rather than relying on hardcoded names.
    """
    root = project_root if project_root is not None else PROJECT_ROOT
    config_path = root / "config" / "notebook_generation.json"

    config = dict(NOTEBOOK_GEN_DEFAULTS)
    if config_path.exists():
        with open(config_path) as f:
            config.update(json.load(f))
    return config

# Notebook naming prefixes
LAYER_PREFIXES = {
    "bronze": "10_bronze_",
    "silver": "20_silver_",
    "gold": "30_gold_",
    "backup": "",
}


@dataclass
class NotebookConfig:
    """Configuration for generating a notebook."""
    module_path: Path
    notebook_name: str
    layer: Literal["bronze", "silver", "gold", "backup", "common", "tests"]
    has_run_function: bool = True
    parameters: dict = None


def generate_metadata_block(lakehouse_config: dict = None) -> str:
    """Generate the notebook METADATA block."""
    meta = {
        "kernel_info": {
            "name": "synapse_pyspark"
        },
        "dependencies": {}
    }

    if lakehouse_config:
        meta["dependencies"]["lakehouse"] = lakehouse_config

    # Format dependencies as indented JSON within META comments
    deps_json = json.dumps(
        {"lakehouse": lakehouse_config} if lakehouse_config else {},
        indent=2
    ).replace("\n", "\n# META   ")

    return f"""# METADATA ********************

# META {{
# META   "kernel_info": {{
# META     "name": "synapse_pyspark"
# META   }},
# META   "dependencies": {deps_json}
# META }}"""


def generate_cell_metadata() -> str:
    """Generate metadata for a code cell."""
    return """# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }"""


def generate_path_setup_cell() -> str:
    """Generate the cell that sets up the Python path for src imports."""
    return """# CELL ********************

# Setup Python path for src imports
# This ensures src modules can be imported in both Fabric and local environments
import sys
from pathlib import Path

# In Fabric, notebooks run from the workspace root where src/ is located
# Locally, we need to ensure the project root is in the path
_notebook_dir = Path.cwd()
_project_root = _notebook_dir.parent if _notebook_dir.name.endswith('.Notebook') else _notebook_dir

if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
"""


def extract_imports_from_module(module_path: Path, skip_imports: list[str] = None) -> list[str]:
    """
    Extract import statements from a Python module.

    Args:
        module_path: Path to the Python module
        skip_imports: List of import patterns to skip (e.g., ['from src.common'])

    Returns:
        List of import statement strings
    """
    skip_imports = skip_imports or []

    with open(module_path) as f:
        lines = f.readlines()

    import_lines = []
    in_multiline_import = False
    in_docstring = False
    docstring_char = None
    current_import = []

    for line in lines:
        stripped = line.strip()

        # Handle docstrings
        if not in_docstring:
            if stripped.startswith('"""') or stripped.startswith("'''"):
                docstring_char = stripped[:3]
                # Check if it's a single-line docstring
                if stripped.count(docstring_char) >= 2:
                    continue  # Single line docstring, skip it
                in_docstring = True
                continue
        else:
            # We're inside a docstring, check if this line ends it
            if docstring_char in stripped:
                in_docstring = False
            continue

        # Skip comments
        if stripped.startswith("#"):
            continue

        # Check if this is an import line
        is_import = stripped.startswith("import ") or stripped.startswith("from ")

        if in_multiline_import:
            current_import.append(line.rstrip())
            # Check if this line ends the multiline import
            if stripped.endswith(")") or (not stripped.endswith("\\") and not stripped.endswith(",")):
                in_multiline_import = False
                # Check if we should skip this import
                full_import = "\n".join(current_import)
                should_skip = any(pattern in full_import for pattern in skip_imports)
                if not should_skip:
                    import_lines.append(full_import)
                current_import = []
        elif is_import:
            # Check if this is a multiline import
            if ("(" in stripped and ")" not in stripped) or stripped.endswith("\\"):
                in_multiline_import = True
                current_import = [line.rstrip()]
            else:
                # Single line import - check if we should skip it
                should_skip = any(pattern in line for pattern in skip_imports)
                if not should_skip:
                    import_lines.append(line.rstrip())
        elif stripped and not is_import:
            # Stop at first non-import, non-comment line (start of actual code)
            break

    return import_lines


def read_module_code(module_path: Path, skip_imports: list[str] = None, skip_top_level_only: bool = False) -> str:
    """
    Read Python module and extract code suitable for notebook inlining.

    Args:
        module_path: Path to the Python module
        skip_imports: List of import patterns to skip (e.g., ['from src.common'])
        skip_top_level_only: If True, only skip imports at indentation level 0 (preserve imports inside functions)

    Returns:
        Code string with imports filtered
    """
    with open(module_path) as f:
        lines = f.readlines()

    skip_imports = skip_imports or []
    result_lines = []
    in_docstring = False
    docstring_char = None
    in_multiline_import = False

    for line in lines:
        stripped = line.strip()

        # Track multiline imports (lines ending with \ or inside parentheses)
        if in_multiline_import:
            # Check if this line ends the multiline import
            if stripped.endswith(")") or (not stripped.endswith("\\") and not stripped.endswith(",")):
                in_multiline_import = False
            continue  # Skip all lines of the multiline import

        # Track docstrings
        if not in_docstring:
            if stripped.startswith('"""') or stripped.startswith("'''"):
                docstring_char = stripped[:3]
                if stripped.count(docstring_char) >= 2:
                    # Single line docstring
                    result_lines.append(line)
                    continue
                in_docstring = True
        else:
            if docstring_char in stripped:
                in_docstring = False
            result_lines.append(line)
            continue

        # Skip specified imports
        should_skip = False
        for skip_pattern in skip_imports:
            if stripped.startswith(skip_pattern):
                # If skip_top_level_only is True, only skip if this is a top-level import (no indentation)
                if skip_top_level_only:
                    # Check if line starts with the pattern (no leading whitespace)
                    if line.startswith(skip_pattern):
                        should_skip = True
                else:
                    should_skip = True

                if should_skip:
                    # Check if this is a multiline import
                    if "(" in stripped and ")" not in stripped:
                        in_multiline_import = True
                    elif stripped.endswith("\\"):
                        in_multiline_import = True
                    break

        if not should_skip:
            result_lines.append(line)

    return "".join(result_lines)


def generate_common_defs_notebook() -> str:
    """Generate the common_defs notebook content with inlined code.

    Inlines the framework defs module first, then defs.py (with the
    framework_defs cross-import stripped). The framework defs module is the
    shared subset; defs.py holds the consumer's project-specific definitions.
    """
    project_root = PROJECT_ROOT
    inline_framework = NOTEBOOK_GEN_CONFIG.get("inline_framework_modules", False)
    framework_defs_path = (
        framework_module_path("defs") if inline_framework
        else project_root / "src" / "common" / "framework_defs.py"
    )
    defs_path = project_root / "src" / "common" / "defs.py"

    content = ["# Fabric notebook source", ""]
    content.append(generate_metadata_block())
    content.append("")

    # Cell 1: Inlined code from the framework defs module + src/common/defs.py
    content.append("# CELL ********************")
    content.append("")

    if framework_defs_path.exists():
        content.append(read_module_code(framework_defs_path).rstrip())
        content.append("")

    if defs_path.exists():
        code = read_module_code(defs_path, skip_imports=[
            "from src.common",
            "import src.common",
            "from pyfabric_dev",
            "import pyfabric_dev",
        ])
        content.append(code.rstrip())
    else:
        content.append("# WARNING: src/common/defs.py not found")
        content.append("# Please create the module first")

    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    return "\n".join(content)


def generate_common_functions_notebook() -> str:
    """Generate the common_functions notebook content with inlined code."""
    project_root = PROJECT_ROOT
    inline_framework = NOTEBOOK_GEN_CONFIG.get("inline_framework_modules", False)
    # Primary functions/fabric modules: from the installed package when inlining
    # framework modules, otherwise the consumer's vendored copies in src/common.
    primary_path = (
        framework_module_path("functions") if inline_framework
        else project_root / "src" / "common" / "functions.py"
    )
    fabric_path = (
        framework_module_path("fabric") if inline_framework
        else project_root / "src" / "common" / "fabric.py"
    )
    # Extra domain modules (from src/common) inlined after the primary.
    extra_paths = [
        project_root / "src" / "common" / name
        for name in NOTEBOOK_GEN_CONFIG.get("common_functions_extra_modules", [])
    ]
    all_module_paths = [primary_path, *extra_paths]

    lakehouse_config = LAKEHOUSE_CONFIGS.get("bronze")  # Default to bronze

    content = ["# Fabric notebook source", ""]
    content.append(generate_metadata_block(lakehouse_config))
    content.append("")

    # Cell 1: Run common_defs
    content.append("# CELL ********************")
    content.append("")
    content.append("%run common_defs")
    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 2: Imports from source file
    content.append("# CELL ********************")
    content.append("")

    # Extract imports from the primary functions module plus any extra domain
    # modules, excluding src.common / pyfabric_dev imports (provided by %run
    # common_defs or the package). Deduplicate so shared symbols appear once.
    if primary_path.exists():
        imports: list[str] = []
        for module_path in all_module_paths:
            if not module_path.exists():
                continue
            for imp in extract_imports_from_module(module_path, skip_imports=[
                "from src.common",
                "import src.common",
                "from pyfabric_dev",
                "import pyfabric_dev",
            ]):
                if imp not in imports:
                    imports.append(imp)

        # Add Fabric-specific imports that aren't in the source file
        # (notebookutils is only available in Fabric, not in local dev)
        fabric_imports = ["from notebookutils import mssparkutils"]

        for imp in imports:
            content.append(imp)

        if imports:
            content.append("")

        for imp in fabric_imports:
            content.append(imp)
    else:
        content.append(f"# WARNING: src/common/{primary_path.name} not found")

    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 3: Fabric-specific utilities (from the framework fabric module)
    content.append("# CELL ********************")
    content.append("")

    if fabric_path.exists():
        # Extract functions from fabric.py, skipping all imports
        code = read_module_code(fabric_path, skip_imports=[
            "import ",
            "from ",
        ])
        content.append(code.rstrip())
    else:
        content.append("# WARNING: src/common/fabric.py not found")
        content.append("# Fabric-specific utilities should be defined here")

    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 4: Environment config loader (extracted from src/common/env.py)
    content.append("# CELL ********************")
    content.append("")

    env_path = project_root / "src" / "common" / "env.py"
    if env_path.exists():
        # Extract functions from env.py, skipping all imports
        code = read_module_code(env_path, skip_imports=[
            "import ",
            "from ",
        ])
        content.append(code.rstrip())
    else:
        content.append("# WARNING: src/common/env.py not found")
        content.append("# Environment config loader should be defined here")

    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 5: Inlined module code. functions.py is the framework-extractable
    # subset; any consumer-declared domain modules are appended after it. All
    # are inlined into the same notebook so callers see every symbol in a
    # single namespace after %run.
    content.append("# CELL ********************")
    content.append("")

    if primary_path.exists():
        # Skip all imports since they're already in Cell 2
        code = read_module_code(primary_path, skip_imports=[
            "import ",
            "from ",
        ])
        content.append(code.rstrip())
    else:
        content.append(f"# WARNING: src/common/{primary_path.name} not found")

    for module_path in extra_paths:
        if module_path.exists():
            content.append("")
            code = read_module_code(module_path, skip_imports=[
                "import ",
                "from ",
            ])
            content.append(code.rstrip())

    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    return "\n".join(content)


def generate_helper_notebook(source_module: str) -> str:
    """Generate a standalone helper notebook that inlines a single src/common module.

    The notebook %runs common_defs (for shared constants) and inlines the
    functions from ``src/common/<source_module>``. Consumers declare these via
    the ``helper_notebooks`` entry in config/notebook_generation.json; the
    framework no longer hardcodes any particular module name.
    """
    module_path = PROJECT_ROOT / "src" / "common" / source_module

    content = ["# Fabric notebook source", ""]
    content.append(generate_metadata_block())
    content.append("")

    # Cell 1: Run common_defs (provides shared constants the helper may need)
    content.append("# CELL ********************")
    content.append("")
    content.append("%run common_defs")
    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 2: Imports
    content.append("# CELL ********************")
    content.append("")
    if module_path.exists():
        imports = extract_imports_from_module(module_path, skip_imports=[
            "from src.common",
            "import src.common",
        ])
        for imp in imports:
            content.append(imp)
    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 3: Inlined code (functions only, top-level imports stripped)
    content.append("# CELL ********************")
    content.append("")
    if module_path.exists():
        code = read_module_code(module_path, skip_imports=[
            "import ",
            "from ",
        ], skip_top_level_only=True)
        content.append(code.rstrip())
    else:
        content.append(f"# WARNING: src/common/{source_module} not found")
    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    return "\n".join(content)


def generate_pipeline_notebook(config: NotebookConfig) -> str:
    """Generate a pipeline notebook (bronze/silver/gold) with inlined code."""
    lakehouse_config = LAKEHOUSE_CONFIGS.get(config.layer)

    content = ["# Fabric notebook source", ""]
    content.append(generate_metadata_block(lakehouse_config))
    content.append("")

    # Cell 1: Run common_functions
    content.append("# CELL ********************")
    content.append("")
    content.append("%run common_functions")
    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Inject %run dependencies declared for this notebook in
    # config/notebook_generation.json. Each entry maps a notebook-name
    # substring to the notebooks that must run first. An entry may be a plain
    # list of targets, or an object with "runs" plus "suppress_main": true to
    # load those deps without executing their run() entry points (the deps are
    # bracketed by RUN_MAIN = False / restore cells).
    for key, spec in NOTEBOOK_GEN_CONFIG.get("notebook_run_dependencies", {}).items():
        if key not in config.notebook_name:
            continue
        if isinstance(spec, list):
            spec = {"runs": spec}
        suppress_main = spec.get("suppress_main", False)

        if suppress_main:
            content.append("# CELL ********************")
            content.append("")
            content.append("_SAVED_RUN_MAIN = globals().get('RUN_MAIN', True)")
            content.append("RUN_MAIN = False")
            content.append("")
            content.append(generate_cell_metadata())
            content.append("")

        for run_target in spec.get("runs", []):
            content.append("# CELL ********************")
            content.append("")
            content.append(f"%run {run_target}")
            content.append("")
            content.append(generate_cell_metadata())
            content.append("")

        if suppress_main:
            content.append("# CELL ********************")
            content.append("")
            content.append("RUN_MAIN = _SAVED_RUN_MAIN")
            content.append("")
            content.append(generate_cell_metadata())
            content.append("")

    # Cell 2: Parameters (if any)
    if config.parameters:
        content.append("# PARAMETERS CELL ********************")
        content.append("")
        for key, value in config.parameters.items():
            # value is already a string representation from ast_unparse_default
            # Special handling: get_lakehouse_path_func should default to cf_get_lakehouse_path
            # (available from %run common_functions) instead of None
            if key == "get_lakehouse_path_func" and value == "None":
                content.append(f'{key} = cf_get_lakehouse_path')
            else:
                content.append(f'{key} = {value}')
        content.append("")
        content.append(generate_cell_metadata())
        content.append("")

    # Cell 3: Setup spark and logger
    content.append("# CELL ********************")
    content.append("")
    content.append("# Allow callers/tests to inject an existing Spark session / logger before executing this")
    content.append("# notebook (e.g., setting globals()['_spark'] or globals()['_logger']).")
    content.append('if "_logger" not in globals():')
    content.append(f'    _logger = cf_create_logger("{config.notebook_name}")')
    content.append("")
    content.append('if "_spark" not in globals():')
    content.append("    _spark = cf_create_spark_session()")
    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 4: Pipeline-specific imports (not provided by common_functions)
    content.append("# CELL ********************")
    content.append("")

    if config.module_path.exists():
        # Extract imports from pipeline module, excluding what common_functions provides
        pipeline_imports = extract_imports_from_module(config.module_path, skip_imports=[
            "from src.common",
            "import src.common",
        ])

        # Add only imports NOT already provided by common_functions
        # Common functions provides: logging, os, random, re, shutil, uuid, zipfile,
        # datetime, functools.reduce, Logger, DataFrame, SparkSession, F,
        # common pyspark.sql.functions, common pyspark.sql.types, DeltaTable
        for imp in pipeline_imports:
            # Skip imports entirely provided by common_functions
            skip_completely = [
                "from logging import Logger",
                "from functools import reduce",
                "from pyspark.sql import functions as F",
            ]

            # These modules are fully covered in common_functions
            if any(imp == skip or imp.startswith(skip) for skip in skip_completely):
                continue

            # For standard library imports, skip if already in common_functions
            stdlib_covered = ["import logging", "import os", "import random", "import re",
                            "import shutil", "import uuid", "import zipfile"]
            if any(imp == skip for skip in stdlib_covered):
                continue

            # For datetime, skip if it's the same as common_functions
            if imp == "from datetime import datetime, timedelta, timezone":
                continue

            content.append(imp)
    else:
        content.append("# No pipeline-specific imports")

    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 5: Inlined pipeline code
    content.append("# CELL ********************")
    content.append("")

    if config.module_path.exists():
        # Skip top-level imports (already in Cell 4), but keep imports inside functions
        code = read_module_code(config.module_path, skip_imports=[
            "import ",
            "from ",
        ], skip_top_level_only=True)
        content.append(code.rstrip())
    else:
        content.append(f"# WARNING: {config.module_path} not found")

    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 6: Run the pipeline
    content.append("# CELL ********************")
    content.append("")
    content.append("# Run the pipeline")
    content.append('if "RUN_MAIN" not in globals():')
    content.append("    RUN_MAIN = True")
    content.append("")
    content.append("if RUN_MAIN:")
    # Build the run() call with parameters
    run_args = ["_spark", "_logger"]
    if config.parameters:
        # Pass optional parameters as keyword arguments so notebook-level
        # variable renames or signature reorderings don't silently misbind.
        for param_name in config.parameters.keys():
            run_args.append(f"{param_name}={param_name}")
    run_call = f"    run({', '.join(run_args)})"
    content.append(run_call)
    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    return "\n".join(content)


def module_imports_fabric_harness(module_path: Path) -> bool:
    """
    True if the test module imports tests.fabric_test_tables.

    Generated Fabric notebooks cannot use ``from tests.*``; when this is True,
    ``generate_test_notebook`` inlines ``tests/fabric_test_tables.py`` so
    ``run_tests`` and helpers resolve in Synapse.
    """
    if not module_path.exists():
        return False
    try:
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
    except (SyntaxError, OSError):
        return False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "tests.fabric_test_tables":
            return True
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "tests.fabric_test_tables":
                    return True
    return False


def generate_test_notebook(config: NotebookConfig) -> str:
    """Generate a test notebook with inlined test code."""
    lakehouse_config = LAKEHOUSE_CONFIGS.get("tests")
    project_root = PROJECT_ROOT.resolve()
    fabric_harness_path = project_root / "tests" / "fabric_test_tables.py"

    content = ["# Fabric notebook source", ""]
    content.append(generate_metadata_block(lakehouse_config))
    content.append("")

    # Cell 1: Disable RUN_MAIN for the notebook being tested
    content.append("# CELL ********************")
    content.append("")
    content.append("RUN_MAIN = False")
    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 2: Run common_functions
    content.append("# CELL ********************")
    content.append("")
    content.append("%run common_functions")
    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 3: Run the production notebook (to get constants and functions)
    # Extract production notebook name from test notebook name (test_X -> X)
    prod_notebook_name = config.notebook_name
    if prod_notebook_name.startswith("test_"):
        prod_notebook_name = prod_notebook_name[5:]  # Remove "test_" prefix

    content.append("# CELL ********************")
    content.append("")
    content.append(f"%run {prod_notebook_name}")
    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 4: Setup spark and logger
    content.append("# CELL ********************")
    content.append("")
    content.append('if "_logger" not in globals():')
    content.append(f'    _logger = cf_create_logger("{config.notebook_name}")')
    content.append("")
    content.append('if "_spark" not in globals():')
    content.append("    _spark = cf_create_spark_session()")
    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 5: Test imports
    content.append("# CELL ********************")
    content.append("")

    # Extract imports from the test source file, excluding src module imports
    # (since those are provided by %run common_functions and %run production_notebook)
    if config.module_path.exists():
        test_imports = extract_imports_from_module(config.module_path, skip_imports=[
            "from src.",
            "import src.",
            "from tests.",
        ])

        for imp in test_imports:
            content.append(imp)
    else:
        content.append("# WARNING: Test imports could not be extracted")

    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 6 (optional): inline Fabric test harness when tests import tests.fabric_test_tables
    if (
        config.module_path.exists()
        and module_imports_fabric_harness(config.module_path)
        and fabric_harness_path.is_file()
    ):
        content.append("# CELL ********************")
        content.append("")
        content.append(
            "# Inlined from tests/fabric_test_tables.py (Fabric cannot import from tests.*)"
        )
        fabric_code = read_module_code(fabric_harness_path, skip_imports=[])
        content.append(fabric_code.rstrip())
        content.append("")
        content.append(generate_cell_metadata())
        content.append("")

    # Cell 7: Inlined test code
    content.append("# CELL ********************")
    content.append("")

    if config.module_path.exists():
        # Skip top-level imports (already in Cell 5), but keep imports inside functions
        code = read_module_code(config.module_path, skip_imports=[
            "import ",
            "from ",
        ], skip_top_level_only=True)
        content.append(code.rstrip())
    else:
        content.append(f"# WARNING: {config.module_path} not found")

    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    # Cell 8: Run the tests
    content.append("# CELL ********************")
    content.append("")
    content.append("# Run the tests")
    content.append("run_tests(_spark, _logger)")
    content.append("")
    content.append(generate_cell_metadata())
    content.append("")

    return "\n".join(content)


def extract_run_function_parameters(module_path: Path) -> dict:
    """
    Extract all parameters (except spark/logger) from the run() function signature.

    Excludes 'spark' and 'logger' parameters as they are always provided by the notebook.
    Required parameters (without defaults) get an empty string default for the parameters cell.

    Returns:
        Dictionary mapping parameter names to their default values (as strings for code generation)
    """
    try:
        with open(module_path) as f:
            tree = ast.parse(f.read())

        # Find the run() function
        run_func = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "run":
                run_func = node
                break

        if not run_func:
            return {}

        # Extract parameters
        # Parameters are in args.args, defaults are in args.defaults
        # Defaults align with the last N parameters
        params = {}
        args = run_func.args
        num_defaults = len(args.defaults)
        num_args = len(args.args)

        # Skip spark and logger (typically first two positional args)
        skip_params = {"spark", "logger"}

        for i, arg in enumerate(args.args):
            param_name = arg.arg
            if param_name in skip_params:
                continue

            # Check if this parameter has a default
            # Defaults align with the last N parameters
            default_index = i - (num_args - num_defaults)
            if default_index >= 0:
                default_value = args.defaults[default_index]
                # Convert AST default value to string representation
                params[param_name] = ast_unparse_default(default_value)
            else:
                # Required parameter without default - use empty string
                params[param_name] = '""'

        return params

    except (SyntaxError, FileNotFoundError) as e:
        print(f"Warning: Could not parse {module_path} for parameters: {e}")
        return {}


def ast_unparse_default(node: ast.AST) -> str:
    """
    Convert an AST node representing a default value to a string representation.

    Handles common cases: None, strings, numbers, booleans, etc.
    For complex expressions, uses ast.unparse if available (Python 3.9+).
    """
    # Try to use ast.unparse for complex expressions first (Python 3.9+)
    try:
        if hasattr(ast, 'unparse'):
            return ast.unparse(node)
    except Exception:
        pass

    # Handle simple constant values
    if isinstance(node, ast.Constant):
        value = node.value
        if value is None:
            return "None"
        elif isinstance(value, str):
            return f'"{value}"'
        elif isinstance(value, bool):
            return str(value)
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            return repr(value)
    elif isinstance(node, ast.NameConstant):  # Python < 3.8 compatibility
        if node.value is None:
            return "None"
        elif isinstance(node.value, bool):
            return str(node.value)
        else:
            return repr(node.value)
    elif isinstance(node, ast.Str):  # Python < 3.8 compatibility
        return f'"{node.s}"'
    elif isinstance(node, ast.Num):  # Python < 3.8 compatibility
        return str(node.n)
    elif isinstance(node, ast.Name) and node.id in ("None", "True", "False"):
        return node.id
    else:
        # Fallback: try repr or return None string
        try:
            return repr(node)
        except Exception:
            return "None"


def find_modules_with_run_function(src_dir: Path) -> list[NotebookConfig]:
    """Find all Python modules that have a run() function and extract their parameters."""
    configs = []

    for layer in ["bronze", "silver", "gold", "backup"]:
        layer_dir = src_dir / layer
        if not layer_dir.exists():
            continue

        for py_file in layer_dir.glob("*.py"):
            if py_file.name.startswith("_"):
                continue

            # Check if module has a run() function
            try:
                with open(py_file) as f:
                    tree = ast.parse(f.read())

                has_run = any(
                    isinstance(node, ast.FunctionDef) and node.name == "run"
                    for node in ast.walk(tree)
                )

                if has_run:
                    # Generate notebook name from module name
                    module_name = py_file.stem
                    prefix = LAYER_PREFIXES.get(layer, "")
                    notebook_name = f"{prefix}{module_name}"

                    # Extract optional parameters from run() function signature
                    parameters = extract_run_function_parameters(py_file)

                    configs.append(NotebookConfig(
                        module_path=py_file,
                        notebook_name=notebook_name,
                        layer=layer,
                        has_run_function=True,
                        parameters=parameters if parameters else None,
                    ))

            except SyntaxError as e:
                print(f"Warning: Could not parse {py_file}: {e}")

    return configs


def validate_no_src_imports(notebook_name: str, content: str, warn_only: bool = False):
    """Ensure no src.* imports leaked into generated notebook content.

    With ``warn_only`` (test notebooks), indented src.* imports only warn —
    they live inside test functions that are skipped in Fabric. Top-level
    (column-0) imports always raise: they execute when the notebook runs.
    """
    in_docstring = False
    warned = False
    for i, line in enumerate(content.split("\n"), 1):
        stripped = line.strip()
        # Track docstrings (triple-quoted strings)
        if not in_docstring:
            for quote in ('"""', "'''"):
                if quote in stripped:
                    # Check if docstring opens and closes on same line
                    if stripped.count(quote) == 1:
                        in_docstring = True
                        break
            if in_docstring:
                continue
        else:
            if '"""' in stripped or "'''" in stripped:
                in_docstring = False
            continue
        # Skip comments
        if stripped.startswith("#"):
            continue
        if stripped.startswith("from src.") or stripped.startswith("import src."):
            msg = (
                f"Notebook '{notebook_name}' line {i} contains src import: {stripped}\n"
                f"  src.* imports don't work in Fabric. Use try/except (ImportError, FileNotFoundError) "
                f"or move the functionality to common_functions."
            )
            is_top_level = not line[:1].isspace()
            if warn_only and not is_top_level:
                if not warned:
                    print(f"  WARNING: {msg}")
                    warned = True  # One warning per notebook is enough
            else:
                raise ValueError(msg)


def write_notebook(output_dir: Path, notebook_name: str, content: str, dry_run: bool = False):
    """Write a notebook to disk."""
    is_test = notebook_name.startswith("test_")
    validate_no_src_imports(notebook_name, content, warn_only=is_test)
    notebook_dir = output_dir / f"{notebook_name}.Notebook"

    if dry_run:
        print(f"  Would create: {notebook_dir}")
        return

    notebook_dir.mkdir(parents=True, exist_ok=True)
    notebook_file = notebook_dir / "notebook-content.py"

    with open(notebook_file, "w") as f:
        f.write(content)

    print(f"  Created: {notebook_dir}")


##############################################################################
# AST-based notebook validation
##############################################################################

import builtins as _builtins_module

# Names provided by the Fabric runtime (not defined in any notebook)
FABRIC_RUNTIME_NAMES = {
    "_spark", "_logger", "spark", "notebookutils", "mssparkutils",
    "display", "RUN_MAIN",
}

# Names that are provided at runtime for specific pipeline notebooks (e.g. by optional
# %run or by common modules not inlined into common_functions). Validation treats these as known.
PIPELINE_VALIDATION_ALLOWLIST: dict[str, set[str]] = {}

# Names from standard library / PySpark that are imported in common_functions
# These are available because common_functions has an imports cell.
COMMON_IMPORTS_NAMES = {
    # Standard library modules imported at top of common_functions
    "json", "logging", "os", "random", "re", "shutil", "uuid", "zipfile", "builtins",
    "datetime", "timedelta", "timezone", "reduce", "Logger",
    # PySpark
    "DataFrame", "SparkSession", "F", "col", "lit", "udf",
    "BooleanType", "LongType", "StringType", "StructField", "StructType",
    "DeltaTable",
    # Fabric-specific
    "mssparkutils",
}


def _extract_code_cells(content: str) -> list[tuple[int, str]]:
    """Extract Python code cells from notebook content.

    Returns list of (line_offset, cell_code) tuples.
    Skips metadata blocks and %run lines.
    """
    cells: list[tuple[int, str]] = []
    current_cell_lines: list[str] = []
    cell_start_line = 0
    in_cell = False

    for i, line in enumerate(content.split("\n"), 1):
        if line.startswith("# CELL **") or line.startswith("# PARAMETERS CELL **"):
            if in_cell and current_cell_lines:
                cells.append((cell_start_line, "\n".join(current_cell_lines)))
            current_cell_lines = []
            cell_start_line = i + 1  # code starts on the next line
            in_cell = True
            continue
        if line.startswith("# METADATA **"):
            if in_cell and current_cell_lines:
                cells.append((cell_start_line, "\n".join(current_cell_lines)))
            in_cell = False
            current_cell_lines = []
            continue
        if line.startswith("# META "):
            continue
        if in_cell:
            current_cell_lines.append(line)

    # Flush last cell
    if in_cell and current_cell_lines:
        cells.append((cell_start_line, "\n".join(current_cell_lines)))

    return cells


def _collect_defined_names_from_stmts(nodes: list[ast.AST], names: set[str]) -> None:
    """Collect names defined in a list of statements (e.g. try/except body)."""
    for node in nodes:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
        elif isinstance(node, ast.ClassDef):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
                elif isinstance(target, (ast.Tuple, ast.List)):
                    for elt in target.elts:
                        if isinstance(elt, ast.Name):
                            names.add(elt.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.asname if alias.asname else alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                names.add(alias.asname if alias.asname else alias.name)
        elif isinstance(node, ast.AugAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, (ast.For, ast.With)):
            _collect_targets(node, names)


def _collect_defined_names_from_code(code: str) -> set[str]:
    """Collect all names defined at module level in the given code."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return set()

    names: set[str] = set()
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
            names.add(node.name)
        elif isinstance(node, ast.ClassDef):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
                elif isinstance(target, ast.Tuple) or isinstance(target, ast.List):
                    for elt in target.elts:
                        if isinstance(elt, ast.Name):
                            names.add(elt.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.asname if alias.asname else alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                names.add(alias.asname if alias.asname else alias.name)
        elif isinstance(node, ast.AugAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, (ast.For, ast.With)):
            # Top-level for/with loop targets
            _collect_targets(node, names)
        elif isinstance(node, ast.Try):
            # Names defined in except blocks (e.g. Fabric fallback helpers) are visible module-wide
            for handler in node.handlers:
                _collect_defined_names_from_stmts(handler.body, names)
    return names


def _collect_targets(node: ast.AST, names: set[str]):
    """Collect assignment targets from for/with statements."""
    if isinstance(node, ast.For):
        if isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node.target, (ast.Tuple, ast.List)):
            for elt in node.target.elts:
                if isinstance(elt, ast.Name):
                    names.add(elt.id)
    elif isinstance(node, ast.With):
        for item in node.items:
            if item.optional_vars and isinstance(item.optional_vars, ast.Name):
                names.add(item.optional_vars.id)


def _collect_notebook_exports(content: str) -> set[str]:
    """Collect all names exported by a notebook (from all its code cells)."""
    names: set[str] = set()
    for _offset, cell_code in _extract_code_cells(content):
        # Skip %run lines
        code_lines = [l for l in cell_code.split("\n") if not l.strip().startswith("%run")]
        code = "\n".join(code_lines)
        names |= _collect_defined_names_from_code(code)
    return names


def _collect_local_names(func_node: ast.FunctionDef) -> set[str]:
    """Collect all locally defined names within a function body."""
    local_names: set[str] = set()

    # Parameters
    for arg in func_node.args.args:
        local_names.add(arg.arg)
    for arg in func_node.args.kwonlyargs:
        local_names.add(arg.arg)
    if func_node.args.vararg:
        local_names.add(func_node.args.vararg.arg)
    if func_node.args.kwarg:
        local_names.add(func_node.args.kwarg.arg)

    # Walk the function body for all Store targets, imports, nested defs
    for node in ast.walk(func_node):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            local_names.add(node.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node is not func_node:  # Skip the function itself
                local_names.add(node.name)
                # Also collect nested function parameters
                for arg in node.args.args:
                    local_names.add(arg.arg)
                for arg in node.args.kwonlyargs:
                    local_names.add(arg.arg)
                if node.args.vararg:
                    local_names.add(node.args.vararg.arg)
                if node.args.kwarg:
                    local_names.add(node.args.kwarg.arg)
        elif isinstance(node, ast.Lambda):
            # Collect lambda parameter names
            for arg in node.args.args:
                local_names.add(arg.arg)
            for arg in node.args.kwonlyargs:
                local_names.add(arg.arg)
            if node.args.vararg:
                local_names.add(node.args.vararg.arg)
            if node.args.kwarg:
                local_names.add(node.args.kwarg.arg)
        elif isinstance(node, ast.ClassDef):
            local_names.add(node.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                local_names.add(alias.asname if alias.asname else alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                local_names.add(alias.asname if alias.asname else alias.name)
        elif isinstance(node, ast.ExceptHandler):
            if node.name:
                local_names.add(node.name)
        elif isinstance(node, ast.comprehension):
            if isinstance(node.target, ast.Name):
                local_names.add(node.target.id)
            elif isinstance(node.target, (ast.Tuple, ast.List)):
                for elt in node.target.elts:
                    if isinstance(elt, ast.Name):
                        local_names.add(elt.id)

    return local_names


@dataclass
class UnresolvedName:
    """An unresolved name reference found during validation."""
    name: str
    line: int  # Line number in the notebook file
    func_name: str | None  # Function where it was found, or None for module level


def _find_undefined_names(code: str, line_offset: int, known_names: set[str]) -> list[UnresolvedName]:
    """Find names in code that are used but not defined."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return []

    python_builtins = set(dir(_builtins_module))
    all_known = known_names | python_builtins | FABRIC_RUNTIME_NAMES

    # Collect module-level defined names
    module_names = _collect_defined_names_from_code(code)
    all_known = all_known | module_names

    issues: list[UnresolvedName] = []

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            local_names = _collect_local_names(node)
            func_scope = all_known | local_names

            # Walk function body looking for Name in Load context
            for child in ast.walk(node):
                if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load):
                    name = child.id
                    if name.startswith("_"):
                        continue
                    if name in func_scope:
                        continue
                    issues.append(UnresolvedName(
                        name=name,
                        line=line_offset + child.lineno - 1,
                        func_name=node.name,
                    ))

        # Also check module-level Load references (outside functions)
        elif isinstance(node, ast.Expr):
            for child in ast.walk(node):
                if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load):
                    if child.id.startswith("_") or child.id in all_known:
                        continue
                    issues.append(UnresolvedName(
                        name=child.id,
                        line=line_offset + child.lineno - 1,
                        func_name=None,
                    ))
        elif isinstance(node, ast.If):
            # Module-level if statements (like RUN_MAIN guard)
            _check_node_for_undefined(node, line_offset, all_known, issues)

    return issues


def _check_node_for_undefined(
    node: ast.AST, line_offset: int, known: set[str], issues: list[UnresolvedName]
):
    """Check an AST node tree for undefined name references."""
    # First collect all Store targets within this block
    block_locals: set[str] = set()
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Store):
            block_locals.add(child.id)
        elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            block_locals.add(child.name)
        elif isinstance(child, ast.Import):
            for alias in child.names:
                block_locals.add(alias.asname if alias.asname else alias.name.split(".")[0])
        elif isinstance(child, ast.ImportFrom):
            for alias in child.names:
                block_locals.add(alias.asname if alias.asname else alias.name)
        elif isinstance(child, ast.comprehension):
            if isinstance(child.target, ast.Name):
                block_locals.add(child.target.id)

    effective_known = known | block_locals

    for child in ast.walk(node):
        if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load):
            if child.id.startswith("_") or child.id in effective_known:
                continue
            issues.append(UnresolvedName(
                name=child.id,
                line=line_offset + child.lineno - 1,
                func_name=None,
            ))


def validate_all_notebooks(generated: dict[str, str]) -> int:
    """
    Validate all generated notebooks for undefined name references.

    Args:
        generated: Dict mapping notebook_name -> content

    Returns:
        Number of errors found in production notebooks
    """
    print("\n" + "=" * 50)
    print("Validating notebooks for undefined names...")
    print("=" * 50)

    # Step 1: Collect exports from common_defs
    common_defs_exports: set[str] = set()
    if "common_defs" in generated:
        common_defs_exports = _collect_notebook_exports(generated["common_defs"])

    # Step 2: Collect exports from common_functions
    common_functions_exports: set[str] = set()
    if "common_functions" in generated:
        common_functions_exports = _collect_notebook_exports(generated["common_functions"])

    # Base known names for pipeline notebooks
    base_known = common_defs_exports | common_functions_exports | COMMON_IMPORTS_NAMES

    # Step 3: Validate common_functions itself
    errors_count = 0
    if "common_functions" in generated:
        count = _validate_single_notebook(
            "common_functions", generated["common_functions"],
            common_defs_exports | COMMON_IMPORTS_NAMES,
            is_test=False,
        )
        errors_count += count

    # Step 4: Validate pipeline notebooks
    # Collect exports from each pipeline notebook for cross-referencing
    pipeline_exports: dict[str, set[str]] = {}
    for name, content in generated.items():
        if name in ("common_defs", "common_functions") or name.startswith("test_"):
            continue
        pipeline_exports[name] = _collect_notebook_exports(content)

    for name, content in generated.items():
        if name in ("common_defs", "common_functions") or name.startswith("test_"):
            continue

        known = set(base_known)
        # Add per-notebook allowlist (names provided at runtime, e.g. optional %run)
        known |= PIPELINE_VALIDATION_ALLOWLIST.get(name, set())
        # Add exports from %run dependencies
        for line in content.split("\n"):
            stripped = line.strip()
            if stripped.startswith("%run ") and stripped != "%run common_functions" and stripped != "%run common_defs":
                dep_name = stripped[5:].strip()
                if dep_name in pipeline_exports:
                    known |= pipeline_exports[dep_name]

        count = _validate_single_notebook(name, content, known, is_test=False)
        errors_count += count

    # Step 5: Validate test notebooks
    # Names used in the generated "run tests" cell that are always expected
    test_runner_names = {"unittest", "sys", "run_tests"}
    # Names provided by tests.conftest when run with pytest; in Fabric they may be
    # defined in try/except ImportError fallback (validator collects those from Try handlers)
    test_conftest_names = {"clean_default_db_table", "force_create_default_db_table"}

    for name, content in generated.items():
        if not name.startswith("test_"):
            continue

        known = set(base_known) | test_runner_names | test_conftest_names
        # Add exports from %run dependencies (production notebook + any others)
        for line in content.split("\n"):
            stripped = line.strip()
            if stripped.startswith("%run ") and stripped != "%run common_functions" and stripped != "%run common_defs":
                dep_name = stripped[5:].strip()
                if dep_name in pipeline_exports:
                    known |= pipeline_exports[dep_name]

        count = _validate_single_notebook(name, content, known, is_test=True)
        # Test notebook issues are warnings, don't count as errors

    return errors_count


def _validate_single_notebook(
    notebook_name: str, content: str, known_names: set[str], is_test: bool
) -> int:
    """Validate a single notebook. Returns number of issues found."""
    all_issues: list[UnresolvedName] = []

    # First, collect all names defined across ALL cells in this notebook
    # so that cross-cell references resolve correctly
    notebook_own_exports = _collect_notebook_exports(content)
    effective_known = known_names | notebook_own_exports

    for offset, cell_code in _extract_code_cells(content):
        # Skip %run lines from the code
        code_lines = [l for l in cell_code.split("\n") if not l.strip().startswith("%run")]
        code = "\n".join(code_lines)
        if not code.strip():
            continue

        issues = _find_undefined_names(code, offset, effective_known)
        all_issues.extend(issues)

    # Deduplicate by (name, line)
    seen: set[tuple[str, int]] = set()
    unique_issues: list[UnresolvedName] = []
    for issue in all_issues:
        key = (issue.name, issue.line)
        if key not in seen:
            seen.add(key)
            unique_issues.append(issue)

    if unique_issues:
        prefix = "WARNING" if is_test else "ERROR"
        print(f"\n  {prefix}: {notebook_name} has {len(unique_issues)} undefined name(s):")
        for issue in unique_issues:
            loc = f"line {issue.line}"
            if issue.func_name:
                loc += f" in {issue.func_name}()"
            print(f"    - '{issue.name}' at {loc}")

    return 0 if is_test else len(unique_issues)


def main():
    parser = argparse.ArgumentParser(description="Generate Fabric notebooks from Python modules")
    parser.add_argument("--project-root", type=Path, default=Path.cwd(),
                        help="Consumer project root containing src/, config/, etc. (default: CWD)")
    parser.add_argument("--only", choices=["bronze", "silver", "gold", "backup", "common", "tests"],
                        help="Generate only notebooks for specified layer")
    parser.add_argument("--all", action="store_true",
                        help="Regenerate all notebooks including common")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be generated without writing files")
    parser.add_argument("--output", type=Path, default=None,
                        help="Output directory (default: project root)")
    parser.add_argument("--strict-validate", action="store_true",
                        help="Exit with non-zero status if validation finds issues in production notebooks")
    args = parser.parse_args()

    global PROJECT_ROOT, LAKEHOUSE_CONFIGS, NOTEBOOK_GEN_CONFIG
    PROJECT_ROOT = args.project_root.resolve()
    LAKEHOUSE_CONFIGS = load_lakehouse_config(PROJECT_ROOT)
    NOTEBOOK_GEN_CONFIG = load_notebook_generation_config(PROJECT_ROOT)

    project_root = PROJECT_ROOT
    src_dir = project_root / "src"
    output_dir = args.output or project_root

    print("Fabric Notebook Generator")
    print("=" * 50)

    # Track all generated notebook contents for validation
    generated_notebooks: dict[str, str] = {}

    # Generate common notebooks
    if args.only in [None, "common"] or args.all:
        print("\nGenerating common notebooks...")

        # common_defs
        content = generate_common_defs_notebook()
        write_notebook(output_dir / "common", "common_defs", content, args.dry_run)
        generated_notebooks["common_defs"] = content

        # common_functions
        content = generate_common_functions_notebook()
        write_notebook(output_dir / "common", "common_functions", content, args.dry_run)
        generated_notebooks["common_functions"] = content

    # Generate consumer-declared helper notebooks: standalone notebooks that
    # each inline a single src/common module. Declared in
    # config/notebook_generation.json (empty by default), generated only when
    # the source module is present.
    for spec in NOTEBOOK_GEN_CONFIG.get("helper_notebooks", []):
        layer = spec.get("layer", "bronze")
        if args.only not in [None, layer] and not args.all:
            continue
        source = spec["source"]
        if (project_root / "src" / "common" / source).exists():
            content = generate_helper_notebook(source)
            write_notebook(output_dir / layer, spec["name"], content, args.dry_run)
            generated_notebooks[spec["name"]] = content

    # Generate pipeline notebooks
    if args.only in [None, "bronze", "silver", "gold", "backup"] or args.all:
        configs = find_modules_with_run_function(src_dir)

        if args.only:
            configs = [c for c in configs if c.layer == args.only]

        if configs:
            print(f"\nGenerating {len(configs)} pipeline notebooks...")
            for config in configs:
                content = generate_pipeline_notebook(config)
                # Write production notebooks under layer dir (bronze/, silver/, gold/)
                layer_output_dir = output_dir / config.layer
                write_notebook(layer_output_dir, config.notebook_name, content, args.dry_run)
                generated_notebooks[config.notebook_name] = content
        else:
            print("\nNo pipeline modules found with run() functions.")
            print("Create modules in src/bronze/, src/silver/, or src/gold/ with a run() function.")

    # Generate test notebooks for migrated modules
    if args.only in [None, "tests"] or args.all:
        tests_dir = project_root / "tests"
        if tests_dir.exists():
            # Find test files that correspond to migrated modules
            migrated_configs = find_modules_with_run_function(src_dir)
            test_notebooks_generated = 0

            for config in migrated_configs:
                # Map module name to test file
                # e.g., src/silver/transform_accounts.py -> tests/test_silver_transform_accounts.py
                module_name = config.module_path.stem  # transform_accounts
                layer = config.layer  # silver
                test_file = tests_dir / f"test_{layer}_{module_name}.py"

                # Also try without layer prefix
                if not test_file.exists():
                    test_file = tests_dir / f"test_{module_name}.py"

                if test_file.exists():
                    # Generate test notebook with the notebook naming convention
                    # e.g., test_20_silver_transform_accounts
                    test_notebook_name = f"test_{config.notebook_name}"

                    test_config = NotebookConfig(
                        module_path=test_file,
                        notebook_name=test_notebook_name,
                        layer="tests",
                        has_run_function=False,
                    )
                    content = generate_test_notebook(test_config)
                    # Write test notebooks under layer/tests/ (e.g. bronze/tests/, silver/tests/, gold/tests/)
                    test_output_dir = output_dir / config.layer / "tests"
                    write_notebook(test_output_dir, test_config.notebook_name, content, args.dry_run)
                    generated_notebooks[test_config.notebook_name] = content
                    test_notebooks_generated += 1

            if test_notebooks_generated > 0:
                print(f"\nGenerated {test_notebooks_generated} test notebooks for migrated modules")

    # Validate all generated notebooks
    if not args.dry_run and generated_notebooks:
        error_count = validate_all_notebooks(generated_notebooks)
        if error_count > 0:
            print(f"\nValidation: {error_count} undefined name(s) found in production notebooks.")
            if args.strict_validate:
                print("Fix the source modules and regenerate (--strict-validate is on).")
                sys.exit(1)
        else:
            print("\nValidation passed: no undefined names found.")

    print("\nDone!")


if __name__ == "__main__":
    main()
