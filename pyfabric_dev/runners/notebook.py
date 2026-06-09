"""Run Fabric PySpark notebooks locally.

Parses ``notebook-content.py`` artifacts, handles ``%run`` commands by
recursively executing dependencies, and runs each cell in a shared
namespace so the resulting notebook state mirrors what Fabric would
produce. Project-specific behavior (lakehouse path overrides, helper
function substitutions, per-notebook globals injection) is supplied
through :class:`RunnerHooks`.
"""
from __future__ import annotations

import json
import re
import sys
import traceback
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pyfabric_dev.runners.hooks import RunnerHooks


# Subdirectories searched by name when resolving a ``%run`` target.
def _default_search_dirs(medallion_layers: Iterable[str]) -> List[str]:
    layers = list(medallion_layers)
    return [
        *layers,
        *(f"{layer}/tests" for layer in layers),
        "common",
        "common/tests",
        "tests",
    ]


class NotebookRunner:
    """Parses and executes Fabric notebook files locally."""

    def __init__(
        self,
        notebook_path: Path,
        project_root: Path,
        *,
        hooks: Optional[RunnerHooks] = None,
        medallion_layers: Optional[Iterable[str]] = None,
        lakehouse_config_path: Optional[Path] = None,
    ):
        self.notebook_path = notebook_path
        self.project_root = project_root
        self.hooks = hooks or RunnerHooks()
        if medallion_layers is None:
            from pyfabric_dev.local_config import MEDALLION_LAYERS as _layers
            medallion_layers = _layers
        self.medallion_layers = tuple(medallion_layers)
        self.lakehouse_config_path = (
            lakehouse_config_path
            if lakehouse_config_path is not None
            else project_root / "config" / "lakehouse_config.json"
        )
        self.executed_notebooks: set[str] = set()
        self.globals_dict: Dict[str, object] = dict(self.hooks.initial_globals)

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def parse_notebook(self, notebook_path: Path) -> List[str]:
        """Extract Python cells, ignoring METADATA blocks."""
        if not notebook_path.exists():
            raise FileNotFoundError(f"Notebook not found: {notebook_path}")

        content = notebook_path.read_text(encoding="utf-8")
        cells: List[str] = []
        lines = content.split("\n")
        in_metadata = False
        current_cell: List[str] = []

        for line in lines:
            if line.strip() == "# METADATA ********************":
                in_metadata = True
                if current_cell:
                    cell_content = "\n".join(current_cell).strip()
                    if cell_content:
                        cells.append(cell_content)
                    current_cell = []
                continue

            if in_metadata:
                if line.strip().startswith("# META {") or line.strip().startswith("# META}"):
                    continue
                if line.strip() == "" or (
                    not line.strip().startswith("#") and not line.strip().startswith("META")
                ):
                    in_metadata = False
                else:
                    continue

            if line.strip() in ("# CELL ********************", "# PARAMETERS CELL ********************"):
                if current_cell:
                    cell_content = "\n".join(current_cell).strip()
                    if cell_content:
                        cells.append(cell_content)
                    current_cell = []
                continue

            if not in_metadata:
                current_cell.append(line)

        if current_cell:
            cell_content = "\n".join(current_cell).strip()
            if cell_content:
                cells.append(cell_content)

        return cells

    def extract_default_lakehouse(self, notebook_path: Path) -> Optional[str]:
        """Extract default_lakehouse_name from notebook METADATA block."""
        content = notebook_path.read_text(encoding="utf-8")
        meta_lines: List[str] = []
        in_meta = False
        for line in content.split("\n"):
            stripped = line.strip()
            if stripped == "# METADATA ********************":
                in_meta = True
                meta_lines = []
                continue
            if in_meta:
                if stripped.startswith("# META"):
                    json_part = stripped[7:] if len(stripped) > 7 else ""
                    meta_lines.append(json_part)
                elif stripped == "":
                    continue
                else:
                    if meta_lines:
                        try:
                            meta = json.loads("\n".join(meta_lines))
                            name = meta.get("dependencies", {}).get("lakehouse", {}).get(
                                "default_lakehouse_name"
                            )
                            if name:
                                return name
                        except (json.JSONDecodeError, AttributeError):
                            pass
                    in_meta = False
                    meta_lines = []

        if meta_lines:
            try:
                meta = json.loads("\n".join(meta_lines))
                return meta.get("dependencies", {}).get("lakehouse", {}).get(
                    "default_lakehouse_name"
                )
            except (json.JSONDecodeError, AttributeError):
                pass
        return None

    # ------------------------------------------------------------------
    # Lakehouse registration
    # ------------------------------------------------------------------

    def _build_lakehouse_id_map(self) -> Dict[str, str]:
        if not self.lakehouse_config_path.exists():
            return {}
        try:
            config = json.loads(self.lakehouse_config_path.read_text())
        except (json.JSONDecodeError, ValueError) as e:
            print(f"  ⚠️  Malformed lakehouse config: {e}")
            return {}
        id_map: Dict[str, str] = {}
        for layer_cfg in config.values():
            lh_id = layer_cfg.get("default_lakehouse")
            lh_name = layer_cfg.get("default_lakehouse_name")
            if lh_id and lh_name:
                id_map[lh_id] = lh_name
        return id_map

    def _auto_register_lakehouse(self, spark, db_name: str) -> None:
        """Register Delta tables from the local data dir under ``db_name``."""
        from pyfabric_dev.local_config import DEV_BASE_DIR, LOCAL_TABLES_PATH

        lakehouse_tables_dir = DEV_BASE_DIR / db_name / "Tables"
        lakehouse_tables_dir.mkdir(parents=True, exist_ok=True)

        db_location = str(lakehouse_tables_dir.resolve())
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db_name}` LOCATION '{db_location}'")

        scan_dirs = [lakehouse_tables_dir]
        if LOCAL_TABLES_PATH.exists():
            scan_dirs.append(LOCAL_TABLES_PATH)

        registered = 0
        seen_tables: set = set()
        for tables_dir in scan_dirs:
            if not tables_dir.exists():
                continue
            is_legacy = tables_dir != lakehouse_tables_dir
            table_dirs = sorted(
                d for d in tables_dir.iterdir()
                if d.is_dir()
                and not d.name.startswith(".")
                and not d.name.endswith(".db")
                and (d / "_delta_log").exists()
            )
            for table_dir in table_dirs:
                table_name = table_dir.name
                if is_legacy and table_name in seen_tables:
                    continue
                seen_tables.add(table_name)
                try:
                    spark.sql(f"DESCRIBE TABLE `{db_name}`.`{table_name}`")
                    location_row = spark.sql(
                        f"DESCRIBE DETAIL `{db_name}`.`{table_name}`"
                    ).select("location").first()
                    if location_row and location_row[0] == str(table_dir.resolve()):
                        continue
                    spark.sql(f"DROP TABLE IF EXISTS `{db_name}`.`{table_name}`")
                except Exception:
                    pass
                location = str(table_dir.resolve())
                try:
                    spark.sql(
                        f"CREATE TABLE `{db_name}`.`{table_name}` "
                        f"USING DELTA LOCATION '{location}'"
                    )
                    registered += 1
                except Exception as e:
                    print(f"  ⚠️  Could not register {db_name}.{table_name}: {e}")

        print(f"  🗄️  Auto-registered {registered} tables in '{db_name}'")

    # ------------------------------------------------------------------
    # Path resolution
    # ------------------------------------------------------------------

    def resolve_notebook_path(self, notebook_name: str) -> Path:
        """Resolve a ``%run`` notebook name to an actual file path."""
        clean_name = notebook_name.replace(".Notebook", "")

        possible_paths = [
            self.project_root / f"{clean_name}.Notebook" / "notebook-content.py",
            self.project_root / f"{notebook_name}.Notebook" / "notebook-content.py",
            self.project_root / clean_name / "notebook-content.py",
            self.project_root / notebook_name / "notebook-content.py",
        ]

        for subdir in _default_search_dirs(self.medallion_layers):
            possible_paths.append(
                self.project_root / subdir / f"{clean_name}.Notebook" / "notebook-content.py"
            )

        for path in possible_paths:
            if path.exists():
                return path

        raise FileNotFoundError(
            f"Could not find notebook '{notebook_name}'. Tried: "
            f"{[str(p) for p in possible_paths]}"
        )

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute_notebook(self, notebook_path: Path, is_dependency: bool = False) -> None:
        notebook_key = str(notebook_path.resolve())

        if notebook_key in self.executed_notebooks:
            if not is_dependency:
                print(f"⚠️  Skipping already executed notebook: {notebook_path.name}")
            return

        self.executed_notebooks.add(notebook_key)

        if not is_dependency:
            print(f"\n📓 Executing notebook: {notebook_path.name}")
            print("=" * 70)

        if self.hooks.notebook_globals is not None:
            try:
                extra = self.hooks.notebook_globals(notebook_path) or {}
                self.globals_dict.update(extra)
            except Exception as e:
                print(f"  ⚠️  notebook_globals hook raised: {e}")

        try:
            cells = self.parse_notebook(notebook_path)

            if not is_dependency:
                default_lakehouse = self.extract_default_lakehouse(notebook_path)
                if default_lakehouse:
                    self._pending_default_db = default_lakehouse

            is_common_functions = (
                self.hooks.common_functions_name in notebook_path.name
                or self.hooks.common_functions_name in str(notebook_path)
            )

            # Ensure a Delta-enabled Spark session is available before any cell
            # runs. Hook-less CLI runs inject no `_spark`, and a notebook's own
            # ``getOrCreate()`` would otherwise yield a session without Delta
            # wired in. Consumers that supply ``_spark`` via hooks are untouched.
            if "_spark" not in self.globals_dict:
                from pyfabric_dev.spark import create_spark_session
                self.globals_dict["_spark"] = create_spark_session()

            for i, cell in enumerate(cells, 1):
                pending_db = getattr(self, "_pending_default_db", None)
                if pending_db and "_spark" in self.globals_dict:
                    spark = self.globals_dict["_spark"]
                    try:
                        spark.sql(f"USE `{pending_db}`")
                    except Exception:
                        self._auto_register_lakehouse(spark, pending_db)
                        try:
                            spark.sql(f"USE `{pending_db}`")
                            print(f"  🗄️  Default database: {pending_db}")
                        except Exception as e:
                            print(f"  ⚠️  Could not switch to database '{pending_db}': {e}")
                    else:
                        print(f"  🗄️  Default database: {pending_db}")
                    self._pending_default_db = None

                    for lh_name in self._build_lakehouse_id_map().values():
                        if lh_name != pending_db:
                            self._auto_register_lakehouse(spark, lh_name)
                            try:
                                spark.sql(f"USE `{pending_db}`")
                            except Exception as e:
                                print(f"  ⚠️  Could not restore database '{pending_db}': {e}")

                if cell.strip().startswith("%run"):
                    match = re.match(r"%run\s+(\S+)", cell.strip())
                    if match:
                        notebook_name = match.group(1)
                        print(f"  📦 Resolving dependency: {notebook_name}")

                        if notebook_name == self.hooks.common_functions_name:
                            common_defs_path = self.resolve_notebook_path("common_defs")
                            if str(common_defs_path.resolve()) not in self.executed_notebooks:
                                self.execute_notebook(common_defs_path, is_dependency=True)

                            dep_path = self.resolve_notebook_path(notebook_name)
                            self.execute_notebook(dep_path, is_dependency=True)

                            if self.hooks.common_functions_overrides:
                                print("     → Overriding key functions with local versions")
                                for func_name, func in self.hooks.common_functions_overrides.items():
                                    self.globals_dict[func_name] = func
                                    print(f"       ✓ {func_name}")
                            continue

                        dep_path = self.resolve_notebook_path(notebook_name)
                        self.execute_notebook(dep_path, is_dependency=True)
                        continue
                    else:
                        print(f"  ⚠️  Warning: Could not parse %run command: {cell.strip()}")
                        continue

                try:
                    code = compile(cell, f"{notebook_path.name}:cell_{i}", "exec")
                    exec(code, self.globals_dict)

                    # If we just finished executing the common_functions notebook
                    # itself (not a %run of it), re-apply overrides.
                    if (
                        is_common_functions
                        and self.hooks.common_functions_overrides
                        and i == len(cells)
                    ):
                        for func_name, func in self.hooks.common_functions_overrides.items():
                            self.globals_dict[func_name] = func
                            print(f"     ✓ Overrode {func_name} with local version")

                    if not is_dependency and cell.strip():
                        first_line = cell.strip().split("\n")[0][:60]
                        if not first_line.startswith("#") and not first_line.startswith("%run"):
                            print(f"  ✅ Cell {i}: {first_line}...")

                except SyntaxError as e:
                    print(f"  ❌ Syntax error in cell {i}:")
                    print(f"     {e}")
                    print(f"     Line {e.lineno}: {e.text}")
                    raise
                except Exception as e:
                    print(f"  ❌ Error in cell {i}:")
                    print(f"     {type(e).__name__}: {e}")
                    if not is_dependency:
                        traceback.print_exc()
                    raise

        except Exception as e:
            print(f"\n❌ Failed to execute notebook: {notebook_path.name}")
            print(f"   Error: {type(e).__name__}: {e}")
            if not is_dependency:
                traceback.print_exc()
            raise

    def run(self) -> None:
        print("🚀 Fabric Notebook Local Runner")
        print("=" * 70)
        print(f"📁 Project root: {self.project_root}")
        print(f"📄 Notebook: {self.notebook_path}")
        print()

        try:
            self.execute_notebook(self.notebook_path, is_dependency=False)
            print("\n" + "=" * 70)
            print("✅ Notebook execution completed successfully!")
        except Exception as e:
            print("\n" + "=" * 70)
            print(f"❌ Notebook execution failed: {e}")
            sys.exit(1)
