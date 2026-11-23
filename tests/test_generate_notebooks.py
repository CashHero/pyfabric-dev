"""Tests for the notebook generator's src-import stripping and validation."""

import ast
import subprocess
import sys

import pytest

import pyfabric_dev.cli.generate_notebooks as gen
from pyfabric_dev.cli.generate_notebooks import (
    NotebookConfig,
    extract_imports_from_module,
    generate_test_notebook,
    read_module_code,
    validate_no_src_imports,
)

TEST_NOTEBOOK_SKIP_IMPORTS = [
    "from src.",
    "import src.",
    "from tests.",
]


def test_extract_imports_strips_src_from_any_layer(tmp_path):
    """src.* imports must be stripped regardless of layer — including ones
    added after the generator was written (e.g. src.backup)."""
    module = tmp_path / "test_example.py"
    module.write_text(
        "from datetime import date\n"
        "import pytest\n"
        "from src.common.defs import SOME_CONSTANT\n"
        "from src.bronze.ingest import run\n"
        "from src.backup.backup_lakehouse_files import (\n"
        "    backup_lakehouse_files,\n"
        "    prune_old_snapshots,\n"
        ")\n"
        "import src.gold.build_full_ledger\n"
        "from tests.fixtures import helper\n"
    )

    imports = extract_imports_from_module(module, skip_imports=TEST_NOTEBOOK_SKIP_IMPORTS)

    assert "from datetime import date" in imports
    assert "import pytest" in imports
    assert not any("src." in imp for imp in imports)
    assert not any("tests." in imp for imp in imports)


def test_validate_rejects_top_level_src_import_even_in_test_notebooks():
    content = (
        "import pytest\n"
        "from src.backup.backup_lakehouse_files import run\n"
    )
    with pytest.raises(ValueError, match="src import"):
        validate_no_src_imports("test_backup", content, warn_only=True)


def test_validate_warns_on_indented_src_import_in_test_notebooks(capsys):
    """Indented src.* imports live in test functions skipped in Fabric —
    they warn but don't fail generation."""
    content = (
        "def test_local_only():\n"
        "    from src.bronze.ingest_from_priority import run\n"
    )
    validate_no_src_imports("test_bronze", content, warn_only=True)
    assert "WARNING" in capsys.readouterr().out


def test_validate_rejects_any_src_import_in_production_notebooks():
    content = (
        "def helper():\n"
        "    from src.bronze.ingest import run\n"
    )
    with pytest.raises(ValueError, match="src import"):
        validate_no_src_imports("10_bronze_ingest", content, warn_only=False)


def test_validate_rejects_multiline_top_level_src_import_in_test_notebooks():
    """The opening line of a multiline top-level src import is at column 0,
    so it must raise even in test notebooks."""
    content = (
        "import pytest\n"
        "from src.backup.backup_lakehouse_files import (\n"
        "    backup_lakehouse_files,\n"
        "    prune_old_snapshots,\n"
        ")\n"
    )
    with pytest.raises(ValueError, match="src import"):
        validate_no_src_imports("test_backup", content, warn_only=True)


def test_read_module_code_keeps_indented_imports_but_drops_top_level(tmp_path):
    """With skip_top_level_only, the inlined test body drops column-0 imports
    (re-emitted in the imports cell) but preserves imports inside functions."""
    module = tmp_path / "test_example.py"
    module.write_text(
        "from src.bronze.ingest import run\n"
        "import os\n"
        "\n"
        "def test_local_only():\n"
        "    from src.common.helpers import local_helper\n"
        "    return local_helper(run, os)\n"
    )

    body = read_module_code(
        module, skip_imports=["import ", "from "], skip_top_level_only=True
    )

    assert "from src.bronze.ingest import run" not in body
    assert "import os" not in body
    # Indented import inside the test function survives.
    assert "    from src.common.helpers import local_helper" in body
    assert "def test_local_only():" in body


def test_generate_test_notebook_strips_top_level_src_and_keeps_indented(tmp_path):
    """End-to-end: a generated test notebook must carry no top-level src.*
    import (they execute in Fabric) while keeping indented ones, and must
    pass the test-notebook validation without raising."""
    module = tmp_path / "test_sales.py"
    module.write_text(
        "from datetime import date\n"
        "from src.bronze.ingest import run\n"
        "from src.backup.backup_lakehouse_files import backup_lakehouse_files\n"
        "\n"
        "def run_tests(spark, logger):\n"
        "    from src.common.helpers import local_helper\n"
        "    _ = (date, run, backup_lakehouse_files, local_helper)\n"
    )
    config = NotebookConfig(
        module_path=module, notebook_name="test_sales", layer="tests"
    )

    content = generate_test_notebook(config)

    top_level_src = [
        line
        for line in content.split("\n")
        if line.startswith("from src.") or line.startswith("import src.")
    ]
    assert not top_level_src, f"top-level src imports leaked: {top_level_src}"
    # The indented import inside run_tests is preserved in the inlined body.
    assert "    from src.common.helpers import local_helper" in content
    # Non-src top-level imports still make it into the imports cell.
    assert "from datetime import date" in content
    # Validation (as write_notebook runs it for test notebooks) must not raise.
    validate_no_src_imports("test_sales", content, warn_only=True)


def _write_minimal_test_module(tmp_path):
    module = tmp_path / "test_sales.py"
    module.write_text("def run_tests(spark, logger):\n    pass\n")
    return NotebookConfig(module_path=module, notebook_name="test_sales", layer="tests")


def test_generate_test_notebook_has_no_pip_cell_by_default(tmp_path, monkeypatch):
    monkeypatch.setattr(gen, "NOTEBOOK_GEN_CONFIG", {})
    content = generate_test_notebook(_write_minimal_test_module(tmp_path))
    assert "pip" not in content


def test_generate_test_notebook_installs_configured_pip_packages_first(tmp_path, monkeypatch):
    """test_notebook_pip_packages adds a cell before any other that installs the
    missing packages. It must be plain Python, not %pip, which Fabric rejects in
    high-concurrency pipeline sessions."""
    monkeypatch.setattr(
        gen, "NOTEBOOK_GEN_CONFIG", {"test_notebook_pip_packages": ["pytest==9.2.0", "freezegun"]}
    )
    content = generate_test_notebook(_write_minimal_test_module(tmp_path))
    first_cell = content.split("# CELL ********************")[1].split("# METADATA")[0]
    assert "['pytest==9.2.0', 'freezegun']" in first_cell
    assert "%pip" not in content
    assert "RUN_MAIN = False" in content.split("# CELL ********************")[2]
    ast.parse(first_cell)


def test_pip_install_cell_installs_only_missing_packages(monkeypatch):
    calls = []
    monkeypatch.setattr(subprocess, "check_call", lambda cmd: calls.append(cmd))
    ns: dict = {}
    exec(gen._pip_install_cell(["pytest==9.2.0", "surely-not-installed-pkg[extra]>=1"]), ns)
    assert ns["_missing_pip_packages"] == ["surely-not-installed-pkg[extra]>=1"]
    assert calls == [[sys.executable, "-m", "pip", "install", "-q", "surely-not-installed-pkg[extra]>=1"]]


def test_pip_install_cell_skips_pip_when_another_notebook_installed_while_waiting(monkeypatch):
    """Notebooks in one high-concurrency session share a Python env. The install
    runs under an exclusive lock, and a notebook that waited for the lock checks
    again and skips pip, since another notebook has installed the package."""
    import fcntl
    import importlib.metadata

    installed = set()
    real_distribution = importlib.metadata.distribution

    def fake_distribution(name):
        if name == "late-pkg" and name not in installed:
            raise importlib.metadata.PackageNotFoundError(name)
        return real_distribution("pytest")

    locks = []

    def fake_flock(fd, op):
        locks.append(op)
        installed.add("late-pkg")  # another notebook finished while this one waited

    calls = []
    monkeypatch.setattr(importlib.metadata, "distribution", fake_distribution)
    monkeypatch.setattr(fcntl, "flock", fake_flock)
    monkeypatch.setattr(subprocess, "check_call", lambda cmd: calls.append(cmd))
    ns: dict = {}
    exec(gen._pip_install_cell(["late-pkg==1.0"]), ns)
    assert locks == [fcntl.LOCK_EX]
    assert ns["_missing_pip_packages"] == []
    assert calls == []


def test_pip_install_cell_takes_no_lock_when_nothing_is_missing(monkeypatch):
    import fcntl

    monkeypatch.setattr(fcntl, "flock", lambda fd, op: pytest.fail("locked with nothing to install"))
    monkeypatch.setattr(subprocess, "check_call", lambda cmd: pytest.fail("ran pip with nothing to install"))
    exec(gen._pip_install_cell(["pytest"]), {})
