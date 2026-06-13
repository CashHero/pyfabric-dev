"""Tests for the notebook generator's src-import stripping and validation."""

import pytest

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
