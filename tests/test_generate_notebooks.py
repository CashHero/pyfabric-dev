"""Tests for the notebook generator's src-import stripping and validation."""

import pytest

from pyfabric_dev.cli.generate_notebooks import (
    extract_imports_from_module,
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
