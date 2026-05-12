"""Smoke tests: the package and its submodules import cleanly."""


def test_package_imports():
    import pyfabric_dev

    assert pyfabric_dev.__version__


def test_submodules_import():
    from pyfabric_dev import defs, spark, fabric, functions, mock_notebookutils  # noqa: F401
    from pyfabric_dev import local_config, local_env  # noqa: F401


def test_cli_modules_import():
    from pyfabric_dev.cli import (  # noqa: F401
        generate_notebooks,
        run_tests_parallel,
        run_notebook,
        run_pipeline,
    )


def test_runners_import():
    from pyfabric_dev.runners import (  # noqa: F401
        NotebookRunner,
        PipelineRunner,
        FabricIdResolver,
        RunnerHooks,
    )


def test_runner_hooks_defaults():
    from pyfabric_dev.runners import RunnerHooks

    h = RunnerHooks()
    assert h.initial_globals == {}
    assert h.common_functions_overrides == {}
    assert h.notebook_globals is None
    assert h.common_functions_name == "common_functions"


def test_framework_constants():
    from pyfabric_dev.defs import (
        BRONZE_LAKEHOUSE_NAME,
        SILVER_LAKEHOUSE_NAME,
        GOLD_LAKEHOUSE_NAME,
        BACKUP_LAKEHOUSE_NAME,
        DEFAULT_LAKEHOUSE_PATH,
        BACKUP_RETENTION_DAILY,
        BACKUP_RETENTION_WEEKLY,
        BACKUP_RETENTION_MONTHLY,
    )

    assert BRONZE_LAKEHOUSE_NAME == "bronze_lakehouse"
    assert SILVER_LAKEHOUSE_NAME == "silver_lakehouse"
    assert GOLD_LAKEHOUSE_NAME == "gold_lakehouse"
    assert BACKUP_LAKEHOUSE_NAME == "backup_lakehouse"
    assert DEFAULT_LAKEHOUSE_PATH == "/lakehouse/default"
    assert BACKUP_RETENTION_DAILY == 14
    assert BACKUP_RETENTION_WEEKLY == 8
    assert BACKUP_RETENTION_MONTHLY == 12


def test_local_config_defaults_are_framework_branded():
    """The framework default should NOT carry the cashhero brand."""
    from pyfabric_dev import local_config

    assert ".fabric_dev" in str(local_config.DEV_BASE_DIR) or "FABRIC_DEV_BASE_DIR_NAME" in str(
        local_config.DEV_BASE_DIR
    )
    assert "cashhero" not in str(local_config.DEV_BASE_DIR).lower()


def test_get_lakehouse_path_local():
    from pyfabric_dev.spark import get_lakehouse_path

    path = get_lakehouse_path("Tables/foo")
    assert path.endswith("Tables/foo")
