#!/usr/bin/env python3
"""
Run pytest test files in parallel, mirroring the Fabric test pipeline structure.

Each worker subprocess gets an isolated Spark metastore (via DEV_BASE_DIR) so
that multiple Spark sessions can run concurrently without Derby lock conflicts.

Usage:
    python dev/run_tests_parallel.py                  # Run all tests
    python dev/run_tests_parallel.py --max-workers 4  # Limit concurrency
    python dev/run_tests_parallel.py --dry-run         # Show execution plan
    python dev/run_tests_parallel.py --stage bronze    # Run only bronze tests
    python dev/run_tests_parallel.py -- -m "not slow"  # Forward pytest args

Note: pytest args (e.g. -m, -k) only apply to pytest-based test files.
Notebook runner scripts (listed in NOTEBOOK_RUNNER_SCRIPTS) are always run
as-is with `python <file>` and are not affected by forwarded pytest args.
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from pathlib import Path

# ── Batch structure mirrors Fabric test pipelines ─────────────────────────────
#
# test_etl runs common, bronze, silver, gold, and customizations in parallel.
# Within bronze and gold there are two sequential batches.
#
# Stage 1: everything with no intra-layer dependencies
# Stage 2: bronze batch 2 and gold batch 2 (depend on their batch 1)

# Notebook runner scripts that should be run with `python <file>` instead of pytest.
# These files have no pytest tests — they execute Fabric test notebooks via NotebookRunner.
NOTEBOOK_RUNNER_SCRIPTS = {
    "tests/test_common_defs.py",
    "tests/test_common_functions.py",
    "tests/test_customizations_processor.py",
}

COMMON = [
    "tests/test_common_defs.py",
    "tests/test_common_functions.py",
    "tests/test_common_env.py",
    "tests/test_cf_format_rows_as_table.py",
]

BRONZE_BATCH1 = [
    "tests/test_bronze_extract_from_gl.py",
    "tests/test_bronze_ingest_from_priority.py",
    "tests/test_bronze_ingest_from_quickbooks.py",
    "tests/test_bronze_load_companies_config.py",
    "tests/test_bronze_update_watermark_table.py",
    "tests/test_bronze_post_process_priority_extract.py",
    "tests/test_bronze_preprocess_onprem_watermarks.py",
    "tests/test_bronze_load_budgets.py",
    "tests/test_bronze_load_salaries.py",
]

BRONZE_BATCH2 = [
    "tests/test_bronze_validate_bronze.py",
]

SILVER = [
    "tests/test_silver_transform_accounts.py",
    "tests/test_silver_transform_companies_config.py",
    "tests/test_silver_transform_currency_rates.py",
    "tests/test_silver_transform_general_ledger.py",
    "tests/test_silver_transform_budgets.py",
    "tests/test_silver_transform_salaries.py",
]

GOLD_BATCH1 = [
    "tests/test_gold_configure_companies.py",
    "tests/test_gold_finalize_accounts.py",
    "tests/test_gold_unify_accounts.py",
    "tests/test_gold_build_accounts_balances.py",
    "tests/test_gold_build_dim_dates.py",
    "tests/test_gold_build_budgets.py",
    "tests/test_gold_build_salaries.py",
]

GOLD_BATCH2 = [
    "tests/test_gold_build_fact_tables.py",
    "tests/test_gold_build_full_ledger.py",
    "tests/test_gold_export_to_sql.py",
    "tests/test_gold_onboard_org.py",
    "tests/test_gold_offboard_org.py",
]

CUSTOMIZATIONS = [
    "tests/test_customizations_processor.py",
]

OTHER = [
    "tests/test_extract_imports.py",
    "tests/test_generate_notebooks.py",
    "tests/test_run_pipeline.py",
]

# Ordered list ensures deterministic execution and dry-run output.
STAGE_ORDER = [
    "common",
    "bronze_batch1",
    "bronze_batch2",
    "silver",
    "gold_batch1",
    "gold_batch2",
    "customizations",
    "other",
]

STAGES = {
    "common": {"stage": 1, "files": COMMON},
    "bronze_batch1": {"stage": 1, "files": BRONZE_BATCH1},
    "bronze_batch2": {"stage": 2, "files": BRONZE_BATCH2},
    "silver": {"stage": 1, "files": SILVER},
    "gold_batch1": {"stage": 1, "files": GOLD_BATCH1},
    "gold_batch2": {"stage": 2, "files": GOLD_BATCH2},
    "customizations": {"stage": 1, "files": CUSTOMIZATIONS},
    "other": {"stage": 1, "files": OTHER},
}

# Friendly names for --stage filter
STAGE_ALIASES = {
    "common": ["common"],
    "bronze": ["bronze_batch1", "bronze_batch2"],
    "silver": ["silver"],
    "gold": ["gold_batch1", "gold_batch2"],
    "customizations": ["customizations"],
    "other": ["other"],
}


def _color(text: str, code: int) -> str:
    if not sys.stdout.isatty():
        return text
    return f"\033[{code}m{text}\033[0m"


def _green(text: str) -> str:
    return _color(text, 32)


def _red(text: str) -> str:
    return _color(text, 31)


def _yellow(text: str) -> str:
    return _color(text, 33)


def _bold(text: str) -> str:
    return _color(text, 1)


def _is_notebook_runner(test_file: str) -> bool:
    """Check if a test file is a notebook runner script (no pytest tests)."""
    return test_file in NOTEBOOK_RUNNER_SCRIPTS


# Default per-file timeout in seconds (10 minutes).
DEFAULT_TIMEOUT = 600


def _run_test_file(
    test_file: str,
    worker_id: int,
    tmp_base: str,
    pytest_args: list[str],
    timeout: int = DEFAULT_TIMEOUT,
) -> tuple[str, int, str, float]:
    """Run a single test file in an isolated Spark environment.

    Notebook runner scripts (no pytest tests) are run directly with python.
    Regular test files are run via pytest.

    Returns (test_file, returncode, output, duration_seconds).
    """
    worker_dir = os.path.join(tmp_base, f"worker_{worker_id}")

    env = os.environ.copy()
    env["DEV_BASE_DIR"] = worker_dir
    os.makedirs(worker_dir, exist_ok=True)

    is_script = _is_notebook_runner(test_file)

    start = time.monotonic()
    try:
        if is_script:
            result = subprocess.run(
                [sys.executable, test_file],
                capture_output=True,
                text=True,
                env=env,
                timeout=timeout,
            )
        else:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", test_file, "-v", "--tb=short",
                 "--no-header"] + pytest_args,
                capture_output=True,
                text=True,
                env=env,
                timeout=timeout,
            )
    except subprocess.TimeoutExpired:
        duration = time.monotonic() - start
        return test_file, 1, f"TIMED OUT after {timeout}s", duration
    duration = time.monotonic() - start
    output = result.stdout
    if result.stderr:
        output += "\n" + result.stderr

    return test_file, result.returncode, output, duration


def _collect_files(stage_filter: str | None) -> tuple[list[str], list[str]]:
    """Return (stage1_files, stage2_files) based on optional filter."""
    if stage_filter:
        groups = STAGE_ALIASES.get(stage_filter)
        if not groups:
            print(f"Unknown stage: {stage_filter}")
            print(f"Available: {', '.join(STAGE_ALIASES)}")
            sys.exit(1)
        stage1 = []
        stage2 = []
        for g in groups:
            info = STAGES[g]
            if info["stage"] == 1:
                stage1.extend(info["files"])
            else:
                stage2.extend(info["files"])
        return stage1, stage2

    stage1 = []
    stage2 = []
    for key in STAGE_ORDER:
        info = STAGES[key]
        if info["stage"] == 1:
            stage1.extend(info["files"])
        else:
            stage2.extend(info["files"])
    return stage1, stage2


def _filter_existing(files: list[str], strict: bool = False) -> list[str]:
    """Filter out test files that don't exist on disk.

    In strict mode, exit with an error if any files are missing.
    """
    existing = []
    missing = []
    for f in files:
        if Path(f).exists():
            existing.append(f)
        else:
            missing.append(f)
    if missing:
        if strict:
            for f in missing:
                print(f"  {_red('missing')} {f}")
            print(f"\n{_red('Aborting')}: {len(missing)} test file(s) not found (--strict)")
            sys.exit(1)
        else:
            for f in missing:
                print(f"  {_yellow('skip')} {f} (not found)")
    return existing


def _run_stage(
    label: str,
    files: list[str],
    max_workers: int,
    pytest_args: list[str],
    tmp_base: str,
    worker_counter: list[int],
    timeout: int = DEFAULT_TIMEOUT,
) -> tuple[list[str], list[str]]:
    """Run a list of test files in parallel. Returns (passed, failed) file lists."""
    if not files:
        return [], []

    print(f"\n{_bold(label)} ({len(files)} files, up to {max_workers} workers)")
    print("─" * 60)

    passed = []
    failed = []
    futures: dict[Future, str] = {}

    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        for f in files:
            wid = worker_counter[0]
            worker_counter[0] += 1
            fut = pool.submit(_run_test_file, f, wid, tmp_base, pytest_args, timeout)
            futures[fut] = f

        for fut in as_completed(futures):
            try:
                test_file, rc, output, duration = fut.result()
            except Exception:
                import traceback
                test_file = futures[fut]
                failed.append(test_file)
                short_name = Path(test_file).stem
                print(f"  {_red('CRASHED')}  {short_name}")
                for line in traceback.format_exc().strip().splitlines():
                    print(f"          {line}")
                continue
            short_name = Path(test_file).stem
            duration_str = f"{duration:.1f}s"
            if rc == 0:
                passed.append(test_file)
                print(f"  {_green('PASSED')}  {short_name}  ({duration_str})")
            else:
                failed.append(test_file)
                print(f"  {_red('FAILED')}  {short_name}  ({duration_str})")
                # Show failure details indented
                detail_lines = [
                    line for line in output.strip().splitlines()
                    if "FAILED" in line or "ERROR" in line or "assert" in line.lower()
                ]
                if detail_lines:
                    for line in detail_lines:
                        print(f"          {line}")
                else:
                    # No recognizable failure lines — show full output
                    for line in output.strip().splitlines():
                        print(f"          {line}")

    return passed, failed


def main():
    parser = argparse.ArgumentParser(
        description="Run pytest tests in parallel, mirroring the Fabric test pipeline."
    )
    def _positive_int(value: str) -> int:
        try:
            n = int(value)
        except ValueError:
            raise argparse.ArgumentTypeError(f"invalid integer: {value!r}")
        if n < 1:
            raise argparse.ArgumentTypeError(f"must be >= 1, got {n}")
        return n

    parser.add_argument(
        "--max-workers",
        type=_positive_int,
        default=min(os.cpu_count() or 4, 6),
        help="Max parallel workers (default: min(cpu_count, 6))",
    )
    parser.add_argument(
        "--stage",
        choices=list(STAGE_ALIASES),
        help="Run only a specific layer",
    )
    parser.add_argument(
        "--timeout",
        type=_positive_int,
        default=DEFAULT_TIMEOUT,
        help=f"Per-file timeout in seconds (default: {DEFAULT_TIMEOUT})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show execution plan without running",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if any expected test files are missing on disk",
    )
    parser.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Extra args forwarded to pytest (e.g. -- -m 'not slow')",
    )

    args = parser.parse_args()
    # Strip a single leading "--" separator that REMAINDER may include
    pytest_args = args.pytest_args or []
    if pytest_args and pytest_args[0] == "--":
        pytest_args = pytest_args[1:]
    args.pytest_args = pytest_args
    stage1_files, stage2_files = _collect_files(args.stage)
    stage1_files = _filter_existing(stage1_files, strict=args.strict)
    stage2_files = _filter_existing(stage2_files, strict=args.strict)

    if args.dry_run:
        print(_bold("Stage 1 (parallel):"))
        for f in stage1_files:
            print(f"  {f}")
        if stage2_files:
            print(f"\n{_bold('Stage 2 (parallel, after stage 1):')}")
            for f in stage2_files:
                print(f"  {f}")
        print(f"\nTotal: {len(stage1_files) + len(stage2_files)} files, "
              f"max {args.max_workers} workers")
        return

    start_time = time.monotonic()
    base_dir_name = os.getenv("FABRIC_DEV_BASE_DIR_NAME", ".fabric_dev")
    tmp_base = str(Path.home() / base_dir_name / "_parallel_workers")
    worker_counter = [0]  # mutable counter shared across stages
    all_passed = []
    all_failed = []

    try:
        passed, failed = _run_stage(
            "Stage 1: common + bronze/1 + silver + gold/1 + customizations + other",
            stage1_files,
            args.max_workers,
            args.pytest_args,
            tmp_base,
            worker_counter,
            args.timeout,
        )
        all_passed.extend(passed)
        all_failed.extend(failed)

        if stage2_files and not all_failed:
            passed, failed = _run_stage(
                "Stage 2: bronze/2 + gold/2",
                stage2_files,
                args.max_workers,
                args.pytest_args,
                tmp_base,
                worker_counter,
                args.timeout,
            )
            all_passed.extend(passed)
            all_failed.extend(failed)
        elif stage2_files and all_failed:
            print(f"\n{_yellow('Skipping Stage 2')} — Stage 1 had failures")
    finally:
        shutil.rmtree(tmp_base, ignore_errors=True)

    total_duration = time.monotonic() - start_time
    print(f"\n{'═' * 60}")
    print(f"  {_green(f'{len(all_passed)} passed')}, "
          f"{_red(f'{len(all_failed)} failed') if all_failed else '0 failed'}  "
          f"in {total_duration:.1f}s")
    if all_failed:
        print(f"\n  Failed files:")
        for f in all_failed:
            print(f"    {f}")
    print(f"{'═' * 60}")

    sys.exit(1 if all_failed else 0)


if __name__ == "__main__":
    main()
