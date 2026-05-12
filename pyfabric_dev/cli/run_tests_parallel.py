#!/usr/bin/env python3
"""Run pytest test files in parallel, mirroring a Fabric test pipeline.

Each worker subprocess gets an isolated Spark metastore (via the
``DEV_BASE_DIR`` env var) so that multiple Spark sessions can run
concurrently without Derby lock conflicts.

Batches are loaded from a JSON config file so the runner has no
hardcoded knowledge of any specific project's test files:

    pyfabric-test --config config/test_batches.json

If ``--config`` is omitted the runner looks for
``config/test_batches.json`` next to the CWD; absent that, it
auto-discovers every ``tests/test_*.py`` and runs them as a single
stage.

Config schema:

    {
      "notebook_runner_scripts": ["tests/test_foo.py", ...],
      "groups": {
        "common":        {"stage": 1, "files": [...]},
        "bronze_batch1": {"stage": 1, "files": [...]},
        "bronze_batch2": {"stage": 2, "files": [...]}
      },
      "stage_order": ["common", "bronze_batch1", "bronze_batch2", ...],
      "stage_aliases": {
        "bronze": ["bronze_batch1", "bronze_batch2"]
      }
    }
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple


DEFAULT_TIMEOUT = 600


# ----------------------------------------------------------------------
# Config loading
# ----------------------------------------------------------------------

def _auto_discover_config(project_root: Path) -> dict:
    """Build a default single-stage config from tests/test_*.py."""
    tests_dir = project_root / "tests"
    files = (
        sorted(str(p.relative_to(project_root)) for p in tests_dir.glob("test_*.py"))
        if tests_dir.exists()
        else []
    )
    return {
        "notebook_runner_scripts": [],
        "groups": {"all": {"stage": 1, "files": files}},
        "stage_order": ["all"],
        "stage_aliases": {"all": ["all"]},
    }


def _load_config(config_path: Optional[Path], project_root: Path) -> dict:
    if config_path is not None:
        if not config_path.exists():
            print(f"❌ Config file not found: {config_path}")
            sys.exit(1)
        cfg = json.loads(config_path.read_text())
    else:
        default_path = project_root / "config" / "test_batches.json"
        cfg = json.loads(default_path.read_text()) if default_path.exists() else _auto_discover_config(project_root)

    cfg.setdefault("notebook_runner_scripts", [])
    cfg.setdefault("groups", {})
    cfg.setdefault("stage_order", list(cfg["groups"].keys()))
    cfg.setdefault("stage_aliases", {name: [name] for name in cfg["groups"]})
    return cfg


# ----------------------------------------------------------------------
# Color helpers
# ----------------------------------------------------------------------

def _color(text: str, code: int) -> str:
    if not sys.stdout.isatty():
        return text
    return f"\033[{code}m{text}\033[0m"


def _green(text: str) -> str: return _color(text, 32)
def _red(text: str) -> str: return _color(text, 31)
def _yellow(text: str) -> str: return _color(text, 33)
def _bold(text: str) -> str: return _color(text, 1)


# ----------------------------------------------------------------------
# Worker
# ----------------------------------------------------------------------

def _run_test_file(
    test_file: str,
    worker_id: int,
    tmp_base: str,
    pytest_args: List[str],
    notebook_runner_scripts: List[str],
    timeout: int = DEFAULT_TIMEOUT,
) -> Tuple[str, int, str, float]:
    """Run a single test file. Notebook-runner scripts use ``python``; the
    rest use ``pytest``."""
    worker_dir = os.path.join(tmp_base, f"worker_{worker_id}")
    env = os.environ.copy()
    env["DEV_BASE_DIR"] = worker_dir
    os.makedirs(worker_dir, exist_ok=True)

    is_script = test_file in set(notebook_runner_scripts)

    start = time.monotonic()
    try:
        if is_script:
            result = subprocess.run(
                [sys.executable, test_file],
                capture_output=True, text=True, env=env, timeout=timeout,
            )
        else:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", test_file, "-v", "--tb=short", "--no-header"]
                + pytest_args,
                capture_output=True, text=True, env=env, timeout=timeout,
            )
    except subprocess.TimeoutExpired:
        return test_file, 1, f"TIMED OUT after {timeout}s", time.monotonic() - start

    duration = time.monotonic() - start
    output = result.stdout + (("\n" + result.stderr) if result.stderr else "")
    return test_file, result.returncode, output, duration


# ----------------------------------------------------------------------
# Collection / filtering
# ----------------------------------------------------------------------

def _collect_files(cfg: dict, stage_filter: Optional[str]) -> Tuple[List[str], List[str]]:
    """Return (stage1_files, stage2_files) based on optional stage alias filter."""
    groups = cfg["groups"]
    stage_order = cfg["stage_order"]
    aliases = cfg["stage_aliases"]

    if stage_filter:
        names = aliases.get(stage_filter)
        if not names:
            print(f"Unknown stage: {stage_filter}")
            print(f"Available: {', '.join(aliases)}")
            sys.exit(1)
        stage1, stage2 = [], []
        for name in names:
            info = groups[name]
            (stage1 if info["stage"] == 1 else stage2).extend(info["files"])
        return stage1, stage2

    stage1, stage2 = [], []
    for name in stage_order:
        info = groups[name]
        (stage1 if info["stage"] == 1 else stage2).extend(info["files"])
    return stage1, stage2


def _filter_existing(files: List[str], strict: bool) -> List[str]:
    existing, missing = [], []
    for f in files:
        (existing if Path(f).exists() else missing).append(f)
    if missing:
        if strict:
            for f in missing:
                print(f"  {_red('missing')} {f}")
            print(f"\n{_red('Aborting')}: {len(missing)} test file(s) not found (--strict)")
            sys.exit(1)
        for f in missing:
            print(f"  {_yellow('skip')} {f} (not found)")
    return existing


def _run_stage(
    label: str,
    files: List[str],
    max_workers: int,
    pytest_args: List[str],
    tmp_base: str,
    worker_counter: List[int],
    notebook_runner_scripts: List[str],
    timeout: int = DEFAULT_TIMEOUT,
) -> Tuple[List[str], List[str]]:
    if not files:
        return [], []

    print(f"\n{_bold(label)} ({len(files)} files, up to {max_workers} workers)")
    print("─" * 60)

    passed, failed = [], []
    futures: Dict[Future, str] = {}

    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        for f in files:
            wid = worker_counter[0]
            worker_counter[0] += 1
            fut = pool.submit(
                _run_test_file, f, wid, tmp_base, pytest_args,
                notebook_runner_scripts, timeout,
            )
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
                detail_lines = [
                    line for line in output.strip().splitlines()
                    if "FAILED" in line or "ERROR" in line or "assert" in line.lower()
                ]
                if detail_lines:
                    for line in detail_lines:
                        print(f"          {line}")
                else:
                    for line in output.strip().splitlines():
                        print(f"          {line}")

    return passed, failed


def main():
    def _positive_int(value: str) -> int:
        try:
            n = int(value)
        except ValueError:
            raise argparse.ArgumentTypeError(f"invalid integer: {value!r}")
        if n < 1:
            raise argparse.ArgumentTypeError(f"must be >= 1, got {n}")
        return n

    parser = argparse.ArgumentParser(
        description="Run pytest test files in parallel using a JSON batch config."
    )
    parser.add_argument("--config", type=Path, help="Path to test-batch config JSON")
    parser.add_argument("--project-root", type=Path, default=Path.cwd(),
                        help="Project root (default: CWD)")
    parser.add_argument("--max-workers", type=_positive_int,
                        default=min(os.cpu_count() or 4, 6),
                        help="Max parallel workers (default: min(cpu_count, 6))")
    parser.add_argument("--stage", help="Run only the named stage alias")
    parser.add_argument("--timeout", type=_positive_int, default=DEFAULT_TIMEOUT,
                        help=f"Per-file timeout in seconds (default: {DEFAULT_TIMEOUT})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show execution plan without running")
    parser.add_argument("--strict", action="store_true",
                        help="Fail if any expected test files are missing on disk")
    parser.add_argument("pytest_args", nargs=argparse.REMAINDER,
                        help="Extra args forwarded to pytest (e.g. -- -m 'not slow')")

    args = parser.parse_args()
    pytest_args = args.pytest_args or []
    if pytest_args and pytest_args[0] == "--":
        pytest_args = pytest_args[1:]

    cfg = _load_config(args.config, args.project_root)
    notebook_runner_scripts = cfg["notebook_runner_scripts"]

    stage1_files, stage2_files = _collect_files(cfg, args.stage)
    stage1_files = _filter_existing(stage1_files, args.strict)
    stage2_files = _filter_existing(stage2_files, args.strict)

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

    # Worker tmpdir lives under DEV_BASE_DIR so it follows whatever the
    # consumer configured (e.g. ~/.cashhero_fabric_dev/<hash>/...).
    from pyfabric_dev.local_config import DEV_BASE_DIR
    tmp_base = str(DEV_BASE_DIR / "_parallel_workers")

    start_time = time.monotonic()
    worker_counter = [0]
    all_passed, all_failed = [], []

    try:
        passed, failed = _run_stage(
            "Stage 1", stage1_files, args.max_workers, pytest_args,
            tmp_base, worker_counter, notebook_runner_scripts, args.timeout,
        )
        all_passed.extend(passed); all_failed.extend(failed)

        if stage2_files and not all_failed:
            passed, failed = _run_stage(
                "Stage 2", stage2_files, args.max_workers, pytest_args,
                tmp_base, worker_counter, notebook_runner_scripts, args.timeout,
            )
            all_passed.extend(passed); all_failed.extend(failed)
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
        print("\n  Failed files:")
        for f in all_failed:
            print(f"    {f}")
    print(f"{'═' * 60}")

    sys.exit(1 if all_failed else 0)


if __name__ == "__main__":
    main()
