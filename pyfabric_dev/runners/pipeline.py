"""Run Fabric pipelines locally.

Parses ``pipeline-content.json`` definitions, resolves notebook and
pipeline ``logicalId``s to local file paths via ``.platform`` files,
and executes activities in dependency order — mirroring how Fabric's
orchestrator processes the DAG.

Notebook activities are dispatched to :class:`NotebookRunner`. The
runner instance (and therefore its globals namespace) is shared across
all notebook activities in a single pipeline run, which is what makes
multi-notebook pipelines feel coherent locally.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pyfabric_dev.runners.hooks import RunnerHooks
from pyfabric_dev.runners.notebook import NotebookRunner


SKIPPABLE_ACTIVITY_TYPES = frozenset({"Teams", "MicrosoftTeams", "RefreshDataflow"})


class ActivityStatus(Enum):
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    SKIPPED = "Skipped"


@dataclass
class Activity:
    name: str
    type: str
    type_properties: dict
    depends_on: list
    policy: dict = field(default_factory=dict)


class FabricIdResolver:
    """Resolves Fabric logicalIds to local file paths by scanning .platform files."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self._notebook_map: Dict[str, Path] = {}
        self._pipeline_map: Dict[str, Path] = {}
        self._scanned = False

    def _scan(self) -> None:
        if self._scanned:
            return
        for platform_file in self.project_root.rglob(".platform"):
            try:
                data = json.loads(platform_file.read_text())
                logical_id = data.get("config", {}).get("logicalId")
                artifact_type = data.get("metadata", {}).get("type")
                if not logical_id:
                    continue
                parent_dir = platform_file.parent
                if artifact_type == "Notebook":
                    self._notebook_map[logical_id] = parent_dir
                elif artifact_type == "DataPipeline":
                    self._pipeline_map[logical_id] = parent_dir
            except (json.JSONDecodeError, OSError):
                continue
        self._scanned = True

    def resolve_notebook(self, notebook_id: str) -> Optional[Path]:
        self._scan()
        nb_dir = self._notebook_map.get(notebook_id)
        return nb_dir / "notebook-content.py" if nb_dir else None

    def resolve_pipeline(self, pipeline_id: str) -> Optional[Path]:
        self._scan()
        pl_dir = self._pipeline_map.get(pipeline_id)
        return pl_dir / "pipeline-content.json" if pl_dir else None

    def notebook_display_name(self, notebook_id: str) -> str:
        self._scan()
        nb_dir = self._notebook_map.get(notebook_id)
        if nb_dir:
            return nb_dir.name.replace(".Notebook", "")
        return f"<unknown:{notebook_id[:8]}>"

    def pipeline_display_name(self, pipeline_id: str) -> str:
        self._scan()
        pl_dir = self._pipeline_map.get(pipeline_id)
        if pl_dir:
            return pl_dir.name.replace(".DataPipeline", "")
        return f"<unknown:{pipeline_id[:8]}>"

    def get_all_pipelines(self) -> Dict[str, Path]:
        self._scan()
        result: Dict[str, Path] = {}
        for _logical_id, pipeline_dir in self._pipeline_map.items():
            try:
                data = json.loads((pipeline_dir / ".platform").read_text())
                display_name = data.get("metadata", {}).get("displayName", pipeline_dir.name)
            except (json.JSONDecodeError, OSError):
                display_name = pipeline_dir.name
            result[display_name] = pipeline_dir
        return result


def _load_pipeline(pipeline_path: Path) -> tuple:
    data = json.loads(pipeline_path.read_text())
    props = data.get("properties", {})

    activities = [
        Activity(
            name=a["name"],
            type=a["type"],
            type_properties=a.get("typeProperties", {}),
            depends_on=a.get("dependsOn", []),
            policy=a.get("policy", {}),
        )
        for a in props.get("activities", [])
    ]

    variables: Dict[str, object] = {
        name: var_def.get("defaultValue")
        for name, var_def in props.get("variables", {}).items()
    }
    return activities, variables


def topological_levels(activities: List[Activity]) -> List[List[Activity]]:
    by_name = {a.name: a for a in activities}
    in_degree: Dict[str, int] = {a.name: 0 for a in activities}
    dependents: Dict[str, List[str]] = {a.name: [] for a in activities}

    for a in activities:
        for dep in a.depends_on:
            dep_name = dep["activity"]
            if dep_name in by_name:
                in_degree[a.name] += 1
                dependents[dep_name].append(a.name)

    levels: List[List[Activity]] = []
    ready = sorted(name for name, deg in in_degree.items() if deg == 0)
    while ready:
        levels.append([by_name[name] for name in ready])
        next_ready: List[str] = []
        for name in ready:
            for dep_name in dependents[name]:
                in_degree[dep_name] -= 1
                if in_degree[dep_name] == 0:
                    next_ready.append(dep_name)
        ready = sorted(next_ready)

    total = sum(len(level) for level in levels)
    if total != len(activities):
        sorted_names = {a.name for level in levels for a in level}
        unsorted = [a.name for a in activities if a.name not in sorted_names]
        raise ValueError(f"Cycle detected in activity dependencies: {unsorted}")
    return levels


def check_dependency_conditions(
    activity: Activity, outcomes: Dict[str, ActivityStatus]
) -> bool:
    for dep in activity.depends_on:
        dep_name = dep["activity"]
        conditions = set(dep.get("dependencyConditions", ["Succeeded"]))
        outcome = outcomes.get(dep_name)
        if outcome is None:
            return False

        matched = False
        if "Completed" in conditions:
            matched = outcome in (
                ActivityStatus.SUCCEEDED, ActivityStatus.FAILED, ActivityStatus.SKIPPED
            )
        if not matched and "Skipped" in conditions and outcome == ActivityStatus.SKIPPED:
            matched = True
        if not matched and outcome.value in conditions:
            matched = True
        if not matched:
            return False
    return True


class PipelineRunner:
    """Executes Fabric pipeline definitions locally."""

    def __init__(
        self,
        project_root: Path,
        resolver: FabricIdResolver,
        *,
        dry_run: bool = False,
        hooks: Optional[RunnerHooks] = None,
        medallion_layers: Optional[Iterable[str]] = None,
    ):
        self.project_root = project_root
        self.resolver = resolver
        self.dry_run = dry_run
        self.hooks = hooks or RunnerHooks()
        self.medallion_layers = (
            tuple(medallion_layers) if medallion_layers is not None else None
        )
        self._notebook_runner: Optional[NotebookRunner] = None

    def _get_notebook_runner(self) -> NotebookRunner:
        if self._notebook_runner is None:
            kwargs: Dict[str, object] = {"hooks": self.hooks}
            if self.medallion_layers is not None:
                kwargs["medallion_layers"] = self.medallion_layers
            self._notebook_runner = NotebookRunner(
                notebook_path=Path("/dev/null"),
                project_root=self.project_root,
                **kwargs,  # type: ignore[arg-type]
            )
        return self._notebook_runner

    # ------------------------------------------------------------------

    def run_pipeline(self, pipeline_path: Path, *, depth: int = 0) -> bool:
        indent = "  " * depth
        pipeline_name = pipeline_path.parent.name.replace(".DataPipeline", "")

        print(f"\n{indent}{'=' * 60}")
        print(f"{indent}🔄 Pipeline: {pipeline_name}")
        print(f"{indent}{'=' * 60}")

        activities, variables = _load_pipeline(pipeline_path)
        levels = topological_levels(activities)

        if self.dry_run:
            self._print_execution_plan(levels, variables, indent)
            return True

        outcomes: Dict[str, ActivityStatus] = {}
        has_failure = False

        for level in levels:
            for activity in level:
                if not check_dependency_conditions(activity, outcomes):
                    print(f"{indent}  ⏭️  Skipping '{activity.name}' (dependency conditions not met)")
                    outcomes[activity.name] = ActivityStatus.SKIPPED
                    continue

                status = self._run_activity(activity, variables, outcomes, depth)
                outcomes[activity.name] = status
                if status == ActivityStatus.FAILED:
                    has_failure = True

        emoji = "❌" if has_failure else "✅"
        word = "FAILED" if has_failure else "SUCCEEDED"
        print(f"\n{indent}{emoji} Pipeline '{pipeline_name}' {word}")
        print(f"{indent}{'=' * 60}")
        return not has_failure

    def _print_execution_plan(self, levels, variables, indent: str) -> None:
        print(f"{indent}  Variables: {variables}" if variables else "")
        for i, level in enumerate(levels):
            print(f"{indent}  Level {i}:")
            for a in level:
                deps = ", ".join(
                    f"{d['activity']} ({'/'.join(d.get('dependencyConditions', ['Succeeded']))})"
                    for d in a.depends_on
                )
                extra = ""
                if a.type == "TridentNotebook":
                    nb_id = a.type_properties.get("notebookId", "")
                    extra = f" → {self.resolver.notebook_display_name(nb_id)}"
                elif a.type == "InvokePipeline":
                    pl_id = a.type_properties.get("pipelineId", "")
                    extra = f" → {self.resolver.pipeline_display_name(pl_id)}"
                dep_str = f"  (after: {deps})" if deps else ""
                print(f"{indent}    [{a.type}] {a.name}{extra}{dep_str}")

    # ------------------------------------------------------------------

    def _run_activity(
        self,
        activity: Activity,
        variables: Dict[str, object],
        outcomes: Dict[str, ActivityStatus],
        depth: int,
    ) -> ActivityStatus:
        indent = "  " * depth

        if activity.type == "TridentNotebook":
            return self._run_notebook(activity, indent)
        if activity.type == "InvokePipeline":
            return self._run_child_pipeline(activity, indent, depth)
        if activity.type == "SetVariable":
            return self._run_set_variable(activity, variables, indent)
        if activity.type == "IfCondition":
            return self._run_if_condition(activity, variables, outcomes, indent, depth)
        if activity.type in SKIPPABLE_ACTIVITY_TYPES:
            print(f"{indent}  📩 Skipping {activity.type}: '{activity.name}'")
            return ActivityStatus.SKIPPED

        print(f"{indent}  ⚠️  Unknown activity type '{activity.type}': '{activity.name}' — skipping")
        return ActivityStatus.SKIPPED

    def _run_notebook(self, activity: Activity, indent: str) -> ActivityStatus:
        notebook_id = activity.type_properties.get("notebookId", "")
        notebook_path = self.resolver.resolve_notebook(notebook_id)
        if not notebook_path:
            print(f"{indent}  ❓ Cannot resolve notebook ID {notebook_id} for '{activity.name}'")
            return ActivityStatus.FAILED

        display = self.resolver.notebook_display_name(notebook_id)
        print(f"{indent}  📓 Running notebook: {display}")

        runner = self._get_notebook_runner()

        params = activity.type_properties.get("parameters", {})
        saved: dict = {}
        for param_name, param_def in params.items():
            if param_name in runner.globals_dict:
                saved[param_name] = runner.globals_dict[param_name]
            runner.globals_dict[param_name] = param_def.get("value")
            print(f"{indent}     📎 {param_name} = {param_def.get('value')}")

        start = time.time()
        try:
            runner.execute_notebook(notebook_path, is_dependency=False)
            elapsed = time.time() - start
            print(f"{indent}  ✅ '{display}' succeeded ({elapsed:.1f}s)")
            return ActivityStatus.SUCCEEDED
        except Exception as e:
            elapsed = time.time() - start
            print(f"{indent}  ❌ '{display}' failed ({elapsed:.1f}s): {e}")
            return ActivityStatus.FAILED
        finally:
            for param_name in params:
                if param_name in saved:
                    runner.globals_dict[param_name] = saved[param_name]
                else:
                    runner.globals_dict.pop(param_name, None)

    def _run_child_pipeline(self, activity: Activity, indent: str, depth: int) -> ActivityStatus:
        pipeline_id = activity.type_properties.get("pipelineId", "")
        pipeline_path = self.resolver.resolve_pipeline(pipeline_id)
        if not pipeline_path:
            display = self.resolver.pipeline_display_name(pipeline_id)
            print(f"{indent}  ❓ Cannot resolve pipeline ID {pipeline_id} for '{activity.name}' ({display})")
            return ActivityStatus.FAILED

        display = self.resolver.pipeline_display_name(pipeline_id)
        print(f"{indent}  🔗 Invoking child pipeline: {display}")

        try:
            success = self.run_pipeline(pipeline_path, depth=depth + 1)
            return ActivityStatus.SUCCEEDED if success else ActivityStatus.FAILED
        except Exception as e:
            print(f"{indent}  ❌ Child pipeline '{display}' failed: {e}")
            return ActivityStatus.FAILED

    def _run_set_variable(self, activity: Activity, variables: dict, indent: str) -> ActivityStatus:
        var_name = activity.type_properties.get("variableName", "")
        var_value = activity.type_properties.get("value")
        variables[var_name] = var_value
        print(f"{indent}  📝 Set {var_name} = {var_value}")
        return ActivityStatus.SUCCEEDED

    def _run_if_condition(
        self,
        activity: Activity,
        variables: dict,
        outcomes: dict,
        indent: str,
        depth: int,
    ) -> ActivityStatus:
        expr = activity.type_properties.get("expression", {})
        expr_value = expr.get("value", "")

        result = False
        match = re.search(r"@variables\('(\w+)'\)", expr_value)
        if match:
            result = bool(variables.get(match.group(1)))

        branch_key = "ifTrueActivities" if result else "ifFalseActivities"
        branch_label = "TRUE" if result else "FALSE"
        branch_activities = activity.type_properties.get(branch_key, [])

        print(f"{indent}  🔀 IfCondition '{activity.name}': {expr_value} → {branch_label}")

        inner_activities = [
            Activity(
                name=a["name"],
                type=a["type"],
                type_properties=a.get("typeProperties", {}),
                depends_on=a.get("dependsOn", []),
                policy=a.get("policy", {}),
            )
            for a in branch_activities
        ]

        if not inner_activities:
            return ActivityStatus.SUCCEEDED

        levels = topological_levels(inner_activities)
        inner_outcomes: Dict[str, ActivityStatus] = {}
        has_failure = False
        for level in levels:
            for inner_act in level:
                if not check_dependency_conditions(inner_act, inner_outcomes):
                    inner_outcomes[inner_act.name] = ActivityStatus.SKIPPED
                    continue
                status = self._run_activity(inner_act, variables, outcomes, depth + 1)
                inner_outcomes[inner_act.name] = status
                if status == ActivityStatus.FAILED:
                    has_failure = True

        return ActivityStatus.FAILED if has_failure else ActivityStatus.SUCCEEDED


# ------------------------------------------------------------------
# Discovery helpers (CLI uses these too)
# ------------------------------------------------------------------

def find_pipeline(
    project_root: Path,
    name: str,
    resolver: FabricIdResolver,
    medallion_layers: Iterable[str],
) -> Optional[Path]:
    """Resolve a user-provided pipeline name to its pipeline-content.json path."""
    candidate = project_root / name
    if candidate.is_file() and candidate.name == "pipeline-content.json":
        return candidate
    if candidate.is_dir():
        pc = candidate / "pipeline-content.json"
        if pc.exists():
            return pc
    with_suffix = project_root / f"{name}.DataPipeline" / "pipeline-content.json"
    if with_suffix.exists():
        return with_suffix

    for subdir in medallion_layers:
        pc = project_root / subdir / f"{name}.DataPipeline" / "pipeline-content.json"
        if pc.exists():
            return pc
        pc = project_root / subdir / "tests" / f"{name}.DataPipeline" / "pipeline-content.json"
        if pc.exists():
            return pc

    all_pipelines = resolver.get_all_pipelines()
    for display_name, pipeline_dir in all_pipelines.items():
        if display_name == name or display_name.replace(".DataPipeline", "") == name:
            return pipeline_dir / "pipeline-content.json"

    matches = [
        (dn, d) for dn, d in all_pipelines.items() if name.lower() in dn.lower()
    ]
    if len(matches) == 1:
        return matches[0][1] / "pipeline-content.json"
    return None


def list_pipelines(project_root: Path) -> None:
    resolver = FabricIdResolver(project_root)
    all_pipelines = resolver.get_all_pipelines()
    print("\n📋 Available Pipelines:")
    print("=" * 70)
    for display_name in sorted(all_pipelines):
        rel = all_pipelines[display_name].relative_to(project_root)
        print(f"  {display_name:45s} {rel}")
    print(f"\nTotal: {len(all_pipelines)} pipelines")
