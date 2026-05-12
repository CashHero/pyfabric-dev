"""Extension points for the local notebook/pipeline runners.

Consumers register hooks to inject project-specific globals and to
override Fabric-bound helpers (e.g. ``cf_create_spark_session``) with
local equivalents. Without any hooks the runners execute notebooks
using whatever the notebook's own cells provide.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Optional


@dataclass
class RunnerHooks:
    """Customization points threaded through ``NotebookRunner``/``PipelineRunner``.

    Attributes:
        initial_globals: Seeded into the runner's globals dict before any
            cell executes. Used to inject local replacements for Fabric
            APIs (e.g. ``cf_create_spark_session``, lakehouse path
            constants) that notebooks expect to find at module scope.
        common_functions_overrides: Re-applied after each ``%run
            common_functions`` (or equivalent) so that local versions of
            key helpers aren't clobbered by the production common
            notebook executing in-place.
        notebook_globals: Per-notebook callback. Called with the
            resolved notebook path immediately before its cells run; the
            returned mapping is merged into the runner's globals. Useful
            for injecting symbols that the generator stripped (e.g.
            project-specific imports from a consumer's ``src/``).
        common_functions_name: Name of the notebook whose %run triggers
            ``common_functions_overrides``. Defaults to ``common_functions``.
    """

    initial_globals: Dict[str, object] = field(default_factory=dict)
    common_functions_overrides: Dict[str, object] = field(default_factory=dict)
    notebook_globals: Optional[Callable[[Path], Dict[str, object]]] = None
    common_functions_name: str = "common_functions"
