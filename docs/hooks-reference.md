# Hooks reference

`pyfabric_dev.runners.RunnerHooks` is how a project plugs
consumer-specific behavior into the otherwise-generic notebook and
pipeline runners.

You'll need it as soon as your notebooks depend on helpers that aren't
in the generator's standard inlined surface — for example, project-specific
secret loaders, alternate Spark builders, or symbols that the generator
strips out (like `from pyfabric_dev.functions import *`).

## The dataclass

```python
@dataclass
class RunnerHooks:
    initial_globals:            dict                              = {}
    common_functions_overrides: dict                              = {}
    notebook_globals:           Callable[[Path], dict] | None     = None
    common_functions_name:      str                               = "common_functions"
```

### `initial_globals`

A dict seeded into the runner's namespace **before any cell runs**.

Use case: project-local replacements for Fabric-bound helpers. The
production notebook calls `cf_create_spark_session()`; in Fabric that
resolves via `%run common_functions`, but locally you want a version
that builds a Delta-enabled session against your dev dir. Drop the
local version here.

```python
import my_project.common_functions_local as cfl

hooks = RunnerHooks(
    initial_globals={
        name: getattr(cfl, name) for name in dir(cfl) if not name.startswith("_")
    }
)
```

### `common_functions_overrides`

A dict re-applied **after each `%run common_functions`** completes. This
exists because the inlined `common_functions` notebook overwrites your
local versions when it executes; this dict is the "and then put my local
versions back" knob.

```python
hooks = RunnerHooks(
    common_functions_overrides={
        "cf_create_spark_session": my_local_spark_builder,
        "cf_get_lakehouse_path":   my_local_path_resolver,
    }
)
```

### `notebook_globals`

A callback invoked **before each notebook's cells run**. Receives the
resolved path; returns a dict that's merged into the runner's globals.

Use case: a specific notebook needs symbols the generator strips (e.g.
your `bronze/ingest_from_quickbooks` notebook calls
`get_authenticated_session`, which is imported from a local
`src/common/quickbooks_auth.py` that the generator inlines elsewhere
but doesn't expose to other notebooks).

```python
def _inject_per_notebook(path: Path) -> dict:
    if "ingest_from_quickbooks" in str(path):
        from src.common.quickbooks_auth import get_authenticated_session
        return {"get_authenticated_session": get_authenticated_session}
    return {}

hooks = RunnerHooks(notebook_globals=_inject_per_notebook)
```

### `common_functions_name`

Defaults to `"common_functions"`. Override if your project uses a
different name for the helpers notebook.

## Wiring hooks into the runners

```python
from pyfabric_dev.runners import NotebookRunner, PipelineRunner, RunnerHooks

hooks = RunnerHooks(
    initial_globals=...,
    common_functions_overrides=...,
    notebook_globals=...,
)

NotebookRunner(notebook_path, project_root, hooks=hooks).run()
PipelineRunner(project_root, resolver, hooks=hooks).run_pipeline(pipeline_path)
```

The CLIs (`pyfabric-run-notebook`, `pyfabric-run-pipeline`) construct
runners **without** hooks. If you need hooks, write a 30-line wrapper
script in your project that constructs `RunnerHooks` and calls the
runners directly. A typical pattern is a thin `dev/run_notebook.py` shim — a
`NotebookRunner` subclass that auto-applies the hooks, so consumer tests can
keep instantiating `NotebookRunner(path, root)`.

## When you don't need hooks

If your project's notebooks only use:

- Helpers defined in your own `src/common/functions.py` (the generator
  inlines them into the `common_functions` notebook),
- Plain `pyspark`/`delta` APIs, and
- Symbols from `pyfabric_dev` (these get stripped by the generator; the
  notebook is expected to redefine them or pull them from elsewhere in
  Fabric),

…then you don't need hooks. Construct `NotebookRunner` /
`PipelineRunner` with just `(notebook_path, project_root)` and let the
defaults work.
