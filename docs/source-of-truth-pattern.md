# Source-of-truth pattern

Fabric notebooks can't `import` Python modules from your repository.
Every notebook is its own file. That's bad for review, refactoring, and
reuse.

`pyfabric-dev` flips the model: **plain Python modules are the source
of truth**, and Fabric notebooks are *generated* from them. You commit
both — the modules for humans, the notebooks for Fabric — but you only
ever edit the modules.

```
src/                            (you edit these)
  common/defs.py
  common/functions.py
  bronze/ingest_sales.py
  silver/transform_sales.py
  gold/build_daily_summary.py
tests/
  test_silver_transform_sales.py

bronze/                         (generated; committed)
  10_bronze_ingest_sales.Notebook/
    notebook-content.py
    .platform
silver/
  20_silver_transform_sales.Notebook/
  tests/
    test_20_silver_transform_sales.Notebook/
gold/
  30_gold_build_daily_summary.Notebook/
common/
  common_defs.Notebook/
  common_functions.Notebook/
```

## How generation works

`pyfabric-generate` walks `src/` looking for layer subdirectories
(`bronze/`, `silver/`, `gold/`, `backup/`, `common/`). For each `.py`
file it finds:

1. **Strips `src.common.*` and `pyfabric_dev.*` imports.** Fabric
   doesn't have a `src` or `pyfabric_dev` package at runtime — those
   symbols arrive via `%run common_functions`, which the generator
   inlines into a single `common/common_functions.Notebook`.
2. **Inlines the module body** as the notebook's main cell.
3. **Prepends a `%run common_functions`** cell so the notebook has
   access to shared helpers.
4. **Adds a `.platform` file** with a logical ID Fabric uses for
   identity tracking. (Hand-authored — the generator does not
   regenerate IDs, so they're stable across regeneration.)

## What gets generated

| Source | Notebook | Layer dir |
|---|---|---|
| `src/common/defs.py` | `common_defs.Notebook` | `common/` |
| `src/common/functions.py` | `common_functions.Notebook` | `common/` |
| `src/bronze/*.py` | `10_bronze_*.Notebook` | `bronze/` |
| `src/silver/*.py` | `20_silver_*.Notebook` | `silver/` |
| `src/gold/*.py` | `30_gold_*.Notebook` | `gold/` |
| `tests/test_<layer>_*.py` | `test_<prefix>_*.Notebook` | `<layer>/tests/` |

A module qualifies as a "pipeline module" when it defines a top-level
`run(spark, logger)` function. That function is what the generated
notebook calls — the rest of the file (helpers, constants) is inlined
verbatim.

## Configuration

`config/lakehouse_config.json` declares which lakehouse each layer
binds to. The schema:

```json
{
  "bronze": {
    "default_lakehouse": "<uuid>",
    "default_lakehouse_name": "bronze_lakehouse",
    "default_lakehouse_workspace_id": "<workspace-uuid>",
    "known_lakehouses": [{"id": "<uuid>"}]
  },
  "silver": { ... },
  "gold":   { ... },
  "common": { ... },
  "tests":  { ... }
}
```

The generator embeds these IDs into each notebook's METADATA block.
Fabric uses them to attach the correct lakehouse at runtime.

## Why commit generated notebooks

Two reasons:

1. **Fabric's git integration reads `.Notebook` directories**, not
   Python modules. Without committing the generated artifacts there's
   nothing for Fabric to sync.
2. **Reviewing the generated diff catches generator bugs.** If a
   refactor in `src/silver/transform_sales.py` produces an unexpected
   change in the generated notebook, that shows up in the same PR as
   the source change — not at deploy time.

The typical PR flow:

```bash
# Edit src/silver/transform_sales.py
$EDITOR src/silver/transform_sales.py

# Regenerate notebooks; commit both the source and the generated diff
pyfabric-generate --project-root .
git add src/silver/transform_sales.py silver/20_silver_transform_sales.Notebook/
git commit -m "silver: handle null SKUs"
```

A CI check that runs `pyfabric-generate` and asserts a clean working
tree (`git diff --exit-code`) prevents drift between source and
notebook.
