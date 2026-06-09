# Contributing to pyfabric-dev

Thanks for considering a contribution. This is an early-stage project.

## Workflow

1. Fork & branch from `main`.
2. Open a PR with a clear description. Bug fixes and small improvements are
   welcome without prior discussion; for larger changes (new CLI subcommands,
   API breakage, dependency additions), please open an issue first so we can
   align on direction.
3. Sign off your commits with `git commit -s` ([DCO](https://developercertificate.org/)).
   We use the Developer Certificate of Origin instead of a CLA.

## Local setup

```bash
git clone https://github.com/CashHero/pyfabric-dev.git
cd pyfabric-dev
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
pytest
```

You'll need Java 11+ on `PATH` for PySpark.

## Style

- No comments unless WHY is non-obvious. Don't restate WHAT the code does.
- Don't add backwards-compat shims, feature flags, or speculative abstractions.
- Tests stay green on `main`. If a test is flaky, fix it; don't `@skip`.

## Scope

`pyfabric-dev` is **framework code**. CashHero-specific helpers (Priority ERP,
QuickBooks ingestion, org onboarding logic) live in a separate closed-source
repo and won't be accepted here.

If your contribution feels generic enough that *any* Fabric medallion project
could use it, it belongs here. If it encodes business logic specific to your
data sources or schemas, keep it in your own repo.

## Releasing (maintainers)

1. Bump `version` in `pyproject.toml` and add a dated section to `CHANGELOG.md`.
2. Commit and merge to `main`.
3. Run from a clean checkout on `main`:

```bash
./scripts/release.sh 0.5.0
```

Use `--dry-run` to print the commands without executing them. The script tags
and pushes `v<version>`, creates a GitHub release, and the publish workflow
ships the package to PyPI.
