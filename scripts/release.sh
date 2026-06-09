#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $(basename "$0") [--dry-run] VERSION

Create and push a git tag, then publish a GitHub release.
VERSION must match the version in pyproject.toml (e.g. 0.5.0 or v0.5.0).

Publishing the GitHub release triggers the PyPI publish workflow.

Example:
  $(basename "$0") 0.5.0
EOF
}

DRY_RUN=false
VERSION_ARG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -n "$VERSION_ARG" ]]; then
        echo "error: unexpected argument: $1" >&2
        usage >&2
        exit 1
      fi
      VERSION_ARG="$1"
      shift
      ;;
  esac
done

if [[ -z "$VERSION_ARG" ]]; then
  echo "error: VERSION is required" >&2
  usage >&2
  exit 1
fi

VERSION="${VERSION_ARG#v}"
TAG="v${VERSION}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

run() {
  if $DRY_RUN; then
    echo "+ $*"
  else
    echo "> $*"
    "$@"
  fi
}

PYPROJECT_VERSION="$(
  python3 -c "import tomllib; print(tomllib.load(open('pyproject.toml', 'rb'))['project']['version'])"
)"

if [[ "$PYPROJECT_VERSION" != "$VERSION" ]]; then
  echo "error: requested version $VERSION does not match pyproject.toml ($PYPROJECT_VERSION)" >&2
  exit 1
fi

# __init__.__version__ is a separate hardcoded string; keep it from drifting
# out of sync with pyproject.toml.
INIT_VERSION="$(
  python3 -c "import re, pathlib; m = re.search(r'''__version__\s*=\s*[\"']([^\"']+)[\"']''', pathlib.Path('pyfabric_dev/__init__.py').read_text()); print(m.group(1) if m else '')"
)"

if [[ "$INIT_VERSION" != "$VERSION" ]]; then
  echo "error: requested version $VERSION does not match pyfabric_dev/__init__.py (${INIT_VERSION:-not found})" >&2
  exit 1
fi

command -v git >/dev/null || { echo "error: git not found" >&2; exit 1; }
command -v gh >/dev/null || { echo "error: gh not found" >&2; exit 1; }

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "error: working tree is dirty; commit or stash changes before releasing" >&2
  exit 1
fi

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$BRANCH" != "main" ]]; then
  echo "error: releases must be cut from main (currently on $BRANCH)" >&2
  exit 1
fi

# Ensure local main matches origin/main, so the tag lands on the merged
# release commit rather than a stale or unpushed local HEAD.
git fetch --quiet origin main
HEAD_SHA="$(git rev-parse HEAD)"
if [[ "$HEAD_SHA" != "$(git rev-parse origin/main)" ]]; then
  echo "error: local main is out of sync with origin/main; pull/push before releasing" >&2
  exit 1
fi

echo "Releasing $TAG..."

# Each step is idempotent so a release that failed partway (e.g. the tag was
# pushed but the GitHub release never got created) can be re-run safely.
if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  if [[ "$(git rev-parse "${TAG}^{commit}")" != "$HEAD_SHA" ]]; then
    echo "error: tag $TAG already exists locally but points to a different commit" >&2
    exit 1
  fi
  echo "tag $TAG already exists locally and matches HEAD; skipping tag creation"
else
  run git tag "$TAG"
fi

REMOTE_TAG_SHA="$(git ls-remote --tags origin "refs/tags/$TAG" | awk '{print $1}')"
if [[ -n "$REMOTE_TAG_SHA" ]]; then
  if [[ "$REMOTE_TAG_SHA" != "$HEAD_SHA" ]]; then
    echo "error: tag $TAG already exists on origin but points to a different commit" >&2
    exit 1
  fi
  echo "tag $TAG already on origin and matches HEAD; skipping push"
else
  run git push origin "$TAG"
fi

if gh release view "$TAG" >/dev/null 2>&1; then
  echo "release $TAG already exists; skipping creation"
else
  run gh release create "$TAG" --title "$TAG" --notes "See CHANGELOG.md"
fi

echo "Done. PyPI publish will run when the GitHub release is published."
