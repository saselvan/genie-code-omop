#!/bin/bash
# install.sh — install or update genie-code-omop skill into your workspace
# Usage: ./install.sh [profile-name]
#   profile-name defaults to DEFAULT
# Run again to update to latest from main branch.

set -euo pipefail

PROFILE="${1:-DEFAULT}"
REPO="saselvan/genie-code-omop"
SKILL_NAME="omop-pipeline-builder"

if ! command -v databricks &> /dev/null; then
  echo "Error: databricks CLI not found. Install from https://docs.databricks.com/en/dev-tools/cli/install.html" >&2
  exit 1
fi

USERNAME=$(databricks current-user me --profile "$PROFILE" -o json | python3 -c "import sys, json; print(json.load(sys.stdin)['userName'])")

if [ -z "$USERNAME" ]; then
  echo "Error: failed to resolve workspace username for profile '$PROFILE'" >&2
  echo "Run 'databricks current-user me --profile $PROFILE' to debug" >&2
  exit 1
fi

DEST="/Workspace/Users/$USERNAME/.assistant/skills/$SKILL_NAME"

TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT

echo "Downloading $REPO main branch..."
if ! curl -fsSL "https://github.com/$REPO/archive/main.tar.gz" | tar xz -C "$TMP"; then
  echo "Error: failed to download or extract tarball" >&2
  exit 1
fi

EXTRACTED_DIR=$(find "$TMP" -maxdepth 1 -type d -name "genie-code-omop-*" | head -1)
if [ -z "$EXTRACTED_DIR" ]; then
  echo "Error: extracted directory not found in $TMP" >&2
  exit 1
fi

echo "Importing to $DEST..."
databricks workspace import-dir "$EXTRACTED_DIR" "$DEST" --overwrite --profile "$PROFILE"

echo ""
echo "Installed: $DEST"
echo "Restart Genie Code Agent panel to discover the skill."
