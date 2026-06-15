#!/usr/bin/env bash
# Installs the Zuul client skills so Claude Code can discover them.
#
# Each subdirectory here containing a SKILL.md is a skill. This script symlinks
# every such skill into a Claude Code skills directory (symlink, so edits to the
# source under client/skills/ take effect immediately).
#
# Usage:
#   ./install.sh             # install into the project: <repo>/.claude/skills
#   ./install.sh --user      # install into the user scope: ~/.claude/skills
#   ./install.sh --copy      # copy instead of symlink
#   ./install.sh --uninstall # remove the installed skill links
#
# Invoke installed skills in Claude Code as /zuul-client-design and /zuul-k8s-deploy.
set -euo pipefail

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SRC_DIR/../.." && pwd)"

scope="project"
mode="symlink"
action="install"
for arg in "$@"; do
  case "$arg" in
    --user) scope="user" ;;
    --project) scope="project" ;;
    --copy) mode="copy" ;;
    --uninstall) action="uninstall" ;;
    -h|--help) sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "install.sh: unknown argument: $arg" >&2; exit 2 ;;
  esac
done

if [[ "$scope" == "user" ]]; then
  DEST="$HOME/.claude/skills"
else
  DEST="$REPO_ROOT/.claude/skills"
fi
mkdir -p "$DEST"

# Collect skills (directories holding a SKILL.md).
shopt -s nullglob
installed=0
for skill_md in "$SRC_DIR"/*/SKILL.md; do
  skill_dir="$(dirname "$skill_md")"
  name="$(basename "$skill_dir")"
  target="$DEST/$name"

  # Always clear any prior install of this skill first (idempotent).
  rm -rf "$target"
  if [[ "$action" == "uninstall" ]]; then
    echo "removed  $target"
    continue
  fi

  if [[ "$mode" == "copy" ]]; then
    cp -R "$skill_dir" "$target"
    echo "copied   $skill_dir -> $target"
  elif [[ "$scope" == "project" ]]; then
    # Relative link (DEST is <repo>/.claude/skills) so a committed .claude/skills
    # resolves in any clone, independent of where the repo lives.
    ln -s "../../client/skills/$name" "$target"
    echo "linked   $target -> ../../client/skills/$name"
  else
    ln -s "$skill_dir" "$target"
    echo "linked   $target -> $skill_dir"
  fi
  installed=$((installed + 1))
done

if [[ "$action" == "install" ]]; then
  echo
  echo "Installed $installed skill(s) into $DEST"
  echo "Use them in Claude Code: /zuul-client-design, /zuul-k8s-deploy"
fi
