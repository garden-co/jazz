#!/usr/bin/env bash
# Regenerates TODO.md from the todo/ directory structure.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TODO="$ROOT/TODO.md"
TODO_DIR="$ROOT/todo"

extract_what() {
  # Pull the first non-empty line after "## What"
  awk '/^## What/{found=1; next} found && /^[^#]/ && NF{print; exit}' "$1"
}

{
  echo "# TODO"
  echo ""
  # --- Issues (flat, grouped by priority) ---
  if [ -d "$TODO_DIR/issues" ]; then
    echo "## Issues"
    echo ""
    for priority in critical high medium low unknown; do
      items=()
      for f in "$TODO_DIR/issues"/*.md; do
        [ -f "$f" ] || continue
        file_priority=$(awk '/^## Priority/{found=1; next} found && NF{print tolower($0); exit}' "$f")
        if [ "$file_priority" = "$priority" ]; then
          items+=("$f")
        fi
      done
      if [ ${#items[@]} -gt 0 ]; then
        label=$(echo "$priority" | awk '{print toupper(substr($0,1,1)) tolower(substr($0,2))}')
        echo "### $label"
        echo ""
        for f in "${items[@]}"; do
          name=$(basename "$f" .md)
          rel_path="${f#$ROOT/}"
          what=$(extract_what "$f")
          if [ -n "$what" ]; then
            echo "- [**$name**]($rel_path) — $what"
          else
            echo "- [**$name**]($rel_path)"
          fi
        done
        echo ""
      fi
    done
  fi

  # --- Ideas (grouped by priority bucket) ---
  if [ -d "$TODO_DIR/ideas" ]; then
    echo "## Ideas"
    echo ""
    for bucket_dir in "$TODO_DIR/ideas"/*/; do
      [ -d "$bucket_dir" ] || continue
      bucket=$(basename "$bucket_dir")
      # Pretty-print bucket name: 1_mvp -> MVP, 2_launch -> Launch, 3_later -> Later
      label=$(echo "$bucket" | sed 's/^[0-9]*_//' | awk '{print toupper(substr($0,1,1)) tolower(substr($0,2))}')
      echo "### $label"
      echo ""
      found=0
      for f in "$bucket_dir"*.md; do
        [ -f "$f" ] || continue
        found=1
        name=$(basename "$f" .md)
        rel_path="${f#$ROOT/}"
        what=$(extract_what "$f")
        if [ -n "$what" ]; then
          echo "- [**$name**]($rel_path) — $what"
        else
          echo "- [**$name**]($rel_path)"
        fi
      done
      if [ "$found" -eq 0 ]; then
        echo "(empty)"
      fi
      echo ""
    done
  fi


  # --- Projects (shaped specs) ---
  if [ -d "$TODO_DIR/projects" ]; then
    echo "## Projects"
    echo ""
    for proj_dir in "$TODO_DIR/projects"/*/; do
      [ -d "$proj_dir" ] || continue
      name=$(basename "$proj_dir")
      rel_path="${proj_dir#$ROOT/}"
      echo "- [**$name**]($rel_path)"
    done
  fi
} > "$TODO"

echo "Updated $TODO"
