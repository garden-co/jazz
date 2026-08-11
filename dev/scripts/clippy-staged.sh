#!/bin/sh
# Scoped clippy for Lefthook. The hook passes {staged_files}, so this script
# can invoke Cargo directly without Node spawning git/cargo (some restricted
# shells reject child_process.spawnSync even though direct commands work).
set -eu

# Lefthook runs commands from the repository root. Keeping the fallback as the
# current directory means this hook has no child-process dependency at all;
# callers invoking it elsewhere can set JAZZ_REPO_ROOT explicitly.
root=${JAZZ_REPO_ROOT:-.}
cd "$root"
root=$(pwd)
packages=""
workspace=0
seen=' '

metadata=$(mktemp "${TMPDIR:-/tmp}/clippy-metadata.XXXXXX")
members=$(mktemp "${TMPDIR:-/tmp}/clippy-members.XXXXXX")
trap 'rm -f "$metadata" "$members"' EXIT HUP INT TERM
if ! cargo metadata --no-deps --format-version 1 >"$metadata"; then
  echo "Clippy: unable to determine Cargo workspace members" >&2
  exit 1
fi
if ! node "$root/dev/scripts/clippy-workspace-metadata.mjs" \
  <"$metadata" >"$members"; then
  echo "Clippy: unable to parse Cargo workspace metadata" >&2
  exit 1
fi

is_root_member_manifest() {
  manifest=$1
  case "$manifest" in
    /*) manifest_abs=$manifest ;;
    *) manifest_abs=$root/$manifest ;;
  esac
  while IFS= read -r member; do
    [ "$manifest_abs" = "$member" ] && return 0
  done <"$members"
  return 1
}

for file in "$@"; do
  case "$file" in
    *.rs|*/Cargo.toml|Cargo.toml) : ;;
    *) continue ;;
  esac
  path=$file
  case "$path" in ./*) path=${path#./} ;; esac
  if [ "$path" = Cargo.toml ]; then workspace=1; break; fi
  directory=$(dirname "$path")
  while :; do
    manifest=$directory/Cargo.toml
    if [ -f "$manifest" ]; then
      package=$(sed -n 's/^name[[:space:]]*=[[:space:]]*"\([^"]*\)"/\1/p' "$manifest" | head -n 1)
      if [ -z "$package" ]; then
        workspace=1
      elif is_root_member_manifest "$manifest"; then
        case "$seen" in *" $package "*) : ;; *)
        packages="$packages $package"
        seen="$seen$package "
        esac
      else
        # Auxiliary crates may be standalone, excluded, or depend on
        # workspace-only assumptions. Preserve the old safe behavior: lint
        # the authoritative root workspace instead of making their own
        # manifest a blocking hook target.
        workspace=1
      fi
      break
    fi
    [ "$directory" = . ] && { workspace=1; break; }
    parent=$(dirname "$directory")
    [ "$parent" = "$directory" ] && { workspace=1; break; }
    directory=$parent
  done
done

if [ "$workspace" -eq 1 ]; then
  set -- clippy --workspace --all-targets -- -D warnings
elif [ -n "$packages" ]; then
  set -- clippy
  for package in $packages; do set -- "$@" --package "$package"; done
  set -- "$@" -- -D warnings
else
  set --
fi

if [ "$#" -gt 0 ]; then
  echo "Clippy: cargo $*"
  exec cargo "$@"
fi

echo "Clippy: no staged Rust or Cargo.toml changes"
