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
packages=""
workspace=0
seen=' '

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
      elif case "$seen" in *" $package "*) false ;; *) true ;; esac; then
        packages="$packages $package"
        seen="$seen$package "
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
  set -- clippy --workspace -- -D warnings
else
  [ -n "$packages" ] || { echo "Clippy: no staged Rust or Cargo.toml changes"; exit 0; }
  set -- clippy
  for package in $packages; do set -- "$@" --package "$package"; done
  set -- "$@" -- -D warnings
fi

echo "Clippy: cargo $*"
exec cargo "$@"
