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
standalone_manifests=""
workspace=0
seen=' '
newline='
'

root_members=$(sed -n '/^members[[:space:]]*=[[:space:]]*\[/,/^]/p' Cargo.toml |
  sed -n 's/^[[:space:]]*"\([^"]*\)".*/\1/p')

is_root_member_manifest() {
  manifest=$1
  case "$manifest" in
    /*) manifest_abs=$manifest ;;
    *) manifest_abs=$root/$manifest ;;
  esac
  while IFS= read -r member; do
    [ -n "$member" ] || continue
    [ "$manifest_abs" = "$root/$member/Cargo.toml" ] && return 0
  done <<EOF
$root_members
EOF
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
        case "$newline$standalone_manifests" in *"$newline$manifest$newline"*) : ;; *)
          standalone_manifests="$standalone_manifests$manifest$newline"
        esac
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
elif [ -n "$packages" ]; then
  set -- clippy
  for package in $packages; do set -- "$@" --package "$package"; done
  set -- "$@" -- -D warnings
else
  set --
fi

if [ "$#" -gt 0 ]; then
  echo "Clippy: cargo $*"
  cargo "$@"
fi

while IFS= read -r manifest; do
  [ -n "$manifest" ] || continue
  echo "Clippy: cargo clippy --manifest-path $manifest -- -D warnings"
  cargo clippy --manifest-path "$manifest" -- -D warnings
done <<EOF
$standalone_manifests
EOF

[ "$workspace" -eq 1 ] || [ -n "$packages" ] || [ -n "$standalone_manifests" ] ||
  echo "Clippy: no staged Rust or Cargo.toml changes"
