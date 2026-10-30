#!/usr/bin/env bash
# Set up the two throwaway checkouts the migration tool builds in:
#
#   .a59/  jazz at v2.0.0-alpha.59, plus the read-only export hook and the
#          a59-export binary as a workspace member.
#   .a60/  jazz at the SQLite engine commit, plus the import hook and the
#          sqlite-import binary as a workspace member.
#
# Each binary is built by its own checkout's workspace (its lockfile,
# patches and profiles), so it links exactly that version of Jazz. The two
# can't share one build: they pull different libsqlite3-sys versions, which
# both declare `links = "sqlite3"`.
#
#   dev/migrate-alpha59/prepare.sh [a59|a60|both]
#   A59_DIR=... A60_DIR=...  use existing checkouts instead of cloning.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="${JAZZ_REPO:-https://github.com/garden-co/jazz.git}"
a59_ref="v2.0.0-alpha.59"
# Main with the catalogue stored in the app file (#4439). Repin to the
# alpha.60 release commit before any production run.
a60_ref="${A60_REF:-241b31d2}"

checkout() { # dir ref
  local dir="$1" ref="$2"
  if [[ ! -d "$dir/.git" && ! -f "$dir/.git" ]]; then
    git clone --filter=blob:none --no-checkout "$repo" "$dir"
  fi
  git -C "$dir" fetch --quiet origin "$ref" 2>/dev/null || git -C "$dir" fetch --quiet origin
  git -C "$dir" checkout --quiet --detach "$ref" 2>/dev/null || git -C "$dir" checkout --quiet --detach FETCH_HEAD
}

install_tool() { # dir crate
  local dir="$1" crate="$2"
  mkdir -p "$dir/dev/migrate-alpha59"
  rm -rf "$dir/dev/migrate-alpha59/dump" "$dir/dev/migrate-alpha59/$crate"
  cp -R "$here/dump" "$dir/dev/migrate-alpha59/dump"
  cp -R "$here/$crate" "$dir/dev/migrate-alpha59/$crate"
  rm -rf "$dir/dev/migrate-alpha59/dump/target" "$dir/dev/migrate-alpha59/dump/Cargo.lock"
  # Join that checkout's workspace.
  if ! grep -q "\"dev/migrate-alpha59/$crate\"" "$dir/Cargo.toml"; then
    sed -i.bak "0,/^members = \[/s||members = [\n  \"dev/migrate-alpha59/$crate\",|" "$dir/Cargo.toml"
    rm -f "$dir/Cargo.toml.bak"
  fi
}

hook() { # src dest mod-file anchor-line
  local src="$1" dest="$2" mod_file="$3" anchor="$4" name
  name="$(basename "$dest" .rs)"
  cp "$src" "$dest"
  if ! grep -q "pub mod $name;" "$mod_file"; then
    sed -i.bak "s|^$anchor\$|$anchor\npub mod $name;|" "$mod_file"
    rm -f "$mod_file.bak"
    grep -q "pub mod $name;" "$mod_file" || { echo "could not install $name into $mod_file" >&2; exit 1; }
  fi
}

prepare_a59() {
  local dir="${A59_DIR:-$here/.a59}"
  [[ -n "${A59_DIR:-}" ]] || checkout "$dir" "$a59_ref"
  hook "$here/export/hooks/migrate_export.rs" "$dir/crates/jazz/layers/node/src/node/migrate_export.rs" \
    "$dir/crates/jazz/layers/node/src/node/mod.rs" "pub mod api_error;"
  hook "$here/export/hooks/server_migrate_export.rs" "$dir/crates/jazz-server/src/server/migrate_export.rs" \
    "$dir/crates/jazz-server/src/server/mod.rs" "pub mod routes;"
  hook "$here/export/hooks/account_migrate_export.rs" "$dir/crates/jazz/layers/types/src/account_registry/migrate_export.rs" \
    "$dir/crates/jazz/layers/types/src/account_registry.rs" "pub mod storage;"
  install_tool "$dir" export
  install_tool "$dir" produce
  echo "alpha.59 exporter ready in $dir: cargo build --release -p migrate-alpha59-export"
}

prepare_a60() {
  local dir="${A60_DIR:-$here/.a60}"
  [[ -n "${A60_DIR:-}" ]] || checkout "$dir" "$a60_ref"
  hook "$here/import/hooks/runtime_migrate_import.rs" "$dir/crates/jazz-server/src/runtime/migrate_import.rs" \
    "$dir/crates/jazz-server/src/runtime/mod.rs" "pub mod auth_admission;"
  hook "$here/import/hooks/server_migrate_import.rs" "$dir/crates/jazz-server/src/server/migrate_import.rs" \
    "$dir/crates/jazz-server/src/server/mod.rs" "pub mod routes;"
  install_tool "$dir" import
  echo "SQLite importer ready in $dir: cargo build --release -p migrate-alpha59-import"
}

case "${1:-both}" in
  a59) prepare_a59 ;;
  a60) prepare_a60 ;;
  both) prepare_a59; prepare_a60 ;;
  *) echo "usage: $0 [a59|a60|both]" >&2; exit 2 ;;
esac
