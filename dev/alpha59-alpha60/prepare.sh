#!/usr/bin/env bash
# Set up the alpha.59 checkout the exporter builds in: jazz at
# v2.0.0-alpha.59, plus the read-only export hooks, the dump crate, and the
# exporter and test-fixture crates as workspace members.
#
# The exporter has to link alpha.59 itself: it reads the store with
# alpha.59's own decoders. It can't share a build with `jazz-tools` either:
# the two pull different libsqlite3-sys versions, which both declare
# `links = "sqlite3"`. The import side is `jazz-alpha59-alpha60-import`
# (crates/jazz-cli).
#
#   dev/alpha59-alpha60/prepare.sh
#   A59_DIR=...  use an existing checkout instead of cloning into .a59/.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
crates="$(cd "$here/../../crates" && pwd)"
repo="${JAZZ_REPO:-https://github.com/garden-co/jazz.git}"
a59_ref="v2.0.0-alpha.59"

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
  mkdir -p "$dir/dev/alpha59-alpha60"
  rm -rf "$dir/dev/alpha59-alpha60/$crate"
  cp -R "$here/$crate" "$dir/dev/alpha59-alpha60/$crate"
  # Join that checkout's workspace.
  # perl, not sed: `0,/re/` and `\n` in a replacement are GNU-only, and BSD
  # sed on macOS silently left the workspace unchanged.
  if ! grep -q "\"dev/alpha59-alpha60/$crate\"" "$dir/Cargo.toml"; then
    MEMBER="dev/alpha59-alpha60/$crate" perl -0pi -e \
      's/^members = \[/members = [\n  "$ENV{MEMBER}",/m' "$dir/Cargo.toml"
    grep -q "^  \"dev/alpha59-alpha60/$crate\",\$" "$dir/Cargo.toml" ||
      { echo "could not add dev/alpha59-alpha60/$crate to $dir/Cargo.toml" >&2; exit 1; }
  fi
}

hook() { # src dest mod-file anchor-line
  local src="$1" dest="$2" mod_file="$3" anchor="$4" name
  name="$(basename "$dest" .rs)"
  cp "$src" "$dest"
  if ! grep -q "pub mod $name;" "$mod_file"; then
    ANCHOR="$anchor" NAME="$name" perl -pi -e \
      's/^\Q$ENV{ANCHOR}\E$/$ENV{ANCHOR}\npub mod $ENV{NAME};/' "$mod_file"
    grep -qx "pub mod $name;" "$mod_file" || { echo "could not install $name into $mod_file" >&2; exit 1; }
  fi
}

dir="${A59_DIR:-$here/.a59}"
[[ -n "${A59_DIR:-}" ]] || checkout "$dir" "$a59_ref"
hook "$here/export/hooks/migrate_export.rs" "$dir/crates/jazz/layers/node/src/node/migrate_export.rs" \
  "$dir/crates/jazz/layers/node/src/node/mod.rs" "pub mod api_error;"
hook "$here/export/hooks/server_migrate_export.rs" "$dir/crates/jazz-server/src/server/migrate_export.rs" \
  "$dir/crates/jazz-server/src/server/mod.rs" "pub mod routes;"
hook "$here/export/hooks/account_migrate_export.rs" "$dir/crates/jazz/layers/types/src/account_registry/migrate_export.rs" \
  "$dir/crates/jazz/layers/types/src/account_registry.rs" "pub mod storage;"
# The dump format, shared with `jazz-alpha59-alpha60-import`.
rm -rf "$dir/crates/jazz-alpha59-alpha60-dump"
cp -R "$crates/jazz-alpha59-alpha60-dump" "$dir/crates/jazz-alpha59-alpha60-dump"
install_tool "$dir" export
install_tool "$dir" produce
echo "alpha59-alpha60 exporter ready in $dir: cargo build --release -p jazz-alpha59-alpha60-export"
