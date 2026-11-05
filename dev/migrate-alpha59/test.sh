#!/usr/bin/env bash
# End-to-end check of the alpha.59 migration on a synthetic tenant: build
# the exporter (in an alpha.59 checkout) and this checkout's jazz-tools,
# produce a tenant through a real alpha.59 server, export, import, verify,
# boot this jazz-tools' server on the result, and make sure verify catches a
# tampered cell.
#
#   dev/migrate-alpha59/test.sh            (clones .a59/ on first run)
#   A59_DIR=... dev/migrate-alpha59/test.sh
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
a59="${A59_DIR:-$here/.a59}"
root="$(cd "$here/../.." && pwd)"
t59="${A59_TARGET:-$a59/target}"
t60="${A60_TARGET:-$root/target}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
app=0a590000-0000-4000-8000-000000000059

"$here/prepare.sh"
# `--bin` filters every listed package, so the tool crates build separately.
(cd "$a59" && CARGO_TARGET_DIR="$t59" cargo build --release -p jazz-cli --bin jazz-tools &&
  CARGO_TARGET_DIR="$t59" cargo build --release -p migrate-alpha59-produce \
    -p jazz-alpha59-export -p jazz-alpha59-dump &&
  CARGO_TARGET_DIR="$t59" cargo test --release -p jazz-alpha59-export)
# The importer as self-hosters download it.
(cd "$root" && CARGO_TARGET_DIR="$t60" cargo build --release -p jazz-cli --bin jazz-alpha59-import)
import=("$t60/release/jazz-alpha59-import")

"$t59/release/a59-produce" --out "$work/a59" --server-bin "$t59/release/jazz-tools"
"$t59/release/jazz-alpha59-export" --data-dir "$work/a59" --work "$work/copy" --out "$work/dump"
"$t59/release/jazz-alpha59-export" --data-dir "$work/a59" --work "$work/copy2" --out "$work/census" --catalogue-only
"$t59/release/dump-census" "$work/census" | tee "$work/census.json"
grep -q '"unsupported_by_sqlite":\[\]' "$work/census.json"

(cd "$root" && CARGO_TARGET_DIR="$t60" cargo test --release -p jazz-cli --lib migrate_alpha59)
"${import[@]}" import --dump "$work/dump" --data-dir "$work/a60" --app-id "$app" |
  tee "$work/import.json"
# user-1, user-2 and user-9 link as createJazz users; revoked user-3 does not.
grep -q '"backend_identities_linked":3' "$work/import.json"
# The import keeps the app id beside the data, exactly as given: sqlite()
# on this dir runs as the alpha.59 app.
test "$(cat "$work/a60/app-id")" = "$app"
# The row settled out of order keeps its update as the winner, with the
# creation of the insert that settled after it.
# (Repeated deletes need two alpha.59 clients; unit tests cover them.)
grep -q '"inserts_with_their_own_creation":1' "$work/import.json"
# Two rows edited while deleted: one restored with the edit, one left
# deleted, its edit dropped.
grep -q '"versions_written_while_deleted":2' "$work/import.json"
grep -q '"restores_bringing_content_written_while_deleted":1' "$work/import.json"
grep -q '"deleted_rows_dropping_content_written_while_deleted":1' "$work/import.json"
# No version was refused: the v1 project keeps `paused`, which v2 dropped.
grep -q '"versions_skipped_rejected":0' "$work/import.json"
grep -q '"versions_keeping_a_removed_enum_variant":1' "$work/import.json"
"${import[@]}" verify --dump "$work/dump" --data-dir "$work/a60" --app-id "$app"

# Only the active schema came over, and rows written under v1 read under
# v2's names: no `title` column, every document has a `heading`.
python3 - "$work/a60/jazz.sqlite" <<'PY'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
assert c.execute("select count(*) from _jazz_lenses").fetchone()[0] == 0, "old lenses came over"
assert c.execute("select count(*) from _jazz_schemas").fetchone()[0] == 1, "old schemas came over"
cols = [r[1].split("~")[0] for r in c.execute("pragma table_info(documents)")]
assert "title" not in cols and "heading" in cols, cols
heading = next(r[1] for r in c.execute("pragma table_info(documents)") if r[1].split("~")[0] == "heading")
missing = c.execute(f'select count(*) from documents where "{heading}" is null').fetchone()[0]
assert missing == 0, f"{missing} documents lost their title"
# The project written under v1 keeps `paused`, though v2 dropped it.
table = next(r[0] for r in c.execute("select name from sqlite_master where type = 'table'")
             if r[0].split("~")[0] == "projects" and not r[0].endswith("__h"))
status = next(r[1] for r in c.execute(f'pragma table_info("{table}")') if r[1].split("~")[0] == "status")
paused = c.execute(f'select count(*) from "{table}" join _jazz_enum_tags e '
                   f'on e.tbl = ? and e.col = ? and e.tag = "{table}"."{status}" '
                   "where e.name = 'paused'", (table, status)).fetchone()[0]
assert paused == 1, f"{paused} projects kept `paused`"
PY

# Boot check: the server starts on the migrated dir and serves
# the migrated permissions.
cp -R "$work/a60" "$work/boot"
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
"$t60/release/jazz-tools" server "$app" --data-dir "$work/boot" --port "$port" --admin-secret test \
  >"$work/boot.log" 2>&1 &
server=$!
for _ in $(seq 1 100); do curl -sf "localhost:$port/health" >/dev/null && break; sleep 0.1; done
# The import's one deployment is the bundle a backend booting now pins to.
curl -sf -H "X-Jazz-Admin-Secret: test" "localhost:$port/apps/$app/admin/bundles" >"$work/bundles.json"
python3 - "$work/bundles.json" <<'PY'
import json, sys
bundles = json.load(open(sys.argv[1]))["bundles"]
assert len(bundles) == 1, bundles
assert bundles[0]["schemaHash"] and bundles[0]["permissions"], bundles
PY
kill -TERM "$server"
wait "$server" || true

# Verify must notice a changed cell.
cp -R "$work/a60" "$work/tampered"
python3 - "$work/tampered/jazz.sqlite" <<'PY'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
# Stored column names may carry a `~<hash>` suffix.
col = next(r[1] for r in c.execute("pragma table_info(projects)") if r[1].split("~")[0] == "name")
c.execute(f'update projects set "{col}" = \'tampered\' where rowid = (select min(rowid) from projects)')
c.commit()
PY
if "${import[@]}" verify --dump "$work/dump" --data-dir "$work/tampered" --app-id "$app"; then
  echo "verify missed a tampered cell" >&2
  exit 1
fi
echo "migrate-alpha59: all checks passed"
