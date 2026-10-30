#!/usr/bin/env bash
# End-to-end check of the migration tool on a synthetic alpha.59 tenant:
# build both binaries, produce a tenant through a real alpha.59 server,
# export, import, verify, boot a real SQLite-engine server on the result,
# and make sure verify catches a tampered cell.
#
#   dev/migrate-alpha59/test.sh            (clones .a59/ and .a60/ on first run)
#   A59_DIR=... A60_DIR=... dev/migrate-alpha59/test.sh
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
a59="${A59_DIR:-$here/.a59}"
a60="${A60_DIR:-$here/.a60}"
t59="${A59_TARGET:-$a59/target}"
t60="${A60_TARGET:-$a60/target}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
app=0a590000-0000-4000-8000-000000000059

"$here/prepare.sh" both
# `--bin` filters every listed package, so the tool crates build separately.
(cd "$a59" && CARGO_TARGET_DIR="$t59" cargo build --release -p jazz-cli --bin jazz-tools &&
  CARGO_TARGET_DIR="$t59" cargo build --release -p migrate-alpha59-produce \
    -p migrate-alpha59-export -p migrate-alpha59-dump)
(cd "$a60" && CARGO_TARGET_DIR="$t60" cargo build --release -p jazz-cli --bin jazz-tools &&
  CARGO_TARGET_DIR="$t60" cargo build --release -p migrate-alpha59-import)

"$t59/release/a59-produce" --out "$work/a59" --server-bin "$t59/release/jazz-tools"
"$t59/release/a59-export" --data-dir "$work/a59" --work "$work/copy" --out "$work/dump"
"$t59/release/a59-export" --data-dir "$work/a59" --work "$work/copy2" --out "$work/census" --catalogue-only
"$t59/release/dump-census" "$work/census" | tee "$work/census.json"
grep -q '"unsupported_by_sqlite":\[\]' "$work/census.json"

"$t60/release/sqlite-import" --dump "$work/dump" --data-dir "$work/a60" --app-id "$app"
"$t60/release/sqlite-import" verify --dump "$work/dump" --data-dir "$work/a60" --app-id "$app"

# Boot check: the real alpha.60 server starts on the migrated dir and serves
# the migrated permissions.
cp -R "$work/a60" "$work/boot"
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
"$t60/release/jazz-tools" server "$app" --data-dir "$work/boot" --port "$port" --admin-secret test \
  >"$work/boot.log" 2>&1 &
server=$!
for _ in $(seq 1 100); do curl -sf "localhost:$port/health" >/dev/null && break; sleep 0.1; done
curl -sf -H "X-Jazz-Admin-Secret: test" "localhost:$port/apps/$app/admin/permissions" | grep -q '"version":2'
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
if "$t60/release/sqlite-import" verify --dump "$work/dump" --data-dir "$work/tampered" --app-id "$app"; then
  echo "verify missed a tampered cell" >&2
  exit 1
fi
echo "migrate-alpha59: all checks passed"
