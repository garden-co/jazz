#!/usr/bin/env bash
# t1's fixture stores (written by older releases, in bed/t1-migrations/fixture-data*) must open on the RC with the
# upgraded app, and keep their identity: since alpha.68 (jazz#5570) store_id/app_id live in jazz.sqlite's
# _jazz_host_identity and must equal the old files, which stay as unchanged mirrors.
. "$(dirname "$0")/lib.sh"
body() {
  local W=$AG/.work/fixtures; rm -rf "$W"; mkdir -p "$W"; cp -r "$AG/bed/_common" "$AG/bed/t1-migrations" "$W/"
  cd "$W/t1-migrations" && use_rc_pnpm >/dev/null || echo "FAIL rc install"
  for fx in fixture-data*; do
    FX=$fx ./fixture-check.sh | grep -E "^(PASS|FAIL)" | sed "s/^PASS existing fixture store opens.*/PASS $fx opens on the RC (alice reads her task)/; s/^FAIL /FAIL $fx: /"
    local db=/tmp/t1fx/jazz/jazz.sqlite q="select cast(v as text) from _jazz_host_identity where cast(k as text)="
    for k in store_id:store-id app_id:app-id; do local col=${k%%:*} file=${k##*:}
      local want got; want=$(cat "$fx/jazz/$file"); got=$(sqlite3 "$db" "$q'$col'" 2>/dev/null)
      [ "$got" = "$want" ] && echo "PASS $fx $col kept in jazz.sqlite" || echo "FAIL $fx $col in jazz.sqlite '$got', file '$want'"; done
    [ "$(cat /tmp/t1fx/jazz/store-id)" = "$(cat "$fx/jazz/store-id")" ] && echo "PASS $fx store-id mirror unchanged" || echo "FAIL $fx store-id mirror changed"
  done
}
ag_job fixtures body
