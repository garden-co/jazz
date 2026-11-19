#!/usr/bin/env bash
# Expected refusals of `jazz-tools upgrade` (run only when the upgrade codemod changed): alpha.65-format apps that
# upgrade must refuse with a clear message and leave untouched. t7: a payload enum (names tickets.kind);
# t2: computed columns and transforms ("not supported yet").
. "$(dirname "$0")/lib.sh"
body() {
  local W=$AG/.work/refusals; rm -rf "$W"; mkdir -p "$W/cli"; cp -r "$AG/refusals/." "$W/"
  ( cd "$W/cli" && echo '{"name":"rc-cli","private":true}' > package.json && use_rc_pnpm >/dev/null ) || { echo "FAIL rc cli install"; return; }
  local CLI=$W/cli/node_modules/jazz-tools/bin/jazz-tools.js
  for spec in "t7-enum-cases:tickets.kind" "t2-computed:computed columns and transforms"; do
    local d=${spec%%:*} pat=${spec#*:}
    ( cd "$W/$d" && pnpm install > install.log 2>&1 || echo "FAIL $d install (alpha.65 deps)"
      local before; before=$(find . -path ./node_modules -prune -o -type f ! -name '*.log' -print0 | sort -z | xargs -0 sha256sum | sha256sum)
      node "$CLI" upgrade > upgrade.log 2>&1; local rc=$?
      local after; after=$(find . -path ./node_modules -prune -o -type f ! -name '*.log' -print0 | sort -z | xargs -0 sha256sum | sha256sum)
      [ $rc -ne 0 ] && echo "PASS $d upgrade refuses (exit $rc)" || echo "FAIL $d upgrade did not refuse"
      grep -q "$pat" upgrade.log && echo "PASS $d refusal names '$pat'" || { echo "FAIL $d refusal text:"; head -3 upgrade.log; }
      [ "$before" = "$after" ] && echo "PASS $d untouched" || echo "FAIL $d files changed by a refused upgrade" )
  done
}
ag_job refusals body
