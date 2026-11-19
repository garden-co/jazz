# Shared helpers for the adopter gate. Source it: `. "$(dirname "$0")/lib.sh"`.
# Every check prints `PASS <name>` / `FAIL <name>` / `REVIEW <name>` / `SKIP <name>` lines; jobs
# tee them to $AG_RESULTS/<job>.txt for the verdict job, and exit non-zero on any FAIL.
set -u
AG=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
AG_RESULTS=${AG_RESULTS:-$AG/.results}; mkdir -p "$AG_RESULTS" "$AG/.work"
# RC inputs (see fetch-rc.sh): AG_TGZ holds the npm tarballs + NPM-SHA256SUMS, the core binary and language libs.
AG_TGZ=${AG_TGZ:-$AG/.rc}
ag_version() { cat "$AG_TGZ/VERSION"; }

kill_port() { local p; for p in "$@"; do for pid in $(ss -ltnpH "sport = :$p" 2>/dev/null | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u); do kill "$pid" 2>/dev/null; done; done; }
wait_port() { local i; for i in $(seq "${2:-60}"); do ss -ltn | grep -q ":$1 " && return 0; sleep 1; done; return 1; }

# Point the jazz deps of the package in the current dir at the RC tarballs (adds them if absent), pnpm-install,
# and print how many jazz lockfile entries are not the RC.
use_rc_pnpm() {
  local V; V=$(ag_version)
  node -e '
    const fs = require("fs"), [dir, v] = process.argv.slice(1), p = JSON.parse(fs.readFileSync("package.json"));
    p.dependencies ??= {};
    for (const n of ["jazz-tools", "jazz-wasm", "jazz-napi"]) p.dependencies[n] = `file:${dir}/${n}-${v}.tgz`;
    fs.writeFileSync("package.json", JSON.stringify(p, null, 2));' "$AG_TGZ" "$V"
  printf 'blockExoticSubdeps: false\nstrictDepBuilds: false\noverrides:\n  jazz-wasm: file:%s/jazz-wasm-%s.tgz\n  jazz-napi: file:%s/jazz-napi-%s.tgz\n  "@garden-co/jazz-napi-linux-x64-gnu": file:%s/garden-co-jazz-napi-linux-x64-gnu-%s.tgz\n' \
    "$AG_TGZ" "$V" "$AG_TGZ" "$V" "$AG_TGZ" "$V" > pnpm-workspace.yaml
  rm -rf node_modules pnpm-lock.yaml
  pnpm install --no-frozen-lockfile > install-rc.log 2>&1 || { echo "FAIL rc install ($(basename "$PWD"))"; tail -5 install-rc.log; return 1; }
  local stray; stray=$(grep -oE "(jazz-tools|jazz-wasm|jazz-napi[a-z0-9-]*)@2\.0[^:'( ]*" pnpm-lock.yaml | grep -vc "$V")
  [ "$stray" = 0 ] && echo "PASS lockfile all $V ($(basename "$PWD"))" || echo "FAIL lockfile has $stray non-RC jazz entries ($(basename "$PWD"))"
}

# Run a job body with its output teed to the results file; exit 1 if any FAIL line.
ag_job() { local name=$1; shift; "$@" 2>&1 | tee "$AG_RESULTS/$name.txt"
  # A job that checked nothing failed: a setup step broke before any check ran (it once passed silently).
  grep -qE "^(PASS|FAIL|REVIEW|SKIP) " "$AG_RESULTS/$name.txt" || echo "FAIL $name ran no checks (setup broke before the first check; see the log above)" | tee -a "$AG_RESULTS/$name.txt"
  ! grep -q "^FAIL" "$AG_RESULTS/$name.txt"; }
