#!/usr/bin/env bash
# Go module check for an rc without a pushed tag (per #release):
#   go-rc-check.sh <jazz repo> <rc commit> <dir with libjazz_ffi-<os>-<arch>.tar.gz> <version e.g. 2.0.0-alpha.66-rc.1> <work dir>
# Embeds the libs in a checkout of the rc commit, serves packages/jazz-go from a local GOPROXY (like go-module-tag.yml's dry run),
# then in a clean module outside the repo: go get, CGO_ENABLED=0 build of the t9 Go gate, run with no JAZZ_FFI_LIBRARY against t9.
set -euo pipefail; R=$1; C=$2; A=$3; V=$4; W=$5; MOD=github.com/garden-co/jazz/packages/jazz-go/v2
HERE=$(cd "$(dirname "$0")" && pwd); export PATH=/vercel/sandbox/go/bin:$PATH
rm -rf $W && mkdir -p $W && git -C $R worktree add -f --detach $W/src $C >/dev/null 2>&1
( cd $W/src && dev/scripts/go-embed-ffi-libraries.sh $A )
python3 - $W/proxy v$V $W/src/packages/jazz-go <<'PY'
import json, os, sys, zipfile
proxy, ver, src = sys.argv[1:4]; mod = "github.com/garden-co/jazz/packages/jazz-go/v2"
d = os.path.join(proxy, mod, "@v"); os.makedirs(d)
open(os.path.join(d, "list"), "w").write(ver + "\n"); open(os.path.join(d, ver + ".info"), "w").write(json.dumps({"Version": ver}))
open(os.path.join(d, ver + ".mod"), "w").write(open(os.path.join(src, "go.mod")).read())
with zipfile.ZipFile(os.path.join(d, ver + ".zip"), "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, files in os.walk(src):
        for f in files: full = os.path.join(root, f); z.write(full, f"{mod}@{ver}/" + os.path.relpath(full, src))
print("module zip MiB", round(os.path.getsize(os.path.join(d, ver + ".zip")) / 2**20, 1))
PY
app=$W/app; mkdir -p $app/jazzapp
cp $W/src/examples/stage-plan/apps/go-cli/{jazzapp/jazz_app.go,stageplan.go,jazz.permissions.json} $app/ 2>/dev/null || true
mv $app/jazz_app.go $app/jazzapp/; cp $HERE/go-gate/t9gate_test.go $app/
sed -i 's#github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp#example.com/app/jazzapp#' $app/t9gate_test.go $app/stageplan.go
printf 'package main\nfunc main() {}\n' > $app/main.go
printf 'module example.com/app\n\ngo 1.22\n' > $app/go.mod
cd $app && export GOPROXY="file://$W/proxy,https://proxy.golang.org" GOFLAGS=-mod=mod GONOSUMDB=github.com/garden-co GOSUMDB=off
go get "$MOD@v$V" && grep "jazz-go" go.mod
CGO_ENABLED=0 go test -c -o gate.test . && echo "PASS go get + CGO_ENABLED=0 build from the local proxy"
cd $app && env -u JAZZ_FFI_LIBRARY JAZZ_SERVER_URL=${JAZZ_SERVER_URL:-http://127.0.0.1:8780/jazz} JAZZ_APP_ID=${JAZZ_APP_ID:?} T9_WORKLOAD_ISSUER=http://127.0.0.1:8790 T9_USER_A=user-a T9_USER_B=user-b $app/gate.test -test.v -test.run TestT9Gate 2>&1 | grep -E "PASS|FAIL|ok|t9gate|note" 
