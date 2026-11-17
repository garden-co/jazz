#!/usr/bin/env bash
# Point the t9 TS backend at an rc's npm tarballs + that tag's StagePlan app files, then restart it fresh.
# Usage: rc-backend.sh <version e.g. 2.0.0-alpha.66-rc.1> <jazz repo for the tag>
set -eu; V=$1; R=$2; D=/vercel/sandbox/rc/$V/npm; HERE=$(cd "$(dirname "$0")" && pwd)
rm -rf $D && mkdir -p $D/stageplan && cd $D && echo '{"name":"t9rc","private":true,"type":"module"}' > package.json
npm install "https://github.com/garden-co/jazz/releases/download/v$V/jazz-tools-$V.tgz" ws@8 >/dev/null 2>&1
node -e 'console.log("jazz-tools", require("./node_modules/jazz-tools/package.json").version)'
git -C $R fetch -q origin tag v$V && git -C $R archive v$V examples/stage-plan/apps/react-localfirst | tar -x -C $D/stageplan --strip-components=4
ln -sfn $D/node_modules/jazz-tools $HERE/node_modules/jazz-tools
for pid in $(ss -ltnpH "sport = :8780" | grep -o 'pid=[0-9]*' | cut -d= -f2); do kill $pid; done; sleep 1; rm -rf $HERE/data
APP_DIR=$D/stageplan /vercel/sandbox/g65/t9-up.sh; sleep 2; tail -n 1 /tmp/t9srv.log
cd $HERE && APP_DIR=$D/stageplan npx tsx ts-backend-client.ts 2>&1 | tail -1
SERVER=http://127.0.0.1:8780/jazz APP_BINDINGS=$D/stageplan/jazz.generated.ts APP_DIR=$D/stageplan npx tsx ts-end-users.ts 2>&1 | tail -1
