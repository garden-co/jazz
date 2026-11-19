#!/usr/bin/env bash
# t9 language gate: a stock TS createJazz StagePlan backend on the RC tarballs (two local OIDC issuers: workloads 8790,
# users 8791), then one client language against it. Usage: t9.sh ts|rust|go|kotlin|swift
# StagePlan's app files and the client examples come from this checkout (the RC's commit in CI).
. "$(dirname "$0")/lib.sh"
REPO=$(cd "$AG/../.." && pwd); SP=$REPO/examples/stage-plan/apps
APP_ID=d1e92c0e-2b64-5e3b-9684-a6fb3205dc2b   # what the TS client derives from http://127.0.0.1:8780/jazz
backend() {
  local W=$AG/.work/t9; rm -rf "$W"; cp -r "$AG/t9" "$W"; cd "$W" && use_rc_pnpm || return 1
  mkdir -p stageplan && cp "$SP"/react-localfirst/jazz.{app.json,permissions.json,generated.ts} stageplan/   # resolve jazz-tools from the RC install
  kill_port 8780 8790 8791
  OIDC_PORT=8790 setsid npx tsx oidc.ts > oidc-w.log 2>&1 < /dev/null &
  OIDC_PORT=8791 setsid npx tsx oidc.ts > oidc-u.log 2>&1 < /dev/null &
  APP_DIR=$W/stageplan setsid npx tsx server.ts > server.log 2>&1 < /dev/null &
  wait_port 8790 && wait_port 8791 && wait_port 8780 && echo "PASS t9 backend up on the RC ($(node -p 'require("jazz-tools/package.json").version'))" \
    || { echo "FAIL t9 backend start"; tail -5 server.log; return 1; }
  for u in user-a user-b; do curl -sf -X POST "http://127.0.0.1:8780/account/$u" >/dev/null || echo "FAIL t9 createAccountFor $u"; done   # forUser needs accounts
}
body() {
  backend || return
  local W=$AG/.work/t9; cd "$W"
  case $1 in
    ts) APP_DIR=$W/stageplan npx tsx ts-backend-client.ts 2>&1 | grep -E "^(PASS|FAIL|BACKEND)" | sed 's/^BACKEND-CLIENT /t9 ts backend clients: /; s/^t9 ts backend clients: PASS/PASS t9 ts backend clients/; s/^t9 ts backend clients: FAIL/FAIL t9 ts backend clients/'
        SERVER=http://127.0.0.1:8780/jazz APP_BINDINGS=$W/stageplan/jazz.generated.ts APP_DIR=$W/stageplan npx tsx ts-end-users.ts 2>&1 | grep -E "^(PASS|FAIL|STAGEPLAN)" | sed 's/^STAGEPLAN PASS/PASS t9 ts end users/; s/^STAGEPLAN FAIL/FAIL t9 ts end users/' ;;
    rust) local RB=$REPO/target/debug/stage-plan
        if [ -n "${AG_REGISTRY:-}" ]; then   # a clean crate on crates.io's jazz-app-support, outside the workspace
          local C=$W/rs; rm -rf "$C"; cp -r "$SP/rust-cli" "$C"; rm -rf "$C/tests" "$C/target"
          python3 - "$C/Cargo.toml" "$(ag_version)" <<'PY'
import sys, re; p, v = sys.argv[1:]; s = open(p).read()
s = re.sub(r'jazz-app-support = \{[^}]*\}', f'jazz-app-support = "={v}"', s); s = s.split('[dev-dependencies]')[0] + '\n[workspace]\n'; open(p, 'w').write(s)
PY
          ( cd "$C" && cargo build -q ) > rust-build.log 2>&1 || { echo "FAIL t9 rust build from crates.io"; tail -5 rust-build.log; return; }
          grep -q 'path+' "$C/Cargo.lock" && echo "FAIL t9 rust: a path dependency in the lockfile" || echo "PASS t9 rust: only registry crates"
          RB=$C/target/debug/stage-plan
        else ( cd "$REPO" && cargo build -q -p stage-plan-rust-cli ) > rust-build.log 2>&1 || { echo "FAIL t9 rust build"; tail -5 rust-build.log; return; }; fi
        RUST_BIN=$RB APP_ID=$APP_ID ./run-lang-clients.sh 2>&1 | grep -E "^(PASS|FAIL)" ;;
    go) if [ -n "${AG_REGISTRY:-}" ]; then   # the public module proxy and sumdb, a clean module, no cgo, embedded lib
          local G=$W/gomod; rm -rf "$G"; mkdir -p "$G/jazzapp"; cp "$SP/go-cli/jazzapp/jazz_app.go" "$G/jazzapp/"; cp "$SP/go-cli/stageplan.go" "$SP/go-cli/jazz.permissions.json" "$AG/t9/go-gate/t9gate_test.go" "$G/"
          sed -i 's#github.com/garden-co/jazz/examples/stage-plan/apps/go-cli/jazzapp#example.com/app/jazzapp#' "$G/t9gate_test.go" "$G/stageplan.go"
          printf 'package main\nfunc main() {}\n' > "$G/main.go"; printf 'module example.com/app\n\ngo 1.22\n' > "$G/go.mod"
          ( cd "$G" && env -u GOPROXY -u GOFLAGS go get "github.com/garden-co/jazz/packages/jazz-go/v2@v$(ag_version)" && go mod tidy && CGO_ENABLED=0 go test -c -o gate.test . ) > go-build.log 2>&1 \
            && echo "PASS t9 go get from the public proxy + CGO_ENABLED=0 build" || { echo "FAIL t9 go get/build"; tail -5 go-build.log; return; }
          ( cd "$G" && env -u JAZZ_FFI_LIBRARY JAZZ_SERVER_URL=http://127.0.0.1:8780/jazz JAZZ_APP_ID=$APP_ID T9_WORKLOAD_ISSUER=http://127.0.0.1:8790 T9_USER_A=user-a T9_USER_B=user-b ./gate.test -test.v -test.run TestT9Gate ) 2>&1 | tee go.log \
            | grep -E "PASS|FAIL" | sed 's/^ *t9gate_test.go:[0-9]*: //; s/^--- PASS: /PASS t9 go /; s/^--- FAIL: /FAIL t9 go /' | grep -E "^(PASS|FAIL) "
          return; fi
        [ -f "$AG_TGZ/libjazz_ffi-linux-x86_64.tar.gz" ] || { echo "FAIL t9 go: no libjazz_ffi-linux-x86_64.tar.gz in the RC inputs"; return; }
        mkdir -p "$W/ffiart" && cp "$AG_TGZ/libjazz_ffi-linux-x86_64.tar.gz" "$W/ffiart/"
        JAZZ_APP_ID=$APP_ID ./go-rc-check.sh "$REPO" HEAD "$W/ffiart" "$(ag_version)" "$W/gomod" 2>&1 | tee go.log | grep -E "PASS|FAIL" | sed 's/^ *t9gate_test.go:[0-9]*: //; s/^--- PASS: /PASS t9 go /; s/^--- FAIL: /FAIL t9 go /' | grep -E "^(PASS|FAIL) " ;;
    kotlin) local K=$W/kt; rm -rf "$K"; cp -r "$SP/kotlin-cli" "$K"; rm -rf "$K/build" "$K/.gradle"
        local jar; jar=$(ls "$AG_TGZ"/jazz-kotlin-*.jar 2>/dev/null | grep -v android | head -1)
        if [ -n "${AG_REGISTRY:-}" ]; then   # Maven Central only: the one declared dependency, nothing else
          echo 'rootProject.name = "stage-plan-kotlin-cli"' > "$K/settings.gradle.kts"
          sed -i "s#implementation(\"tools.jazz:jazz-kotlin\")#implementation(\"tools.jazz:jazz-kotlin:$(ag_version)\")#; /tasks.named<JavaExec>(\"run\")/d" "$K/build.gradle.kts"
        elif [ -n "$jar" ]; then   # the release jar alone, with its declared Maven deps: what an adopter builds against
          echo 'rootProject.name = "stage-plan-kotlin-cli"' > "$K/settings.gradle.kts"
          python3 - "$K/build.gradle.kts" "$jar" <<'PY'
import sys, re; p, jar = sys.argv[1:]; s = open(p).read()
s = s.replace('implementation("tools.jazz:jazz-kotlin")', f'''implementation(files("{jar}"))
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.9.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("net.java.dev.jna:jna:5.17.0")''')
s = re.sub(r'tasks\.named<JavaExec>\("run"\).*\n', '', s); open(p, 'w').write(s)
PY
        else echo "note: no release jar; building jazz-kotlin from source"; fi
        ( cd "$K" && gradle installDist --no-daemon -q ) > kt-build.log 2>&1 || { echo "FAIL t9 kotlin build"; tail -8 kt-build.log; return; }
        KT_CLI=$K/build/install/stage-plan-kotlin-cli/bin/stage-plan-kotlin-cli APP_ID=$APP_ID ./run-lang-clients.sh 2>&1 | grep -E "^(PASS|FAIL)" ;;
    swift) [ -f "$AG_TGZ/libjazz_ffi-linux-x86_64.tar.gz" ] || { echo "FAIL t9 swift: no libjazz_ffi-linux-x86_64.tar.gz in the RC inputs"; return; }
        local L=$W/ffi; mkdir -p "$L" && tar xzf "$AG_TGZ/libjazz_ffi-linux-x86_64.tar.gz" -C "$L"
        printf 'prefix=%s\nName: jazz_ffi\nDescription: Jazz core (release libjazz_ffi)\nVersion: %s\nLibs: -L${prefix} -ljazz_ffi\nCflags: -I${prefix}/include\n' "$L" "$(ag_version)" > "$L/jazz_ffi.pc"
        local SWD=$SP/swift-cli
        if [ -n "${AG_REGISTRY:-}" ]; then   # the jazz-swift mirror at this exact version, outside the repo
          SWD=$W/sw; rm -rf "$SWD"; cp -r "$SP/swift-cli" "$SWD"; rm -rf "$SWD/.build" "$SWD/Tests"
          sed -i "s#.package(name: \"jazz\", path: \"../../../..\")#.package(url: \"https://github.com/garden-co/jazz-swift\", exact: \"$(ag_version)\")#; s#package: \"jazz\")#package: \"jazz-swift\")#; /testTarget/d" "$SWD/Package.swift"
        fi
        ( cd "$SWD" && PKG_CONFIG_PATH=$L swift build ) > sw-build.log 2>&1 || { echo "FAIL t9 swift build"; tail -8 sw-build.log; return; }
        LD_LIBRARY_PATH=$L${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH} SW_BIN=$SWD/.build/debug/stage-plan-swift APP_ID=$APP_ID ./run-lang-clients.sh 2>&1 | grep -E "^(PASS|FAIL)" ;;
    *) echo "FAIL unknown language $1" ;;
  esac
  kill_port 8780 8790 8791
}
ag_job "t9-$1" body "$1"
