import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";

// Node --test isolates this worker; create every fixture after dropping real root privileges.
if (process.getuid() === 0) {
  const nobodyGroup = spawnSync("id", ["-g", "nobody"], { encoding: "utf8" });
  assert.equal(nobodyGroup.status, 0, nobodyGroup.error?.message ?? nobodyGroup.stderr);
  const gid = nobodyGroup.stdout.trim();
  assert.match(gid, /^\d+$/, "nobody must have a numeric primary GID");
  assert.ok(Number.isSafeInteger(Number(gid)), "nobody primary GID must be representable");
  process.setgroups([]);
  process.setgid(Number(gid));
  process.setuid("nobody");
}

const SCRIPT = path.resolve(new URL("./bootstrap_runner.sh", import.meta.url).pathname);
const HELPER = path.resolve(new URL("./bootstrap_runner_helper.py", import.meta.url).pathname);
const ORCHESTRATION = path.resolve(
  new URL("./bootstrap_runner_orchestration.sh", import.meta.url).pathname,
);

function temporaryDirectory(prefix) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function helper(root, ...args) {
  return spawnSync("python3", [HELPER, ...args], { cwd: root, encoding: "utf8" });
}
function makeRemovable(directory) {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    if (entry.isDirectory()) makeRemovable(path.join(directory, entry.name));
  }
  fs.chmodSync(directory, 0o700);
}

test("pinned download verification accepts only the exact present digest", () => {
  const root = temporaryDirectory("jazz-bootstrap-digest-");
  const artifact = path.join(root, "artifact");
  const bytes = Buffer.from("publisher-approved bytes");
  const expected = crypto.createHash("sha256").update(bytes).digest("hex");
  fs.writeFileSync(artifact, bytes);

  try {
    assert.equal(helper(root, "verify-download", artifact, expected).status, 0);
    assert.notEqual(helper(root, "verify-download", artifact, "0".repeat(64)).status, 0);
    assert.notEqual(
      helper(root, "verify-download", path.join(root, "missing"), expected).status,
      0,
    );
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("runner manifest detects modified, missing, and unmanifested package files", () => {
  const root = temporaryDirectory("jazz-bootstrap-manifest-");
  const packageDir = path.join(root, "runner");
  const manifest = path.join(packageDir, ".bootstrap-manifest");
  fs.mkdirSync(path.join(packageDir, "bin"), { recursive: true });
  fs.writeFileSync(path.join(packageDir, "config.sh"), "verified config\n");
  fs.writeFileSync(path.join(packageDir, "bin", "Runner.Listener"), "verified listener\n");

  fs.chmodSync(path.join(packageDir, "bin"), 0o755);
  fs.chmodSync(path.join(packageDir, "config.sh"), 0o644);
  fs.chmodSync(path.join(packageDir, "bin", "Runner.Listener"), 0o644);
  try {
    assert.equal(helper(root, "manifest", packageDir, manifest).status, 0);
    assert.equal(
      fs.readFileSync(manifest, "utf8"),
      fs.readFileSync(new URL("./bootstrap_runner_manifest_v1.json", import.meta.url), "utf8"),
      "format-1 manifest bytes remain canonical",
    );
    const verified = helper(root, "verify", packageDir, manifest);
    assert.equal(verified.status, 0, verified.stderr);
    fs.writeFileSync(path.join(packageDir, "config.sh"), "changed config\n");
    assert.notEqual(
      helper(root, "verify", packageDir, manifest).status,
      0,
      "modified file is rejected",
    );
    fs.writeFileSync(path.join(packageDir, "config.sh"), "verified config\n");
    fs.unlinkSync(path.join(packageDir, "bin", "Runner.Listener"));
    assert.notEqual(
      helper(root, "verify", packageDir, manifest).status,
      0,
      "partial package is rejected",
    );
    fs.writeFileSync(path.join(packageDir, "bin", "Runner.Listener"), "verified listener\n");
    fs.writeFileSync(path.join(packageDir, "unexpected"), "unmanifested\n");
    assert.notEqual(
      helper(root, "verify", packageDir, manifest).status,
      0,
      "extra package file is rejected",
    );
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("archive extraction rejects traversal and refuses a nonempty staging directory", () => {
  const root = temporaryDirectory("jazz-bootstrap-archive-");
  const archive = path.join(root, "unsafe.tar.gz");
  const staging = path.join(root, "staging");
  const outside = path.join(root, "escaped");
  const createUnsafeArchive = spawnSync(
    "python3",
    [
      "-c",
      "import io,tarfile; t=tarfile.open(%r,'w:gz'); m=tarfile.TarInfo('../escaped'); b=b'no'; m.size=len(b); t.addfile(m,io.BytesIO(b)); t.close()".replace(
        "%r",
        JSON.stringify(archive),
      ),
    ],
    { encoding: "utf8" },
  );
  assert.equal(createUnsafeArchive.status, 0, createUnsafeArchive.stderr);

  try {
    fs.mkdirSync(staging);
    fs.writeFileSync(path.join(staging, "keep"), "unchanged");
    const occupied = helper(root, "extract", archive, staging);
    assert.notEqual(occupied.status, 0, "extractor requires an empty staging directory");
    assert.equal(fs.readFileSync(path.join(staging, "keep"), "utf8"), "unchanged");
    fs.rmSync(staging, { recursive: true, force: true });
    const traversal = helper(root, "extract", archive, staging);
    assert.notEqual(traversal.status, 0, "path traversal archive is rejected");
    assert.equal(fs.existsSync(outside), false, "rejected archive created no file outside staging");
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("archive extraction accepts conventional directories and rejects file trailing slashes", () => {
  const root = temporaryDirectory("jazz-bootstrap-directory-slash-");
  const archive = path.join(root, "directory.tar.gz");
  const staging = path.join(root, "staging");
  const source = `import io, tarfile
t = tarfile.open(${JSON.stringify(archive)}, "w:gz")
directory = tarfile.TarInfo("bin/")
directory.type = tarfile.DIRTYPE
t.addfile(directory)
payload = tarfile.TarInfo("bin/tool")
payload.size = 4
t.addfile(payload, io.BytesIO(b"safe"))
t.close()`;
  const created = spawnSync("python3", ["-c", source], { encoding: "utf8" });
  assert.equal(created.status, 0, created.stderr);

  try {
    const extracted = helper(root, "extract", archive, staging);
    assert.equal(extracted.status, 0, extracted.stderr);
    assert.equal(fs.readFileSync(path.join(staging, "bin", "tool"), "utf8"), "safe");

    const fileArchive = path.join(root, "file-slash.tar.gz");
    const fileSource = `import io, tarfile
t = tarfile.open(${JSON.stringify(fileArchive)}, "w:gz")
payload = tarfile.TarInfo("file/")
payload.size = 4
t.addfile(payload, io.BytesIO(b"evil"))
t.close()`;
    const fileCreated = spawnSync("python3", ["-c", fileSource], { encoding: "utf8" });
    assert.equal(fileCreated.status, 0, fileCreated.stderr);
    const rejected = helper(root, "extract", fileArchive, path.join(root, "file-staging"));
    assert.notEqual(rejected.status, 0, "trailing slash is not normalized for regular files");
    assert.equal(fs.existsSync(path.join(root, "file-staging", "file")), false);
  } finally {
    makeRemovable(root);
    fs.rmSync(root, { recursive: true, force: true });
  }
});
test("archive extraction rejects a file beneath a symlink parent escape", () => {
  const root = temporaryDirectory("jazz-bootstrap-link-archive-");
  const archive = path.join(root, "symlink-parent.tar.gz");
  const staging = path.join(root, "staging");
  const escapedFile = path.join(root, "outside", "pwn");
  const source = `import io, tarfile
t = tarfile.open(${JSON.stringify(archive)}, "w:gz")
for name in ("a", "x"):
    directory = tarfile.TarInfo(name)
    directory.type = tarfile.DIRTYPE
    t.addfile(directory)
for name, target in (("a/b", "../x"), ("a/b/link", "../../outside")):
    link = tarfile.TarInfo(name)
    link.type = tarfile.SYMTYPE
    link.linkname = target
    t.addfile(link)
payload = tarfile.TarInfo("a/b/link/pwn")
payload.size = 2
t.addfile(payload, io.BytesIO(b"no"))
t.close()`;
  const created = spawnSync("python3", ["-c", source], { encoding: "utf8" });
  assert.equal(created.status, 0, created.stderr);

  try {
    const result = helper(root, "extract", archive, staging);
    assert.notEqual(
      result.status,
      0,
      "extractor rejects members whose parents traverse archive symlinks",
    );
    assert.equal(
      fs.existsSync(escapedFile),
      false,
      "rejected archive wrote nothing outside its staging root",
    );
  } finally {
    makeRemovable(root);
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("archive extraction preserves a valid internal leaf symlink", () => {
  const root = temporaryDirectory("jazz-bootstrap-leaf-link-");
  const archive = path.join(root, "leaf-symlink.tar.gz");
  const staging = path.join(root, "staging");
  const source = `import io, tarfile
t = tarfile.open(${JSON.stringify(archive)}, "w:gz")
payload = tarfile.TarInfo("target")
payload.size = 4
t.addfile(payload, io.BytesIO(b"safe"))
link = tarfile.TarInfo("alias")
link.type = tarfile.SYMTYPE
link.linkname = "target"
t.addfile(link)
t.close()`;
  const created = spawnSync("python3", ["-c", source], { encoding: "utf8" });
  assert.equal(created.status, 0, created.stderr);
  try {
    const result = helper(root, "extract", archive, staging);
    assert.equal(result.status, 0, result.stderr);
    assert.equal(fs.readlinkSync(path.join(staging, "alias")), "target");
    assert.equal(fs.readFileSync(path.join(staging, "alias"), "utf8"), "safe");
  } finally {
    makeRemovable(root);
    fs.rmSync(root, { recursive: true, force: true });
  }
});

function serviceUnitName(url, runnerName) {
  const canonicalUrl = url.replace(/\/$/, "").toLowerCase();
  const digest = crypto.createHash("sha256").update(`${canonicalUrl}\0${runnerName}`).digest("hex");
  return `jazz-benchmark-runner-${digest}.service`;
}

function serviceUnitContents({ user, pkg, runnerName = "fixture-runner" }) {
  return `[Unit]
Description=Jazz benchmark runner ${runnerName}

[Service]
User=${user}
WorkingDirectory=${pkg}
ExecStart=${pkg}/runsvc.sh

KillSignal=SIGTERM
TimeoutStopSec=5min

[Install]
WantedBy=multi-user.target
`;
}

function commandTrace(name) {
  return `trace_name=${name}
trace_args=""
trace_separator=""
trace_redact_next=0
for trace_arg do
  if [ "$trace_name" = runuser ]; then
    case "$trace_arg" in */config.sh) trace_name=runuser-config ;; esac
  fi
  if [ "$trace_redact_next" = 1 ]; then
    trace_arg="[redacted]"
    trace_redact_next=0
  else
    case "$trace_arg" in
      --token) printf '%s-token-argv-present\\n' "$trace_name" >> "$TRACE"; trace_redact_next=1 ;;
      ACTIONS_RUNNER_INPUT_TOKEN=*|*fixture-token*)
        printf '%s-token-argv-present\\n' "$trace_name" >> "$TRACE"
        trace_arg="[redacted]"
        ;;
    esac
  fi
  trace_args="$trace_args$trace_separator$trace_arg"
  trace_separator=" "
done
printf '%s:%s\\n' '${name}' "$trace_args" >> "$TRACE"
if [ "$trace_name" != config ] && [ "\${FIXTURE_CONFIG_CHILD:-}" = 1 ]; then
  trace_name="$trace_name-config-child"
fi
if [ "\${ACTIONS_RUNNER_INPUT_TOKEN+x}" = x ]; then
  printf '%s-token-present\\n' "$trace_name" >> "$TRACE"
fi`;
}

function bootstrapFixture({
  rawPackage = true,
  runnerUrl = "https://github.com/garden-co/jazz2",
  runnerName = "fixture-runner",
} = {}) {
  assert.notEqual(process.getuid(), 0, "sandboxed bootstrap tests must run unprivileged");
  const root = temporaryDirectory("jazz-bootstrap-entry-");
  assert.deepEqual(
    fs.readdirSync(root),
    [],
    "each invocation starts from a fresh harness-created root",
  );
  const trace = path.join(root, "trace.log");
  const runner = os.userInfo().username;
  const runnerUid = process.getuid();
  const runnerGid = process.getgid();
  const home = path.join(root, "home", runner);
  const state = path.join(root, "var", "lib", "actions-runner", runner);
  const pkg = path.join(root, "opt", "actions-runner", "2.337.0");
  const unitDir = path.join(root, "etc", "systemd", "system");
  const loadedUnitDir = path.join(root, "systemd-loaded");
  const unitName = serviceUnitName(runnerUrl, runnerName);
  const unitFile = path.join(unitDir, unitName);
  const cgroup = path.join(root, "sys", "fs", "cgroup", "system.slice", unitName);
  const activeState = path.join(root, "systemd-active");
  fs.mkdirSync(unitDir, { recursive: true });
  fs.mkdirSync(cgroup, { recursive: true });
  fs.writeFileSync(path.join(cgroup, "cgroup.events"), "populated 0\nfrozen 0\n");
  fs.writeFileSync(activeState, "active\n");
  fs.writeFileSync(unitFile, serviceUnitContents({ user: runner, pkg, runnerName }), {
    mode: 0o644,
  });
  fs.mkdirSync(loadedUnitDir);
  fs.copyFileSync(unitFile, path.join(loadedUnitDir, unitName));
  fs.mkdirSync(path.join(home, ".cargo", "bin"), { recursive: true });
  fs.mkdirSync(path.join(root, "var", "tmp"), { recursive: true });
  fs.mkdirSync(path.join(root, "dev"), { recursive: true });
  fs.writeFileSync(path.join(root, "dev", "null"), "");
  fs.mkdirSync(state, { recursive: true });
  fs.mkdirSync(path.join(pkg, "bin"), { recursive: true });
  fs.writeFileSync(
    path.join(pkg, "config.sh"),
    `#!/bin/sh
${commandTrace("config")}
export FIXTURE_CONFIG_CHILD=1
printf 'config-cwd:%s\\n' "$PWD" >> "$TRACE"
if [ "\${ALLOW_CONFIG:-}" = 1 ]; then
  printf '{"agentName":"%s","serverUrl":"%s","workFolder":"_work"}\\n' "$RUNNER_NAME" "$RUNNER_URL" > "$BOOTSTRAP_FIXTURE_STATE/.runner"
  printf 'fixture credentials\\n' > "$BOOTSTRAP_FIXTURE_STATE/.credentials"
  if [ "\${GENERATE_SVC:-}" = 1 ]; then
    printf 'config-package-mode:%s\\n' "$(stat -c %a "$PWD")" >> "$TRACE"
    printf '#!/bin/sh\\nprintf executed > "%s"\\n' "$GENERATED_SVC_EXECUTED" > "$PWD/svc.sh"
    chmod 0755 "$PWD/svc.sh"
  fi
  if [ "\${FAIL_CONFIG_AFTER_SVC:-}" = 1 ]; then exit 91; fi
  if [ "\${INTERRUPT_CONFIG:-}" = 1 ] || [ "\${CONFIG_HANDOFF:-}" = 1 ]; then
    if [ -n "\${CONFIG_PROCESS_PID_FILE:-}" ]; then
      printf '%s\\n' "$$" > "$CONFIG_PROCESS_PID_FILE"
    fi
    printf 'config-blocked\\n' > "$CONFIG_BLOCKED_MARKER"
    if [ "\${INTERRUPT_CONFIG:-}" = 1 ]; then kill -TERM "$(cat "$BOOTSTRAP_PID_FILE")"; fi
    while [ ! -f "$CONFIG_RELEASE_MARKER" ]; do sleep 0.01; done
    printf 'config-after-signal\\n' > "$CONFIG_AFTER_SIGNAL_MARKER"
    if [ -n "\${CONFIG_AFTER_SIGNAL_STATE:-}" ]; then
      printf 'written after cancellation\\n' > "$CONFIG_AFTER_SIGNAL_STATE"
    fi
  fi
  exit 0
fi
exit 91
`,
    { mode: 0o755 },
  );
  if (!rawPackage) {
    fs.writeFileSync(
      path.join(pkg, "svc.sh"),
      `#!/bin/sh
printf 'svc:%s\\nsvc-cwd:%s\\n' "$*" "$PWD" >> "$TRACE"
`,
      { mode: 0o755 },
    );
  }
  fs.writeFileSync(path.join(pkg, "bin", "Runner.Listener"), "pinned runner fixture");
  fs.writeFileSync(path.join(pkg, "runsvc.sh"), "#!/bin/sh\nexit 0\n", { mode: 0o755 });
  fs.writeFileSync(
    path.join(state, ".runner"),
    JSON.stringify({
      agentName: runnerName,
      serverUrl: runnerUrl,
      workFolder: "_work",
    }),
  );
  fs.writeFileSync(path.join(state, ".credentials"), "fixture credentials");
  for (const file of [".runner", ".credentials"]) {
    fs.chmodSync(path.join(state, file), 0o600);
  }
  fs.chmodSync(state, 0o2700);
  for (const item of [
    ".runner",
    ".credentials",
    ...(rawPackage ? [] : [".service"]),
    ".credentials_rsaparams",
    ".env",
    ".path",
    "_work",
    "_diag",
    "_temp",
  ]) {
    fs.symlinkSync(path.join(state, item), path.join(pkg, item));
  }
  fs.chmodSync(path.join(pkg, "bin"), 0o555);
  for (const file of [
    path.join(pkg, "config.sh"),
    ...(rawPackage ? [] : [path.join(pkg, "svc.sh")]),
    path.join(pkg, "bin", "Runner.Listener"),
    path.join(pkg, "runsvc.sh"),
  ]) {
    fs.chmodSync(file, 0o555);
  }
  const manifest = helper(
    root,
    "manifest",
    pkg,
    path.join(pkg, ".bootstrap-manifest"),
    "--exclude-runtime-links",
  );
  assert.equal(manifest.status, 0, manifest.stderr);
  fs.chmodSync(path.join(pkg, ".bootstrap-manifest"), 0o444);
  fs.chmodSync(pkg, 0o555);
  const nodeRoot = path.join(root, "opt", "node-v24.13.0");
  const node = path.join(nodeRoot, "bin");
  fs.mkdirSync(node, { recursive: true });
  fs.chmodSync(nodeRoot, 0o755);
  fs.chmodSync(node, 0o755);
  fs.writeFileSync(
    path.join(node, "node"),
    `#!/bin/sh\nprintf executed > '${path.join(root, "node-executed")}'\necho v24.13.0\n`,
    { mode: 0o755 },
  );
  const nodeManifest = helper(
    root,
    "manifest",
    nodeRoot,
    path.join(nodeRoot, ".bootstrap-manifest"),
  );
  assert.equal(nodeManifest.status, 0, nodeManifest.stderr);
  fs.chmodSync(path.join(nodeRoot, ".bootstrap-manifest"), 0o444);
  const rustup = path.join(home, ".cargo", "bin", "rustup");
  fs.writeFileSync(
    rustup,
    "#!/bin/sh\ncase \"$*\" in *'toolchain list'*) echo 1.93.1;; *'target list'*) echo wasm32-unknown-unknown;; *) exit 92;; esac\n",
    { mode: 0o755 },
  );
  fs.writeFileSync(
    path.join(home, ".cargo", "bin", "wasm-pack"),
    "#!/bin/sh\necho 'wasm-pack 0.13.1'\n",
    { mode: 0o755 },
  );
  const rustupHome = path.join(home, ".rustup");
  fs.mkdirSync(path.join(rustupHome, "toolchains", "1.93.1", "lib"), { recursive: true });
  fs.writeFileSync(path.join(rustupHome, "settings.toml"), 'default_toolchain = "1.93.1"\n');
  fs.writeFileSync(
    path.join(rustupHome, "toolchains", "1.93.1", "lib", "toolchain"),
    "pinned toolchain fixture\n",
  );
  const toolchainManifestDir = path.join(
    root,
    "var",
    "lib",
    "actions-runner",
    ".toolchain-integrity",
  );
  fs.mkdirSync(toolchainManifestDir, { recursive: true });
  for (const [name, directory] of [
    ["cargo-bin", path.join(home, ".cargo", "bin")],
    ["rustup-home", rustupHome],
  ]) {
    const manifestPath = path.join(toolchainManifestDir, `${name}.json`);
    const manifest = helper(root, "manifest", directory, manifestPath);
    assert.equal(manifest.status, 0, manifest.stderr);
    fs.chmodSync(manifestPath, 0o444);
  }
  fs.chmodSync(toolchainManifestDir, 0o755);
  fs.writeFileSync(path.join(root, "var", "lib", "actions-runner", ".os-dependencies-v1"), "");
  for (const directory of [
    path.join(root, "opt"),
    path.join(root, "opt", "actions-runner"),
    path.join(root, "var"),
    path.join(root, "var", "lib"),
    path.join(root, "var", "lib", "actions-runner"),
  ]) {
    fs.chmodSync(directory, 0o755);
  }

  fs.writeFileSync(path.join(root, ".bootstrap-test-fixture"), "BUG-117-fixture-v1\n");

  const bin = temporaryDirectory("jazz-bootstrap-stubs-");
  const logger = (name, body) => {
    fs.writeFileSync(path.join(bin, name), `#!/bin/sh\n${commandTrace(name)}\n${body}\n`, {
      mode: 0o755,
    });
  };
  logger(
    "getent",
    `case "$*" in "passwd $RUNNER_USER") printf '%s:x:%s:%s::%s:/bin/bash\\n' "$RUNNER_USER" "\${FIXTURE_UID:-${runnerUid}}" "\${FIXTURE_GID:-${runnerGid}}" "$BOOTSTRAP_TEST_ROOT/home/$RUNNER_USER" ;; *) exit 90 ;; esac`,
  );
  fs.writeFileSync(
    path.join(bin, "id"),
    `#!/bin/sh
case "$1:$2" in
  -u:$RUNNER_USER) printf '%s\\n' "\${FIXTURE_UID:-${runnerUid}}" ;;
  -g:$RUNNER_USER) printf '%s\\n' "\${FIXTURE_GID:-${runnerGid}}" ;;
  *) exec /usr/bin/id "$@" ;;
esac
`,
    { mode: 0o755 },
  );
  logger("useradd", "exit 90");
  logger(
    "curl",
    `case "\${CURL_MODE:-}" in missing) exit 0 ;; *) while [ "$#" -gt 0 ]; do if [ "$1" = --output ]; then shift; case "$1" in "$BOOTSTRAP_TEST_ROOT"/*) printf bad-download > "$1"; exit 0 ;; *) exit 90 ;; esac; fi; shift; done; exit 90 ;; esac`,
  );
  logger(
    "apt-get",
    'case "$*" in update|"install -y build-essential"*) ;; "install -y snapd") ;; *) exit 90 ;; esac; [ "\${FAIL_EFFECT:-}" != "apt-get:$*" ]',
  );
  logger(
    "runuser",
    'while [ "$#" -gt 0 ] && [ "$1" != env ]; do shift; done; [ "$#" -gt 0 ] || exit 90; shift; tool=""; for arg do case "$arg" in *=*) ;; *) tool="$arg"; break ;; esac; done; case "$tool" in rustup|wasm-pack|cargo|python3|*/.cargo/bin/rustup|*/.cargo/bin/wasm-pack|*/config.sh) exec /usr/bin/env "$@" ;; *) exit 90 ;; esac',
  );
  logger(
    "systemctl",
    `case "$1" in
  list-units)
    plain=0
    for arg do [ "$arg" != --plain ] || plain=1; done
    state="$(cat "$SYSTEMD_ACTIVE_STATE")"
    case "$state" in
      active) substate=running; prefix="  " ;;
      failed) substate=failed; prefix="● " ;;
      *) substate=dead; prefix="  " ;;
    esac
    [ "$plain" != 1 ] || prefix=""
    for unit in "$SYSTEMD_LOADED_UNIT_DIR"/*.service; do
      [ -f "$unit" ] || continue
      printf '%s%s loaded %s %s Jazz benchmark runner\\n' "$prefix" "$(basename "$unit")" "$state" "$substate"
    done
    exit 0
    ;;
  list-unit-files)
    for unit in "$SYSTEMD_UNIT_DIR"/*.service; do
      [ -f "$unit" ] || continue
      printf '%s enabled enabled\\n' "$(basename "$unit")"
    done
    exit 0
    ;;
  show)
    shift
    unit=""
    properties=""
    value_only=0
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --property=*) properties="\${1#*=}" ;;
        --property) shift; properties="$1" ;;
        --value) value_only=1 ;;
        --*) ;;
        *) unit="$1" ;;
      esac
      shift
    done
    [ -n "$unit" ] || exit 1
    definition="$SYSTEMD_LOADED_UNIT_DIR/$unit"
    if [ ! -f "$definition" ]; then
      [ -f "$SYSTEMD_UNIT_DIR/$unit" ] || exit 1
      cp "$SYSTEMD_UNIT_DIR/$unit" "$definition"
    fi
    old_ifs="$IFS"; IFS=,
    for property in $properties; do
      case "$property" in
        User|WorkingDirectory|KillSignal)
          result="$(awk -F= -v key="$property" '$1 == key { print substr($0, index($0, "=") + 1); exit }' "$definition")"
          ;;
        ExecStart)
          executable="$(awk -F= '$1 == "ExecStart" { print substr($0, index($0, "=") + 1); exit }' "$definition")"
          if [ -n "$executable" ]; then
            result="{ path=\${executable} ; argv[]=\${executable} ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }"
          else
            result=""
          fi
          ;;
        ExecStartPre|ExecStartPost|ExecStop|ExecStopPost)
          if [ "$property" = "\${SYSTEMD_LOADED_HOOK:-}" ]; then
            result="{ path=/bin/false ; argv[]=/bin/false ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }"
          else
            result=""
          fi
          ;;
        KillMode)
          result="$(awk -F= -v key="$property" '$1 == key { print substr($0, index($0, "=") + 1); exit }' "$definition")"
          [ -n "$result" ] || result=control-group
          ;;
        TimeoutStopUSec)
          timeout="$(awk -F= '$1 == "TimeoutStopSec" { print $2; exit }' "$definition")"
          case "$timeout" in 5min) result=300000000 ;; *) result= ;; esac
          ;;
        ControlGroup) result="/system.slice/$unit" ;;
        ActiveState|SubState) result="$(cat "$SYSTEMD_ACTIVE_STATE")" ;;
        *) result= ;;
      esac
      if [ "$value_only" = 1 ]; then printf '%s\\n' "$result"; else printf '%s=%s\\n' "$property" "$result"; fi
    done
    IFS="$old_ifs"
    ;;
  is-active)
    [ "$(cat "$SYSTEMD_ACTIVE_STATE")" = active ]
    ;;
  stop)
    [ "\${SYSTEMCTL_FAIL:-}" != stop ] || exit 1
    printf 'inactive\\n' > "$SYSTEMD_ACTIVE_STATE"
    if [ "\${TAMPER_RUSTUP_ON_STOP:-}" = 1 ]; then
      mkdir -p "$BOOTSTRAP_TEST_ROOT/home/$RUNNER_USER/.cargo/bin"
      printf '#!/bin/sh\\nprintf executed > "%s"\\n' "$TOOLCHAIN_EXECUTED_MARKER" > "$BOOTSTRAP_TEST_ROOT/home/$RUNNER_USER/.cargo/bin/rustup"
      chmod 0755 "$BOOTSTRAP_TEST_ROOT/home/$RUNNER_USER/.cargo/bin/rustup"
    fi
    ;;
  start)
    [ "\${SYSTEMCTL_FAIL:-}" != start ] || exit 1
    printf 'active\\n' > "$SYSTEMD_ACTIVE_STATE"
    ;;
  disable)
    [ "\${SYSTEMCTL_FAIL:-}" != disable ] || exit 1
    ;;
  daemon-reload|enable|reset-failed|show-environment) ;;
  *) exit 90 ;;
esac
[ "\${FAIL_EFFECT:-}" != "systemctl:$*" ]`,
  );
  logger(
    "snap",
    'case "$*" in "wait system seed.loaded"|"install amazon-ssm-agent --classic") ;; *) exit 90 ;; esac; [ "\${FAIL_EFFECT:-}" != "snap:$*" ]',
  );
  logger("corepack", '[ "$#" -eq 1 ] && [ "$1" = enable ]');
  logger("jq", '[ "$1" = -e ] && [ "\${FAIL_JQ:-}" != 1 ]');
  logger("cargo", '[ "$1" = install ] && [ "\${FAIL_EFFECT:-}" != "cargo:$*" ]');
  const sandboxed = (name) =>
    `for arg in "$@"; do case "$arg" in /*) case "$arg" in "$BOOTSTRAP_TEST_ROOT"/*) ;; *) exit 90 ;; esac ;; esac; done; exec /usr/bin/${name} "$@"`;
  // Fixture owners need not have a same-named group (for example, nobody:nogroup).
  // Translate only that intended group, after logging the original command.
  logger(
    "install",
    `fixture_owner="$(/usr/bin/id -un)"
directory=0
if [ "$1" = -d ]; then directory=1; shift; fi
if [ "$1" = -o ] && [ "$2" = "$fixture_owner" ] && [ "$3" = -g ] && [ "$4" = "$fixture_owner" ]; then
  shift 4
  set -- -o "$fixture_owner" -g "${runnerGid}" "$@"
fi
if [ "$directory" = 1 ]; then set -- -d "$@"; fi
${sandboxed("install")}`,
  );
  logger(
    "chown",
    `fixture_owner="$(/usr/bin/id -un)"
if [ "$1" = "$fixture_owner:$fixture_owner" ]; then
  shift
  set -- "$fixture_owner:${runnerGid}" "$@"
elif [ "$1" = -R ] && [ "$2" = "$fixture_owner:$fixture_owner" ]; then
  shift 2
  set -- -R "$fixture_owner:${runnerGid}" "$@"
fi
${sandboxed("chown")}`,
  );
  for (const name of ["chmod", "ln", "mv", "rmdir", "rm", "mktemp"]) logger(name, sandboxed(name));
  let entries = fs.readdirSync(root, { recursive: true }).sort();

  const invoke = (overrides = {}, allowlist = entries) => {
    assert.deepEqual(
      fs.readdirSync(root, { recursive: true }).sort(),
      allowlist,
      "fixture remains within its recorded allowlist before execution",
    );
    return spawnSync("/bin/bash", [SCRIPT], {
      cwd: root,
      encoding: "utf8",
      timeout: 10000,
      env: {
        ...process.env,
        PATH: `${bin}:/usr/bin:/bin`,
        TRACE: trace,
        BOOTSTRAP_TEST_MODE: "1",
        BOOTSTRAP_TEST_ROOT: root,
        RUNNER_USER: runner,
        BOOTSTRAP_FIXTURE_STATE: state,
        SYSTEMD_ACTIVE_STATE: activeState,
        SYSTEMD_UNIT_DIR: unitDir,
        SYSTEMD_LOADED_UNIT_DIR: loadedUnitDir,
        CGROUP_ROOT: path.join(root, "sys", "fs", "cgroup"),
        GENERATED_SVC_EXECUTED: path.join(root, "generated-svc-executed"),
        CONFIG_BLOCKED_MARKER: path.join(root, "config-blocked"),
        CONFIG_RELEASE_MARKER: path.join(root, "config-release"),
        RUNNER_NAME: runnerName,
        RUNNER_URL: runnerUrl,
        RUNNER_TOKEN: "fixture-token",
        INSTALL_SSM_AGENT: "0",
        SKIP_HARDENING: "1",
        ...overrides,
      },
    });
  };
  const invokeAsync = (overrides = {}, allowlist = entries) => {
    assert.deepEqual(
      fs.readdirSync(root, { recursive: true }).sort(),
      allowlist,
      "fixture remains within its recorded allowlist before execution",
    );
    const child = spawn("/bin/bash", [SCRIPT], {
      cwd: root,
      env: {
        ...process.env,
        PATH: `${bin}:/usr/bin:/bin`,
        TRACE: trace,
        BOOTSTRAP_TEST_MODE: "1",
        BOOTSTRAP_TEST_ROOT: root,
        RUNNER_USER: runner,
        BOOTSTRAP_FIXTURE_STATE: state,
        SYSTEMD_ACTIVE_STATE: activeState,
        SYSTEMD_UNIT_DIR: unitDir,
        SYSTEMD_LOADED_UNIT_DIR: loadedUnitDir,
        CGROUP_ROOT: path.join(root, "sys", "fs", "cgroup"),
        GENERATED_SVC_EXECUTED: path.join(root, "generated-svc-executed"),
        CONFIG_BLOCKED_MARKER: path.join(root, "config-blocked"),
        CONFIG_RELEASE_MARKER: path.join(root, "config-release"),
        CONFIG_AFTER_SIGNAL_MARKER: path.join(root, "config-after-signal"),
        RUNNER_NAME: runnerName,
        RUNNER_URL: runnerUrl,
        RUNNER_TOKEN: "fixture-token",
        INSTALL_SSM_AGENT: "0",
        SKIP_HARDENING: "1",
        ...overrides,
      },
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => (stdout += chunk));
    child.stderr.on("data", (chunk) => (stderr += chunk));
    const completion = new Promise((resolve) => {
      child.once("close", (status, signal) => resolve({ status, signal, stdout, stderr }));
    });
    return { child, completion };
  };
  const recordAllowlist = () => {
    entries = fs.readdirSync(root, { recursive: true }).sort();
    return entries;
  };
  const invokeOrchestration = ({
    wasm = false,
    ssm = false,
    autoSsm = false,
    failEffect = "",
  } = {}) => {
    assert.deepEqual(
      fs.readdirSync(root, { recursive: true }).sort(),
      entries,
      "orchestration fixture remains within its recorded allowlist",
    );
    const command = wasm
      ? 'install_verified_wasm_pack "$RUNNER_USER" "$RUNNER_HOME" "$RUNNER_HOME/.cargo/bin:$PATH" 0.13.1'
      : `${autoSsm ? 'resolved="$(resolve_install_ssm auto "$SYS_VENDOR" "$SYS_PRODUCT")"; ' : ""}install_verified_os_dependencies "${autoSsm ? "$resolved" : ssm ? "1" : "0"}" "$INSTALL_OWNER" "$RUNNER_ROOT" "$OS_DEPS_MARKER" "$DEV_NULL"`;
    return spawnSync(
      "/bin/bash",
      ["-euo", "pipefail", "-c", `source "$ORCHESTRATION"; ${command}`],
      {
        cwd: root,
        encoding: "utf8",
        timeout: 10000,
        env: {
          ...process.env,
          PATH: `${bin}:/usr/bin:/bin`,
          TRACE: trace,
          ORCHESTRATION,
          BOOTSTRAP_TEST_ROOT: root,
          RUNNER_USER: runner,
          RUNNER_HOME: home,
          INSTALL_OWNER: runner,
          RUNNER_ROOT: path.join(root, "var", "lib", "actions-runner"),
          OS_DEPS_MARKER: path.join(root, "var", "lib", "actions-runner", ".os-dependencies-v1"),
          DEV_NULL: path.join(root, "dev", "null"),
          SYS_VENDOR: path.join(root, "sys", "devices", "virtual", "dmi", "id", "sys_vendor"),
          SYS_PRODUCT: path.join(root, "sys", "devices", "virtual", "dmi", "id", "product_name"),
          FAIL_EFFECT: failEffect,
        },
      },
    );
  };
  return {
    root,
    trace,
    pkg,
    state,
    unitDir,
    unitFile,
    unitName,
    cgroup,
    activeState,
    node: path.join(node, "node"),
    invoke,
    invokeAsync,
    invokeOrchestration,
    recordAllowlist,
    unloadUnit: (name = unitName) => fs.rmSync(path.join(loadedUnitDir, name)),
    cleanup: () => {
      makeRemovable(root);
      fs.rmSync(root, { recursive: true, force: true });
      fs.rmSync(bin, { recursive: true, force: true });
    },
  };
}

test("bootstrap rejects an unterminated final ExecStop hook before mutation or tool execution", () => {
  const fixture = bootstrapFixture();
  try {
    const candidate = `[Unit]
Description=Jazz benchmark runner fixture-runner

[Install]
WantedBy=multi-user.target

[Service]
User=${os.userInfo().username}
WorkingDirectory=${fixture.pkg}
ExecStart=${fixture.pkg}/runsvc.sh
KillSignal=SIGTERM
TimeoutStopSec=5min
ExecStop=/bin/false`;
    fs.writeFileSync(fixture.unitFile, candidate);
    const result = fixture.invoke({}, fixture.recordAllowlist());
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.doesNotMatch(events, /^systemctl:(disable|stop|enable|start|daemon-reload) /m);
    assert.doesNotMatch(events, /^(config|svc|curl|apt-get|snap|corepack|cargo):/m);
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo|config\.sh)/m);
    assert.equal(fs.existsSync(path.join(fixture.root, "node-executed")), false);
    assert.equal(fs.readFileSync(fixture.activeState, "utf8"), "active\n");
    assert.equal(fs.readFileSync(fixture.unitFile, "utf8"), candidate);
    assert.notEqual(result.status, 0, "a final unterminated hook is still part of the unit");
  } finally {
    fixture.cleanup();
  }
});

for (const hook of ["ExecStartPre", "ExecStartPost", "ExecStop", "ExecStopPost"]) {
  test(`bootstrap rejects manager-loaded ${hook} absent from disk before mutation`, () => {
    const fixture = bootstrapFixture();
    try {
      const unitBefore = fs.readFileSync(fixture.unitFile, "utf8");
      const result = fixture.invoke({ SYSTEMD_LOADED_HOOK: hook });
      const events = fs.readFileSync(fixture.trace, "utf8");
      assert.doesNotMatch(events, /^systemctl:(disable|stop|enable|start|daemon-reload) /m);
      assert.doesNotMatch(events, /^(config|svc|curl|apt-get|snap|corepack|cargo):/m);
      assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo|config\.sh)/m);
      assert.equal(fs.existsSync(path.join(fixture.root, "node-executed")), false);
      assert.equal(fs.readFileSync(fixture.activeState, "utf8"), "active\n");
      assert.equal(fs.readFileSync(fixture.unitFile, "utf8"), unitBefore);
      assert.notEqual(result.status, 0, `loaded ${hook} must not be hidden by a clean disk file`);
    } finally {
      fixture.cleanup();
    }
  });
}

test("bootstrap reaps config on TERM during launch PID handoff before sealing the package", async () => {
  const fixture = bootstrapFixture({ rawPackage: true });
  const bashEnv = path.join(fixture.root, "handoff-env.sh");
  const groupPidFile = path.join(fixture.root, "config-group.pid");
  const configPidFile = path.join(fixture.root, "config-process.pid");
  const signalMarker = path.join(fixture.root, "handoff-signalled");
  const releaseMarker = path.join(fixture.root, "config-release");
  const afterSignalMarker = path.join(fixture.root, "config-after-signal");
  const afterSignalState = path.join(fixture.state, "after-signal");
  fs.rmSync(path.join(fixture.state, ".runner"));
  fs.rmSync(path.join(fixture.state, ".credentials"));
  fs.writeFileSync(
    bashEnv,
    `handoff_bootstrap_pid=$BASHPID
handoff_before_pid_capture() {
  [[ "$BASHPID" == "$handoff_bootstrap_pid" && "$BASH_COMMAND" == 'runner_config_pid=$!' ]] || return 0
  trap - DEBUG
  printf '%s\\n' "$!" > "$CONFIG_GROUP_PID_FILE"
  for (( handoff_attempt=0; handoff_attempt<500; handoff_attempt++ )); do
    if [[ -f "$CONFIG_BLOCKED_MARKER" ]]; then
      printf 'TERM at PID capture\\n' > "$HANDOFF_SIGNAL_MARKER"
      kill -TERM "$BASHPID"
      return 0
    fi
    sleep 0.01
  done
  exit 94
}
trap handoff_before_pid_capture DEBUG
`,
  );
  let execution;
  let completed = false;
  let deadline;
  try {
    execution = fixture.invokeAsync(
      {
        ALLOW_CONFIG: "1",
        GENERATE_SVC: "1",
        CONFIG_HANDOFF: "1",
        BASH_ENV: bashEnv,
        CONFIG_GROUP_PID_FILE: groupPidFile,
        CONFIG_PROCESS_PID_FILE: configPidFile,
        HANDOFF_SIGNAL_MARKER: signalMarker,
        CONFIG_AFTER_SIGNAL_MARKER: afterSignalMarker,
        CONFIG_AFTER_SIGNAL_STATE: afterSignalState,
      },
      fixture.recordAllowlist(),
    );
    const completion = execution.completion.then((result) => {
      completed = true;
      return result;
    });
    const result = await Promise.race([
      completion,
      new Promise((resolve) => {
        deadline = setTimeout(() => resolve(null), 8000);
      }),
    ]);
    clearTimeout(deadline);
    assert.equal(fs.existsSync(signalMarker), true, "TERM is delivered before the PID assignment");
    assert.ok(result, "bootstrap cancels the blocked config without waiting for a release");
    assert.equal(result.status, 143, "the deferred TERM retains its cancellation exit status");
    assert.equal(result.signal, null, "bootstrap handles TERM rather than dying without cleanup");
    fs.writeFileSync(releaseMarker, "continue\n");
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(
      fs.existsSync(afterSignalState),
      false,
      "a surviving config must not write mutable state after cancellation and sealing",
    );
    assert.equal(fs.existsSync(afterSignalMarker), false, "config cannot continue after TERM");
    for (const pidFile of [configPidFile, groupPidFile]) {
      const pid = Number(fs.readFileSync(pidFile, "utf8").trim());
      assert.ok(Number.isSafeInteger(pid) && pid > 1);
      assert.throws(
        () => process.kill(pid, 0),
        { code: "ESRCH" },
        "config and its captured group leader have terminated and been reaped",
      );
    }
    assert.equal(fs.statSync(fixture.pkg).uid, process.getuid());
    assert.equal(fs.statSync(fixture.pkg).mode & 0o7777, 0o555);
    assert.equal(fs.existsSync(path.join(fixture.pkg, "svc.sh")), false);
    assert.equal(fs.existsSync(path.join(fixture.root, "generated-svc-executed")), false);
    assert.doesNotMatch(fs.readFileSync(fixture.trace, "utf8"), /^systemctl:start /m);
  } finally {
    clearTimeout(deadline);
    // A failing implementation can leave the untracked process group behind.
    if (fs.existsSync(groupPidFile)) {
      const pid = Number(fs.readFileSync(groupPidFile, "utf8").trim());
      if (Number.isSafeInteger(pid) && pid > 1) {
        try {
          process.kill(-pid, "SIGKILL");
        } catch (error) {
          if (error.code !== "ESRCH") throw error;
        }
      }
    }
    if (execution && !completed) {
      execution.child.kill("SIGKILL");
      await execution.completion;
    }
    fixture.cleanup();
  }
});

test("bootstrap confines the registration token to config children even on failure cleanup", () => {
  const fixture = bootstrapFixture({ rawPackage: true });
  try {
    fs.rmSync(path.join(fixture.state, ".runner"));
    fs.rmSync(path.join(fixture.state, ".credentials"));
    const result = fixture.invoke(
      { ALLOW_CONFIG: "1", GENERATE_SVC: "1", FAIL_CONFIG_AFTER_SVC: "1" },
      fixture.recordAllowlist(),
    );
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.notEqual(result.status, 0, "config's deliberate failure exercises exit cleanup");
    assert.ok(events.split("\n").includes(`chmod:1770 ${fixture.pkg}`), "setup opens the package");
    assert.ok(
      events.split("\n").includes(`chmod:0555 ${fixture.pkg}`),
      "cleanup seals the package",
    );
    assert.match(events, /^config-token-present$/m, "config receives the registration token");
    assert.doesNotMatch(
      events,
      /-token-argv-present|fixture-token/,
      "no child receives token argv",
    );
    const tokenChildren = events.split("\n").filter((event) => event.endsWith("-token-present"));
    assert.ok(tokenChildren.includes("runuser-config-token-present"));
    for (const event of tokenChildren) {
      assert.match(
        event,
        /^(?:config|runuser-config|[a-z-]+-config-child)-token-present$/,
        "only config's process group inherits the token, not setup or cleanup",
      );
    }
    assert.equal(fs.statSync(fixture.pkg).mode & 0o7777, 0o555);
    assert.equal(fs.existsSync(path.join(fixture.pkg, "svc.sh")), false);
  } finally {
    fixture.cleanup();
  }
});
test("bootstrap rejects a modified Node executable before invoking it", () => {
  const fixture = bootstrapFixture();
  try {
    fs.writeFileSync(
      fixture.node,
      `#!/bin/sh\n# changed after manifest\nprintf executed > '${path.join(fixture.root, "node-executed")}'\necho v24.13.0\n`,
      { mode: 0o755 },
    );
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0, "modified Node tree must fail integrity verification");
    assert.equal(
      fs.existsSync(path.join(fixture.root, "node-executed")),
      false,
      "untrusted Node was never executed",
    );
    assert.doesNotMatch(fs.readFileSync(fixture.trace, "utf8"), /^svc:/m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap rejects an unmanifested Node file before invoking Node", () => {
  const fixture = bootstrapFixture();
  try {
    fs.writeFileSync(
      path.join(path.dirname(path.dirname(fixture.node)), "unmanifested"),
      "unexpected",
    );
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0, "unmanifested Node entries are rejected");
    assert.equal(
      fs.existsSync(path.join(fixture.root, "node-executed")),
      false,
      "untrusted Node was never executed",
    );
    assert.doesNotMatch(fs.readFileSync(fixture.trace, "utf8"), /^svc:/m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap rejects symlinked or writable runner parents before service execution", () => {
  for (const damage of ["symlink", "writable"]) {
    const fixture = bootstrapFixture();
    try {
      const parent = path.join(fixture.root, "opt", "actions-runner");
      if (damage === "symlink") {
        const replacement = path.join(fixture.root, "opt", "attacker-runner");
        fs.renameSync(parent, replacement);
        fs.symlinkSync(replacement, parent);
      } else {
        fs.chmodSync(parent, 0o777);
      }
      const result = fixture.invoke({}, fixture.recordAllowlist());
      assert.notEqual(result.status, 0, `${damage} runner parent must be rejected`);
      assert.doesNotMatch(fs.readFileSync(fixture.trace, "utf8"), /^svc:/m);
    } finally {
      fixture.cleanup();
    }
  }
});

test("bootstrap runs runner configuration and installs the root-managed service from the package directory", () => {
  const fixture = bootstrapFixture();
  try {
    for (const file of [".runner", ".credentials"]) fs.rmSync(path.join(fixture.state, file));
    const result = fixture.invoke({ ALLOW_CONFIG: "1" }, fixture.recordAllowlist());
    assert.equal(result.status, 0, `${result.stderr}\n${result.stdout}`);
    assert.ok(fs.existsSync(fixture.trace), `${result.stderr}\n${result.stdout}`);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^config-token-present$/m);
    assert.doesNotMatch(events, /--token|fixture-token/);
    assert.ok(events.split("\n").includes(`config-cwd:${fixture.pkg}`));
    assert.match(events, /^systemctl:start /m);
    assert.doesNotMatch(events, /^svc:/m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap rejects a symlinked work directory without changing its target", () => {
  const fixture = bootstrapFixture();
  try {
    const target = path.join(fixture.root, "attacker-work");
    fs.mkdirSync(target);
    const sentinel = path.join(target, "existing");
    fs.writeFileSync(sentinel, "do not touch");
    fs.chmodSync(target, 0o755);
    fs.symlinkSync(target, path.join(fixture.state, "_work"));
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0, "symlinked mutable state must fail closed");
    assert.ok(fs.existsSync(fixture.trace), `${result.stderr}\n${result.stdout}`);
    assert.equal(
      fs.statSync(target).mode & 0o777,
      0o755,
      "bootstrap must not change the symlink target's mode",
    );
    assert.equal(fs.readFileSync(sentinel, "utf8"), "do not touch");
    assert.deepEqual(fs.readdirSync(target), ["existing"]);
    assert.doesNotMatch(fs.readFileSync(fixture.trace, "utf8"), /^svc:/m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap entry rejects malformed settings without evaluating them", () => {
  const fixture = bootstrapFixture();
  try {
    const marker = path.join(fixture.root, "evaluated");
    const result = fixture.invoke({ RUNNER_LABELS: `$(touch ${marker})` });
    assert.notEqual(result.status, 0);
    assert.equal(fs.existsSync(marker), false);
    assert.equal(
      fs.existsSync(fixture.trace),
      false,
      "invalid values are rejected before external effects",
    );
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap entry rejects a corrupt pinned download before dependency setup", () => {
  const fixture = bootstrapFixture();
  try {
    fs.rmSync(path.join(fixture.root, "var", "lib", "actions-runner", ".os-dependencies-v1"));
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^curl:/m);
    assert.doesNotMatch(events, /^(apt-get|snap):/m);
    assert.match(events, /^systemctl:stop /m);
    assert.doesNotMatch(events, /^systemctl:(enable|start) /m);
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo|config\.sh)/m);
    assert.equal(fs.existsSync(path.join(fixture.pkg, ".bootstrap-manifest")), true);
    assert.doesNotMatch(events, /svc:/);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap entry fails closed when a pinned download is missing", () => {
  const fixture = bootstrapFixture();
  try {
    fs.rmSync(path.join(fixture.root, "var", "lib", "actions-runner", ".os-dependencies-v1"));
    const result = fixture.invoke({ CURL_MODE: "missing" }, fixture.recordAllowlist());
    assert.notEqual(result.status, 0);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^curl:/m);
    assert.doesNotMatch(events, /^(apt-get|snap):/m);
    assert.match(events, /^systemctl:stop /m);
    assert.doesNotMatch(events, /^systemctl:(enable|start) /m);
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo|config\.sh)/m);
    assert.doesNotMatch(events, /^(config|svc):/m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap entry rejects invalid existing runner state before config or service", () => {
  const fixture = bootstrapFixture();
  try {
    fs.writeFileSync(path.join(fixture.state, ".runner"), "{invalid");
    const result = fixture.invoke({ FAIL_JQ: "1" }, fixture.recordAllowlist());
    assert.notEqual(result.status, 0);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.doesNotMatch(events, /^(systemctl|snap):/m);
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo|config\.sh)/m);
    assert.doesNotMatch(events, /^svc:/m);
    assert.equal(fs.readFileSync(path.join(fixture.state, ".runner"), "utf8"), "{invalid");
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap reuses complete pinned state and starts the root-managed runner unit", () => {
  const fixture = bootstrapFixture();
  try {
    const manifestPath = path.join(fixture.pkg, ".bootstrap-manifest");
    const manifestBefore = fs.readFileSync(manifestPath, "utf8");
    const result = fixture.invoke();
    assert.equal(result.status, 0, result.stderr);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.doesNotMatch(events, /^(curl|apt-get):/m);
    assert.match(events, /^systemctl:start /m);
    const disableIndex = events.indexOf(`systemctl:disable ${fixture.unitName}`);
    const stopIndex = events.indexOf(`systemctl:stop ${fixture.unitName}`);
    const enableIndex = events.indexOf(`systemctl:enable ${fixture.unitName}`);
    const startIndex = events.indexOf(`systemctl:start ${fixture.unitName}`);
    assert.ok(disableIndex >= 0 && disableIndex < stopIndex);
    assert.ok(stopIndex < enableIndex && enableIndex < startIndex);
    assert.equal(fs.readFileSync(manifestPath, "utf8"), manifestBefore);
    assert.equal(fs.existsSync(path.join(fixture.state, ".service")), false);
  } finally {
    fixture.cleanup();
  }
});
test("bootstrap reuses the canonical unit for a differently cased repository URL", () => {
  const fixture = bootstrapFixture();
  try {
    const result = fixture.invoke(
      { RUNNER_URL: "https://github.com/Garden-Co/Jazz2/" },
      fixture.recordAllowlist(),
    );
    assert.equal(result.status, 0, `${result.stderr}\n${result.stdout}`);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.ok(
      events.split("\n").includes(`systemctl:start ${fixture.unitName}`),
      "repository casing and a trailing slash retain the canonical unit identity",
    );
    assert.doesNotMatch(events, /^runuser:.*config\.sh/m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap repairs a missing manager unit for complete current runner state", () => {
  const fixture = bootstrapFixture();
  try {
    fs.rmSync(fixture.unitFile);
    fixture.unloadUnit();
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.equal(result.status, 0, `${result.stderr}\n${result.stdout}`);
    assert.equal(fs.existsSync(fixture.unitFile), true);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^systemctl:start /m);
    assert.doesNotMatch(events, /^svc:/m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap rejects a manager-loaded current unit missing from disk without a .service marker", () => {
  const fixture = bootstrapFixture();
  try {
    fs.rmSync(fixture.unitFile);
    assert.equal(fs.existsSync(path.join(fixture.state, ".service")), false);
    const registrationBefore = fs.readFileSync(path.join(fixture.state, ".runner"), "utf8");
    const credentialsBefore = fs.readFileSync(path.join(fixture.state, ".credentials"), "utf8");
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0, "a loaded unit without a disk definition must be rejected");
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.doesNotMatch(events, /^systemctl:(disable|stop|enable|start|daemon-reload)(?: |$)/m);
    assert.doesNotMatch(events, /^(config|svc|curl|apt-get|snap|corepack|cargo):/m);
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo|config\.sh)/m);
    assert.equal(fs.existsSync(path.join(fixture.root, "node-executed")), false);
    assert.equal(fs.existsSync(fixture.unitFile), false, "the missing disk unit is not repaired");
    assert.equal(fs.readFileSync(fixture.activeState, "utf8"), "active\n");
    assert.equal(fs.readFileSync(path.join(fixture.state, ".runner"), "utf8"), registrationBefore);
    assert.equal(
      fs.readFileSync(path.join(fixture.state, ".credentials"), "utf8"),
      credentialsBefore,
    );
    assert.equal(fs.existsSync(path.join(fixture.state, ".service")), false);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap stops a current unit without a .service marker before checking tools", () => {
  const fixture = bootstrapFixture();
  try {
    assert.equal(fs.existsSync(path.join(fixture.state, ".service")), false);
    const marker = path.join(fixture.root, "rustup-executed-after-stop");
    const result = fixture.invoke(
      { TAMPER_RUSTUP_ON_STOP: "1", TOOLCHAIN_EXECUTED_MARKER: marker },
      fixture.recordAllowlist(),
    );
    assert.notEqual(result.status, 0, "post-stop toolchain tampering must fail closed");
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.ok(events.split("\n").includes(`systemctl:stop ${fixture.unitName}`));
    const disableIndex = events.indexOf(`systemctl:disable ${fixture.unitName}`);
    const stopIndex = events.indexOf(`systemctl:stop ${fixture.unitName}`);
    assert.ok(disableIndex >= 0 && disableIndex < stopIndex);
    assert.equal(fs.existsSync(marker), false, "modified Rustup is never executed");
    assert.doesNotMatch(events, /^runuser:.*\/\.cargo\/bin\/rustup/m);
    assert.doesNotMatch(
      events,
      /^systemctl:start /m,
      "failed verification does not restart the unit",
    );
    assert.doesNotMatch(
      events,
      /^systemctl:restart /m,
      "bootstrap never aggregate-restarts the unit",
    );
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap fails closed when the existing runner unit cannot stop", () => {
  const fixture = bootstrapFixture();
  try {
    const result = fixture.invoke({ SYSTEMCTL_FAIL: "stop" });
    assert.match(result.stderr, /runner service stop failed/);
    assert.notEqual(result.status, 0, "systemd stop failure aborts bootstrap");
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^systemctl:stop /m);
    const disableIndex = events.indexOf(`systemctl:disable ${fixture.unitName}`);
    const stopIndex = events.indexOf(`systemctl:stop ${fixture.unitName}`);
    assert.ok(disableIndex >= 0 && disableIndex < stopIndex);
    assert.doesNotMatch(events, /^runuser:.*\/\.cargo\/bin\/rustup/m);
    assert.doesNotMatch(events, /^systemctl:start /m);
    assert.equal(fs.readFileSync(fixture.activeState, "utf8"), "active\n");
  } finally {
    fixture.cleanup();
  }
});
test("bootstrap stops but does not inspect tools when it cannot disable the verified unit", () => {
  const fixture = bootstrapFixture();
  try {
    const result = fixture.invoke({ SYSTEMCTL_FAIL: "disable" });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /runner service could not be disabled/);
    const events = fs.readFileSync(fixture.trace, "utf8");
    const disableIndex = events.indexOf(`systemctl:disable ${fixture.unitName}`);
    const stopIndex = events.indexOf(`systemctl:stop ${fixture.unitName}`);
    assert.ok(disableIndex >= 0 && disableIndex < stopIndex);
    assert.equal(fs.readFileSync(fixture.activeState, "utf8"), "inactive\n");
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo)/m);
    assert.doesNotMatch(events, /^systemctl:start /m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap rejects a still-populated runner cgroup before persistent tools", () => {
  const fixture = bootstrapFixture();
  try {
    fs.writeFileSync(path.join(fixture.cgroup, "cgroup.events"), "populated 1\nfrozen 0\n");
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.match(result.stderr, /runner cgroup is not empty/);
    assert.notEqual(result.status, 0, "inactive systemd state does not prove an empty cgroup");
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^systemctl:stop /m);
    assert.equal(fs.readFileSync(fixture.activeState, "utf8"), "inactive\n");
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo)/m);
    assert.doesNotMatch(events, /^systemctl:start /m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap rejects a path-matching unit name for another runner registration", () => {
  const fixture = bootstrapFixture();
  try {
    fs.rmSync(fixture.unitFile);
    fixture.unloadUnit();
    const otherUnit = path.join(
      fixture.unitDir,
      serviceUnitName("https://github.com/garden-co/jazz2", "some-other-runner"),
    );
    fs.writeFileSync(
      path.join(fixture.state, ".runner"),
      JSON.stringify({
        agentName: "fixture-runner",
        serverUrl: "https://github.com/garden-co/jazz2",
        workFolder: "_work",
      }),
    );
    fs.writeFileSync(
      otherUnit,
      serviceUnitContents({
        user: os.userInfo().username,
        pkg: fixture.pkg,
        runnerName: "some-other-runner",
      }),
      { mode: 0o644 },
    );
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(
      result.status,
      0,
      "root-owned unit identity must agree with requested registration",
    );
    assert.match(result.stderr, /runner unit identity is ambiguous/);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.doesNotMatch(events, /^systemctl:stop /m);
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo)/m);
    assert.equal(fs.existsSync(otherUnit), true, "unrelated unit remains untouched");
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap rejects a runner account resolving to UID zero before side effects", () => {
  const fixture = bootstrapFixture();
  try {
    const result = fixture.invoke({ FIXTURE_UID: "0" });
    assert.notEqual(result.status, 0, "root cannot be selected as the runner account");
    assert.match(result.stderr, /RUNNER_USER must resolve to a non-root UID/);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.doesNotMatch(events, /^(apt-get|curl|systemctl|snap):/m);
    assert.doesNotMatch(events, /^runuser:/m);
    assert.doesNotMatch(events, /^(config|svc):/m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap creates a Jazz-managed unit after configuring an archive without svc.sh", () => {
  const fixture = bootstrapFixture({ rawPackage: true });
  try {
    fs.rmSync(path.join(fixture.state, ".runner"));
    fs.rmSync(path.join(fixture.state, ".credentials"));
    fs.rmSync(fixture.unitFile);
    fixture.unloadUnit();
    const result = fixture.invoke(
      { ALLOW_CONFIG: "1", GENERATE_SVC: "1" },
      fixture.recordAllowlist(),
    );
    assert.equal(result.status, 0, `${result.stderr}\n${result.stdout}`);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^config-package-mode:1770$/m);
    assert.match(events, /^systemctl:start /m);
    assert.doesNotMatch(events, /^svc:/m);
    assert.equal(fs.existsSync(path.join(fixture.root, "generated-svc-executed")), false);
    assert.equal(fs.existsSync(path.join(fixture.pkg, "svc.sh")), false);
    assert.equal(fs.existsSync(fixture.unitFile), true, "expected root-managed unit is installed");
    assert.equal(fs.statSync(fixture.unitFile).mode & 0o777, 0o644);
    const unit = fs.readFileSync(fixture.unitFile, "utf8");
    assert.ok(unit.split("\n").includes(`User=${os.userInfo().username}`));
    assert.ok(unit.split("\n").includes(`WorkingDirectory=${fixture.pkg}`));
    assert.ok(unit.split("\n").includes(`ExecStart=${fixture.pkg}/runsvc.sh`));
    assert.doesNotMatch(unit, /^KillMode=/m, "the Jazz unit uses systemd's control-group default");
  } finally {
    fixture.cleanup();
  }
});
test("bootstrap stops a blocked config process and seals the runner package on signal", async () => {
  const fixture = bootstrapFixture({ rawPackage: true });
  const bootstrapPidFile = path.join(fixture.root, "bootstrap.pid");
  const blockedMarker = path.join(fixture.root, "config-blocked");
  const releaseMarker = path.join(fixture.root, "config-release");
  const afterSignalMarker = path.join(fixture.root, "config-after-signal");
  const bashEnv = path.join(fixture.root, "bootstrap-env.sh");
  fs.rmSync(path.join(fixture.state, ".runner"));
  fs.rmSync(path.join(fixture.state, ".credentials"));
  fs.writeFileSync(
    bashEnv,
    'if [ ! -e "$BOOTSTRAP_PID_FILE" ]; then printf "%s\\n" "$BASHPID" > "$BOOTSTRAP_PID_FILE"; fi\n',
  );
  let execution;
  let completed = false;
  let completion;
  try {
    execution = fixture.invokeAsync(
      {
        ALLOW_CONFIG: "1",
        GENERATE_SVC: "1",
        INTERRUPT_CONFIG: "1",
        BASH_ENV: bashEnv,
        BOOTSTRAP_PID_FILE: bootstrapPidFile,
        CONFIG_AFTER_SIGNAL_MARKER: afterSignalMarker,
      },
      fixture.recordAllowlist(),
    );
    completion = execution.completion.then((result) => {
      completed = true;
      return result;
    });
    let configBlocked = false;
    for (let attempt = 0; attempt < 500; attempt++) {
      if (fs.existsSync(blockedMarker)) {
        configBlocked = true;
        break;
      }
      if (execution.child.exitCode !== null || execution.child.signalCode !== null) break;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.ok(configBlocked, "config reaches its deliberate blocked state");
    const earlyExit = await Promise.race([
      completion.then((result) => ({ result })),
      new Promise((resolve) => setTimeout(() => resolve(null), 1000)),
    ]);
    if (!earlyExit) fs.writeFileSync(releaseMarker, "continue\n");
    const result = earlyExit?.result ?? (await completion);
    fs.writeFileSync(releaseMarker, "continue\n");
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(
      fs.existsSync(afterSignalMarker),
      false,
      "terminated config cannot write after the package is sealed",
    );
    assert.ok(earlyExit, "bootstrap promptly terminates the blocked config process group");
    assert.notEqual(result.status, 0, "the interrupted bootstrap fails");
    assert.equal(fs.existsSync(bootstrapPidFile), true);
    assert.equal(fs.statSync(fixture.pkg).uid, process.getuid());
    assert.equal(fs.statSync(fixture.pkg).mode & 0o777, 0o555);
    assert.equal(fs.existsSync(path.join(fixture.pkg, "svc.sh")), false);
    assert.doesNotMatch(fs.readFileSync(fixture.trace, "utf8"), /^systemctl:start /m);
  } finally {
    if (execution && !completed) {
      fs.writeFileSync(releaseMarker, "continue\n");
      await completion;
    }
    fixture.cleanup();
  }
});

test("bootstrap removes generated svc.sh when configuration fails", () => {
  const fixture = bootstrapFixture({ rawPackage: true });
  try {
    fs.rmSync(path.join(fixture.state, ".runner"));
    fs.rmSync(path.join(fixture.state, ".credentials"));
    const result = fixture.invoke(
      { ALLOW_CONFIG: "1", GENERATE_SVC: "1", FAIL_CONFIG_AFTER_SVC: "1" },
      fixture.recordAllowlist(),
    );
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /runner configuration failed/);
    assert.equal(fs.statSync(fixture.pkg).uid, process.getuid());
    assert.equal(fs.statSync(fixture.pkg).mode & 0o777, 0o555);
    assert.equal(fs.existsSync(path.join(fixture.pkg, "svc.sh")), false);
    assert.doesNotMatch(fs.readFileSync(fixture.trace, "utf8"), /^systemctl:start /m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap rejects a Jazz unit with an explicit KillMode override before stopping", () => {
  const fixture = bootstrapFixture();
  try {
    const unit = fs.readFileSync(fixture.unitFile, "utf8");
    fs.writeFileSync(
      fixture.unitFile,
      unit.replace("[Service]\n", "[Service]\nKillMode=process\n"),
      { mode: 0o644 },
    );
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0, "an overridden Jazz unit is not trusted");
    assert.match(result.stderr, /unit/i);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.doesNotMatch(events, /^systemctl:stop /m);
    assert.doesNotMatch(events, /^systemctl:disable /m);
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo)/m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap disables a verified legacy unit before controlled reprovision", () => {
  const fixture = bootstrapFixture({
    rawPackage: true,
    runnerUrl: "https://github.com/gardenco/jazz2",
  });
  try {
    fs.rmSync(fixture.unitFile);
    fixture.unloadUnit();
    const legacyName = "actions.runner.gardenco-jazz2.fixture-runner.service";
    const legacyUnitFile = path.join(fixture.unitDir, legacyName);
    const legacyPackage = path.join(fixture.root, "home", os.userInfo().username, "actions-runner");
    fs.mkdirSync(legacyPackage, { recursive: true });
    fs.chmodSync(legacyPackage, 0o755);
    fs.writeFileSync(path.join(legacyPackage, "runsvc.sh"), "#!/bin/sh\nexit 0\n", {
      mode: 0o755,
    });
    const legacyUnit = serviceUnitContents({
      user: os.userInfo().username,
      pkg: legacyPackage,
    }).replace("[Service]\n", "[Service]\nKillMode=process\n");
    fs.writeFileSync(legacyUnitFile, legacyUnit, { mode: 0o644 });
    const legacyCgroup = path.join(fixture.root, "sys", "fs", "cgroup", "system.slice", legacyName);
    fs.mkdirSync(legacyCgroup, { recursive: true });
    fs.writeFileSync(path.join(legacyCgroup, "cgroup.events"), "populated 0\nfrozen 0\n");

    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0, "legacy runner state requires controlled reprovision");
    assert.match(result.stderr, /reprovision/i);
    const events = fs.readFileSync(fixture.trace, "utf8");
    const disableIndex = events.indexOf(`systemctl:disable ${legacyName}`);
    const stopIndex = events.indexOf(`systemctl:stop ${legacyName}`);
    assert.ok(disableIndex >= 0 && disableIndex < stopIndex);
    assert.equal(fs.readFileSync(fixture.activeState, "utf8"), "inactive\n");
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo)/m);
    assert.doesNotMatch(events, /^systemctl:start /m);
    assert.equal(fs.existsSync(legacyUnitFile), true, "bootstrap does not migrate legacy state");
  } finally {
    fixture.cleanup();
  }
});
test("bootstrap disables a verified prior-version unit before controlled reprovision", () => {
  const fixture = bootstrapFixture({
    rawPackage: true,
    runnerUrl: "https://github.com/gardenco/jazz2",
  });
  try {
    fs.rmSync(fixture.unitFile);
    fixture.unloadUnit();
    const legacyName = "actions.runner.gardenco-jazz2.fixture-runner.service";
    const legacyUnitFile = path.join(fixture.unitDir, legacyName);
    const legacyPackage = path.join(fixture.root, "opt", "actions-runner", "2.336.0");
    fs.mkdirSync(legacyPackage, { recursive: true });
    fs.chmodSync(legacyPackage, 0o755);
    fs.writeFileSync(path.join(legacyPackage, "runsvc.sh"), "#!/bin/sh\nexit 0\n", {
      mode: 0o755,
    });
    const legacyUnit = serviceUnitContents({
      user: os.userInfo().username,
      pkg: legacyPackage,
    }).replace("[Service]\n", "[Service]\nKillMode=process\n");
    fs.writeFileSync(legacyUnitFile, legacyUnit, { mode: 0o644 });
    const legacyCgroup = path.join(fixture.root, "sys", "fs", "cgroup", "system.slice", legacyName);
    fs.mkdirSync(legacyCgroup, { recursive: true });
    fs.writeFileSync(path.join(legacyCgroup, "cgroup.events"), "populated 0\nfrozen 0\n");

    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0, "a prior-version runner requires controlled reprovision");
    assert.match(result.stderr, /reprovision/i);
    const events = fs.readFileSync(fixture.trace, "utf8");
    const disableIndex = events.indexOf(`systemctl:disable ${legacyName}`);
    const stopIndex = events.indexOf(`systemctl:stop ${legacyName}`);
    assert.ok(disableIndex >= 0 && disableIndex < stopIndex);
    assert.equal(fs.readFileSync(fixture.activeState, "utf8"), "inactive\n");
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo)/m);
    assert.doesNotMatch(events, /^systemctl:start /m);
    assert.equal(
      fs.existsSync(legacyUnitFile),
      true,
      "bootstrap does not migrate prior-version state",
    );
  } finally {
    fixture.cleanup();
  }
});
test("bootstrap disables and quiesces all verified units before controlled reprovision", () => {
  const fixture = bootstrapFixture({
    rawPackage: true,
    runnerUrl: "https://github.com/gardenco/jazz2",
  });
  try {
    const legacyName = "actions.runner.gardenco-jazz2.fixture-runner.service";
    const legacyUnitFile = path.join(fixture.unitDir, legacyName);
    const legacyPackage = path.join(fixture.root, "home", os.userInfo().username, "actions-runner");
    fs.mkdirSync(legacyPackage, { recursive: true });
    fs.chmodSync(legacyPackage, 0o755);
    fs.writeFileSync(path.join(legacyPackage, "runsvc.sh"), "#!/bin/sh\nexit 0\n", {
      mode: 0o755,
    });
    const legacyUnit = serviceUnitContents({
      user: os.userInfo().username,
      pkg: legacyPackage,
    }).replace("[Service]\n", "[Service]\nKillMode=process\n");
    fs.writeFileSync(legacyUnitFile, legacyUnit, { mode: 0o644 });
    const legacyCgroup = path.join(fixture.root, "sys", "fs", "cgroup", "system.slice", legacyName);
    fs.mkdirSync(legacyCgroup, { recursive: true });
    fs.writeFileSync(path.join(legacyCgroup, "cgroup.events"), "populated 0\nfrozen 0\n");

    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /reprovision/i);
    const events = fs.readFileSync(fixture.trace, "utf8");
    const legacyDisable = events.indexOf(`systemctl:disable ${legacyName}`);
    const jazzDisable = events.indexOf(`systemctl:disable ${fixture.unitName}`);
    const legacyStop = events.indexOf(`systemctl:stop ${legacyName}`);
    const jazzStop = events.indexOf(`systemctl:stop ${fixture.unitName}`);
    const firstStop = Math.min(legacyStop, jazzStop);
    assert.ok(legacyDisable >= 0 && legacyDisable < firstStop);
    assert.ok(jazzDisable >= 0 && jazzDisable < firstStop);
    assert.ok(legacyStop >= 0 && jazzStop >= 0);
    assert.equal(fs.readFileSync(fixture.activeState, "utf8"), "inactive\n");
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo)/m);
    assert.doesNotMatch(events, /^systemctl:start /m);
    assert.equal(fs.existsSync(legacyUnitFile), true, "legacy credentials are not migrated");
    assert.equal(fs.existsSync(fixture.unitFile), true, "verified units are not removed here");
  } finally {
    fixture.cleanup();
  }
});
test("bootstrap leaves legacy services untouched when their names are ambiguous", () => {
  for (const collision of [
    {
      runnerUrl: "https://github.com/foo-bar/baz",
      runnerName: "fixture-runner",
      legacyName: "actions.runner.foo-bar-baz.fixture-runner.service",
      installedRunnerName: "fixture-runner",
      alternate: "https://github.com/foo/bar-baz with runner fixture-runner",
    },
    {
      runnerUrl: "https://github.com/org/foo",
      runnerName: "bar.baz",
      legacyName: "actions.runner.org-foo.bar.baz.service",
      installedRunnerName: "baz",
      alternate: "https://github.com/org/foo.bar with runner baz",
    },
  ]) {
    const fixture = bootstrapFixture({
      rawPackage: true,
      runnerUrl: collision.runnerUrl,
      runnerName: collision.runnerName,
    });
    try {
      fs.rmSync(fixture.unitFile);
      fixture.unloadUnit();
      const legacyUnitFile = path.join(fixture.unitDir, collision.legacyName);
      const legacyPackage = path.join(
        fixture.root,
        "home",
        os.userInfo().username,
        "actions-runner",
      );
      fs.mkdirSync(legacyPackage, { recursive: true });
      fs.chmodSync(legacyPackage, 0o755);
      fs.writeFileSync(path.join(legacyPackage, "runsvc.sh"), "#!/bin/sh\nexit 0\n", {
        mode: 0o755,
      });
      const legacyUnit = serviceUnitContents({
        user: os.userInfo().username,
        pkg: legacyPackage,
        runnerName: collision.installedRunnerName,
      }).replace("[Service]\n", "[Service]\nKillMode=process\n");
      fs.writeFileSync(legacyUnitFile, legacyUnit, { mode: 0o644 });
      const legacyCgroup = path.join(
        fixture.root,
        "sys",
        "fs",
        "cgroup",
        "system.slice",
        collision.legacyName,
      );
      fs.mkdirSync(legacyCgroup, { recursive: true });
      fs.writeFileSync(path.join(legacyCgroup, "cgroup.events"), "populated 0\nfrozen 0\n");

      const result = fixture.invoke({}, fixture.recordAllowlist());
      assert.notEqual(result.status, 0);
      assert.match(
        result.stderr,
        /legacy runner unit identity is ambiguous/,
        `the service name also represents ${collision.alternate}`,
      );
      const events = fs.readFileSync(fixture.trace, "utf8");
      assert.doesNotMatch(
        events,
        new RegExp(`^systemctl:(disable|stop) ${collision.legacyName}$`, "m"),
      );
      assert.equal(fs.readFileSync(fixture.activeState, "utf8"), "active\n");
      assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo)/m);
      assert.equal(fs.existsSync(legacyUnitFile), true);
    } finally {
      fixture.cleanup();
    }
  }
});

test("bootstrap rejects a .service marker without a manager unit before tools", () => {
  const fixture = bootstrapFixture();
  try {
    fs.rmSync(fixture.unitFile);
    fixture.unloadUnit();
    fs.writeFileSync(path.join(fixture.state, ".service"), "legacy marker\n");
    fs.chmodSync(path.join(fixture.state, ".service"), 0o600);
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.match(result.stderr, /runner service marker has no matching manager unit/);
    assert.notEqual(result.status, 0, "the runner-writable marker cannot establish a service");
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo)/m);
    assert.doesNotMatch(events, /^systemctl:stop /m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap removes a stopped managed unit after startup failure", () => {
  const fixture = bootstrapFixture();
  try {
    fs.writeFileSync(fixture.activeState, "inactive\n");
    const result = fixture.invoke({ SYSTEMCTL_FAIL: "start" });
    assert.notEqual(result.status, 0, "a failed manager start is not bootstrap success");
    assert.match(result.stderr, /runner service start failed/);
    const events = fs.readFileSync(fixture.trace, "utf8");
    const startIndex = events.indexOf(`systemctl:start ${fixture.unitName}`);
    const stopIndex = events.lastIndexOf(`systemctl:stop ${fixture.unitName}`);
    const disableIndex = events.lastIndexOf(`systemctl:disable ${fixture.unitName}`);
    assert.ok(startIndex >= 0 && startIndex < stopIndex && stopIndex < disableIndex);
    assert.equal(
      fs.existsSync(fixture.unitFile),
      false,
      "proved cleanup removes the failed managed unit",
    );
    assert.equal(
      fs.readFileSync(fixture.activeState, "utf8"),
      "inactive\n",
      "the manager adapter did not report a successful start",
    );
    assert.doesNotMatch(events, /^systemctl:restart /m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap rejects malformed cgroup state after stopping without running tools", () => {
  const fixture = bootstrapFixture();
  try {
    fs.writeFileSync(path.join(fixture.cgroup, "cgroup.events"), "populated maybe\n");
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0, "unparseable cgroup state is not proof of quiescence");
    assert.match(result.stderr, /runner cgroup state is malformed/);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^systemctl:stop /m);
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo)/m);
    assert.doesNotMatch(events, /^systemctl:start /m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap refuses a modified same-version Rustup before invoking it", () => {
  const fixture = bootstrapFixture();
  try {
    const rustup = path.join(
      fixture.root,
      "home",
      os.userInfo().username,
      ".cargo",
      "bin",
      "rustup",
    );
    const marker = path.join(fixture.root, "rustup-executed");
    fs.writeFileSync(
      rustup,
      `#!/bin/sh\nprintf executed > '${marker}'\ncase "$*" in *'toolchain list'*) echo 1.93.1;; *'target list'*) echo wasm32-unknown-unknown;; *) exit 92;; esac\n`,
      { mode: 0o755 },
    );

    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(
      result.status,
      0,
      "modified Rustup is rejected despite reporting the pinned version",
    );
    assert.equal(fs.existsSync(marker), false, "modified Rustup is never invoked");
    assert.doesNotMatch(fs.readFileSync(fixture.trace, "utf8"), /^systemctl:start /m);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap refuses a modified same-version wasm-pack before invoking it", () => {
  const fixture = bootstrapFixture();
  try {
    const wasmPack = path.join(
      fixture.root,
      "home",
      os.userInfo().username,
      ".cargo",
      "bin",
      "wasm-pack",
    );
    const marker = path.join(fixture.root, "wasm-pack-executed");
    fs.writeFileSync(
      wasmPack,
      `#!/bin/sh\nprintf executed > '${marker}'\necho 'wasm-pack 0.13.1'\n`,
      {
        mode: 0o755,
      },
    );

    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(
      result.status,
      0,
      "modified wasm-pack is rejected despite reporting the pinned version",
    );
    assert.equal(fs.existsSync(marker), false, "modified wasm-pack is never invoked");
    assert.doesNotMatch(fs.readFileSync(fixture.trace, "utf8"), /^systemctl:start /m);
  } finally {
    fixture.cleanup();
  }
});
test("bootstrap treats an empty Cargo bin directory as absent tool state", () => {
  const fixture = bootstrapFixture();
  try {
    const cargoBin = path.join(fixture.root, "home", os.userInfo().username, ".cargo", "bin");
    fs.rmSync(path.join(cargoBin, "rustup"));
    fs.rmSync(path.join(cargoBin, "wasm-pack"));
    fs.rmSync(path.join(fixture.root, "home", os.userInfo().username, ".rustup"), {
      recursive: true,
    });
    fs.rmSync(path.join(fixture.root, "var", "lib", "actions-runner", ".toolchain-integrity"), {
      recursive: true,
    });
    assert.deepEqual(fs.readdirSync(cargoBin), []);

    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /download integrity check failed/);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^curl:/m, "empty directory proceeds to pinned tool download");
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo)/m);
    assert.doesNotMatch(events, /^systemctl:start /m);
  } finally {
    fixture.cleanup();
  }
});
test("bootstrap rechecks newly appeared Rust state after stopping the runner", () => {
  const fixture = bootstrapFixture();
  try {
    const cargoBin = path.join(fixture.root, "home", os.userInfo().username, ".cargo", "bin");
    fs.rmSync(cargoBin, { recursive: true });
    fs.rmSync(path.join(fixture.root, "home", os.userInfo().username, ".rustup"), {
      recursive: true,
    });
    fs.rmSync(path.join(fixture.root, "var", "lib", "actions-runner", ".toolchain-integrity"), {
      recursive: true,
    });
    const marker = path.join(fixture.root, "rustup-executed-after-stop");
    const result = fixture.invoke(
      { TAMPER_RUSTUP_ON_STOP: "1", TOOLCHAIN_EXECUTED_MARKER: marker },
      fixture.recordAllowlist(),
    );
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /existing Rust tool state has no protected integrity manifests/);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^systemctl:stop /m);
    assert.doesNotMatch(events, /^curl:/m);
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo)/m);
    assert.equal(fs.existsSync(marker), false);
    assert.equal(fs.existsSync(path.join(cargoBin, "rustup")), true);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap entry refuses partial, modified, and unmanifested packages without replacement", () => {
  for (const damage of ["partial", "modified", "unmanifested"]) {
    const fixture = bootstrapFixture();
    try {
      const listener = path.join(fixture.pkg, "bin", "Runner.Listener");
      fs.chmodSync(fixture.pkg, 0o755);
      fs.chmodSync(path.join(fixture.pkg, "bin"), 0o755);
      if (damage === "partial") fs.unlinkSync(listener);
      if (damage === "modified") {
        fs.chmodSync(listener, 0o644);
        fs.writeFileSync(listener, "changed after pin");
      }
      if (damage === "unmanifested")
        fs.writeFileSync(path.join(fixture.pkg, "unexpected"), "not in manifest");
      fs.chmodSync(path.join(fixture.pkg, "bin"), 0o555);
      fs.chmodSync(fixture.pkg, 0o555);
      const before = damage === "partial" ? undefined : fs.readFileSync(listener, "utf8");
      const result = fixture.invoke({}, fixture.recordAllowlist());
      assert.notEqual(result.status, 0, `${damage} package must be rejected`);
      const events = fs.readFileSync(fixture.trace, "utf8");
      assert.doesNotMatch(events, /^(curl|apt-get|systemctl|snap):/m);
      assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo|config\.sh)/m);
      assert.doesNotMatch(events, /^svc:/m);
      assert.equal(
        fs.existsSync(listener),
        damage !== "partial",
        "invalid package was not destructively replaced",
      );
      if (before !== undefined) assert.equal(fs.readFileSync(listener, "utf8"), before);
    } finally {
      fixture.cleanup();
    }
  }
});

test("bootstrap entry refuses partial runner configuration before configuration or service", () => {
  const fixture = bootstrapFixture();
  try {
    fs.rmSync(path.join(fixture.state, ".credentials"));
    const result = fixture.invoke(
      { RUNNER_TOKEN: "must-not-reconfigure" },
      fixture.recordAllowlist(),
    );
    assert.notEqual(result.status, 0, "incomplete registration fails closed despite a token");
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.doesNotMatch(events, /^(systemctl|snap):/m);
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo|config\.sh)/m);
    assert.doesNotMatch(events, /^config:/m, "a supplied token cannot reconfigure partial state");
    assert.doesNotMatch(events, /^svc:/m);
    assert.equal(fs.existsSync(path.join(fixture.state, ".credentials")), false);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap entry treats unsafe account names as data", () => {
  const fixture = bootstrapFixture();
  try {
    const marker = path.join(fixture.root, "evaluated");
    const result = fixture.invoke({ RUNNER_USER: `$(touch ${marker})` });
    assert.notEqual(result.status, 0);
    assert.equal(fs.existsSync(marker), false);
    assert.equal(fs.existsSync(fixture.trace), false);
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap entry rejects a missing pinned wasm-pack before configuration or service", () => {
  const fixture = bootstrapFixture();
  try {
    const wasmPack = path.join(
      fixture.root,
      "home",
      os.userInfo().username,
      ".cargo",
      "bin",
      "wasm-pack",
    );
    const cargoBin = path.dirname(wasmPack);
    const manifest = path.join(
      fixture.root,
      "var",
      "lib",
      "actions-runner",
      ".toolchain-integrity",
      "cargo-bin.json",
    );
    fs.rmSync(wasmPack);
    fs.rmSync(manifest);
    const regenerated = helper(fixture.root, "manifest", cargoBin, manifest);
    assert.equal(regenerated.status, 0, regenerated.stderr);
    fs.chmodSync(manifest, 0o444);
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /existing wasm-pack installation is incomplete/);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.doesNotMatch(events, /^(curl|apt-get|snap):/m);
    assert.match(events, /^systemctl:stop /m);
    assert.doesNotMatch(events, /^systemctl:(enable|start) /m);
    assert.doesNotMatch(events, /^svc:/m);
  } finally {
    fixture.cleanup();
  }
});

test("verified wasm-pack installer propagates Cargo failure", () => {
  const fixture = bootstrapFixture();
  try {
    const result = fixture.invokeOrchestration({
      wasm: true,
      failEffect: "cargo:install wasm-pack --version 0.13.1 --locked",
    });
    assert.notEqual(result.status, 0);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^cargo:install wasm-pack --version 0.13.1 --locked$/m);
  } finally {
    fixture.cleanup();
  }
});

test("required SSM dependency failures do not mark setup complete", () => {
  for (const failEffect of [
    "apt-get:install -y snapd",
    "systemctl:enable --now snapd.socket",
    "snap:wait system seed.loaded",
    "snap:install amazon-ssm-agent --classic",
    "systemctl:enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service",
  ]) {
    const fixture = bootstrapFixture();
    try {
      const completionMarker = path.join(
        fixture.root,
        "var",
        "lib",
        "actions-runner",
        ".os-dependencies-v1",
      );
      fs.rmSync(completionMarker);
      fixture.recordAllowlist();
      const result = fixture.invokeOrchestration({ ssm: true, failEffect });
      assert.notEqual(result.status, 0, `${failEffect} must fail setup`);
      const events = fs.readFileSync(fixture.trace, "utf8");
      assert.ok(events.split("\n").includes(failEffect), `${failEffect} must be reached`);
      assert.equal(fs.existsSync(completionMarker), false);
    } finally {
      fixture.cleanup();
    }
  }
});

test("auto-detected AWS SSM failure leaves dependencies unmarked", () => {
  const fixture = bootstrapFixture();
  try {
    const dmi = path.join(fixture.root, "sys", "devices", "virtual", "dmi", "id");
    fs.mkdirSync(dmi, { recursive: true });
    fs.writeFileSync(path.join(dmi, "sys_vendor"), "Amazon EC2\n");
    fs.writeFileSync(path.join(dmi, "product_name"), "r7i.metal\n");
    fs.rmSync(path.join(fixture.root, "var", "lib", "actions-runner", ".os-dependencies-v1"));
    fixture.recordAllowlist();
    const result = fixture.invokeOrchestration({
      autoSsm: true,
      failEffect: "systemctl:enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service",
    });
    assert.notEqual(result.status, 0, "auto-detected AWS SSM failure is fatal");
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^snap:install amazon-ssm-agent --classic$/m);
    assert.match(
      events,
      /^systemctl:enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service$/m,
    );
    assert.equal(
      fs.existsSync(path.join(fixture.root, "var", "lib", "actions-runner", ".os-dependencies-v1")),
      false,
      "failed required setup does not mark dependencies complete",
    );
  } finally {
    fixture.cleanup();
  }
});
