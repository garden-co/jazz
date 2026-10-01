import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

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

function bootstrapFixture() {
  assert.notEqual(process.getuid(), 0, "sandboxed bootstrap tests must run unprivileged");
  const root = temporaryDirectory("jazz-bootstrap-entry-");
  assert.deepEqual(
    fs.readdirSync(root),
    [],
    "each invocation starts from a fresh harness-created root",
  );
  const trace = path.join(root, "trace.log");
  const runner = os.userInfo().username;
  const home = path.join(root, "home", runner);
  const state = path.join(root, "var", "lib", "actions-runner", runner);
  const pkg = path.join(root, "opt", "actions-runner", "2.337.0");
  fs.mkdirSync(path.join(home, ".cargo", "bin"), { recursive: true });
  fs.mkdirSync(path.join(root, "var", "tmp"), { recursive: true });
  fs.mkdirSync(path.join(root, "dev"), { recursive: true });
  fs.writeFileSync(path.join(root, "dev", "null"), "");
  fs.mkdirSync(state, { recursive: true });
  fs.mkdirSync(path.join(pkg, "bin"), { recursive: true });
  fs.writeFileSync(
    path.join(pkg, "config.sh"),
    `#!/bin/sh\nprintf 'config:%s\\nconfig-cwd:%s\\n' "$*" "$PWD" >> "$TRACE"\nif [ "\${ALLOW_CONFIG:-}" = 1 ]; then printf '{"agentName":"fixture-runner","serverUrl":"https://github.com/garden-co/jazz2","workFolder":"_work"}\\n' > "$BOOTSTRAP_FIXTURE_STATE/.runner"; printf 'fixture credentials\\n' > "$BOOTSTRAP_FIXTURE_STATE/.credentials"; exit 0; fi\nexit 91\n`,
    { mode: 0o755 },
  );
  fs.writeFileSync(
    path.join(pkg, "svc.sh"),
    `#!/bin/sh
printf 'svc:%s\\nsvc-cwd:%s\\n' "$*" "$PWD" >> "$TRACE"
if [ "\${1:-}" = stop ] && [ "\${TAMPER_RUSTUP_ON_STOP:-}" = 1 ]; then
  printf '#!/bin/sh\\nprintf executed > "%s"\\n' "$TOOLCHAIN_EXECUTED_MARKER" > "$BOOTSTRAP_TEST_ROOT/home/$RUNNER_USER/.cargo/bin/rustup"
  chmod 0755 "$BOOTSTRAP_TEST_ROOT/home/$RUNNER_USER/.cargo/bin/rustup"
fi
[ "\${FAIL_SERVICE:-}" != "\${1:-}" ]
`,
    { mode: 0o755 },
  );
  fs.writeFileSync(path.join(pkg, "bin", "Runner.Listener"), "pinned runner fixture");
  fs.writeFileSync(
    path.join(state, ".runner"),
    JSON.stringify({
      agentName: "fixture-runner",
      serverUrl: "https://github.com/garden-co/jazz2",
      workFolder: "_work",
    }),
  );
  fs.writeFileSync(path.join(state, ".credentials"), "fixture credentials");
  fs.writeFileSync(path.join(state, ".service"), "fixture service");
  for (const file of [".runner", ".credentials", ".service"]) {
    fs.chmodSync(path.join(state, file), 0o600);
  }
  fs.chmodSync(state, 0o2700);
  for (const item of [
    ".runner",
    ".credentials",
    ".credentials_rsaparams",
    ".service",
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
    path.join(pkg, "svc.sh"),
    path.join(pkg, "bin", "Runner.Listener"),
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
    fs.writeFileSync(
      path.join(bin, name),
      `#!/bin/sh\nprintf '%s\\n' '${name}:'"$*" >> "$TRACE"\n${body}\n`,
      { mode: 0o755 },
    );
  };
  logger(
    "getent",
    `case "$*" in "passwd $RUNNER_USER") printf '%s:x:1000:1000::%s:/bin/bash\\n' "$RUNNER_USER" "$BOOTSTRAP_TEST_ROOT/home/$RUNNER_USER" ;; *) exit 90 ;; esac`,
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
    'case "$*" in "enable --now snapd.socket"|"enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service"|"is-active --quiet snap.amazon-ssm-agent.amazon-ssm-agent.service") ;; *) exit 90 ;; esac; [ "\${FAIL_EFFECT:-}" != "systemctl:$*" ]',
  );
  logger(
    "snap",
    'case "$*" in "wait system seed.loaded"|"install amazon-ssm-agent --classic") ;; *) exit 90 ;; esac; [ "\${FAIL_EFFECT:-}" != "snap:$*" ]',
  );
  logger("corepack", '[ "$#" -eq 1 ] && [ "$1" = enable ]');
  logger("jq", '[ "$1" = -e ] && [ "\${FAIL_JQ:-}" != 1 ]');
  logger("cargo", '[ "$1" = install ] && [ "\${FAIL_EFFECT:-}" != "cargo:$*" ]');
  logger(
    "install",
    'if [ "$1" = -d ] && [ "$2" = -o ] && [ "$3" = "$RUNNER_USER" ] && [ "$4" = -g ] && [ "$5" = root ]; then shift 5; set -- -d -o "$RUNNER_USER" -g "$(id -gn)" "$@"; fi; for arg in "$@"; do case "$arg" in /*) case "$arg" in "$BOOTSTRAP_TEST_ROOT"/*) ;; *) exit 90 ;; esac ;; esac; done; exec /usr/bin/install "$@"',
  );
  const sandboxed = (name) =>
    `for arg in "$@"; do case "$arg" in /*) case "$arg" in "$BOOTSTRAP_TEST_ROOT"/*) ;; *) exit 90 ;; esac ;; esac; done; exec /usr/bin/${name} "$@"`;
  for (const name of ["chown", "chmod", "ln", "mv", "rmdir", "rm", "mktemp"])
    logger(name, sandboxed(name));
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
        RUNNER_NAME: "fixture-runner",
        RUNNER_URL: "https://github.com/garden-co/jazz2",
        RUNNER_TOKEN: "fixture-token",
        INSTALL_SSM_AGENT: "0",
        SKIP_HARDENING: "1",
        ...overrides,
      },
    });
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
    node: path.join(node, "node"),
    invoke,
    invokeOrchestration,
    recordAllowlist,
    cleanup: () => {
      makeRemovable(root);
      fs.rmSync(root, { recursive: true, force: true });
      fs.rmSync(bin, { recursive: true, force: true });
    },
  };
}
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

test("bootstrap runs runner configuration and service commands from the package directory", () => {
  const fixture = bootstrapFixture();
  try {
    for (const file of [".runner", ".credentials", ".service"])
      fs.rmSync(path.join(fixture.state, file));
    const result = fixture.invoke({ ALLOW_CONFIG: "1" }, fixture.recordAllowlist());
    assert.equal(result.status, 0, `${result.stderr}\n${result.stdout}`);
    assert.ok(fs.existsSync(fixture.trace), `${result.stderr}\n${result.stdout}`);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.ok(events.split("\n").includes(`config-cwd:${fixture.pkg}`));
    assert.ok(events.split("\n").includes(`svc-cwd:${fixture.pkg}`));
    assert.ok(events.split("\n").includes(`svc:install ${os.userInfo().username}`));
    assert.match(events, /^svc:start$/m);
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
    assert.doesNotMatch(events, /^(apt-get|systemctl|snap):/m);
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
    assert.doesNotMatch(events, /^(apt-get|systemctl|snap):/m);
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

test("bootstrap entry reuses complete pinned state offline without changing its manifest", () => {
  const fixture = bootstrapFixture();
  try {
    const manifestPath = path.join(fixture.pkg, ".bootstrap-manifest");
    const manifestBefore = fs.readFileSync(manifestPath, "utf8");
    const result = fixture.invoke();
    assert.equal(result.status, 0, result.stderr);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.doesNotMatch(events, /^(curl|apt-get):/m);
    assert.match(events, /^svc:start$/m);
    assert.equal(fs.readFileSync(manifestPath, "utf8"), manifestBefore);
  } finally {
    fixture.cleanup();
  }
});
test("bootstrap stops the runner and rechecks tool manifests before execution", () => {
  const fixture = bootstrapFixture();
  try {
    const marker = path.join(fixture.root, "rustup-executed-after-stop");
    const result = fixture.invoke(
      { TAMPER_RUSTUP_ON_STOP: "1", TOOLCHAIN_EXECUTED_MARKER: marker },
      fixture.recordAllowlist(),
    );
    assert.notEqual(result.status, 0, "post-stop toolchain tampering must fail closed");
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^svc:stop$/m, "an existing runner is quiesced before tool checks");
    assert.equal(
      fs.existsSync(marker),
      false,
      "modified Rustup is not executed after service stop",
    );
    assert.doesNotMatch(events, /^runuser:.*\/\.cargo\/bin\/rustup/m);
    assert.doesNotMatch(
      events,
      /^svc:start$/m,
      "failed post-stop verification keeps service stopped",
    );
  } finally {
    fixture.cleanup();
  }
});

test("bootstrap fails closed when the existing runner service cannot stop", () => {
  const fixture = bootstrapFixture();
  try {
    const result = fixture.invoke({ FAIL_SERVICE: "stop" });
    assert.notEqual(result.status, 0, "service stop failure aborts bootstrap");
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.match(events, /^svc:stop$/m);
    assert.doesNotMatch(events, /^runuser:.*\/\.cargo\/bin\/rustup/m);
    assert.doesNotMatch(events, /^svc:start$/m);
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
    assert.doesNotMatch(fs.readFileSync(fixture.trace, "utf8"), /^svc:start$/m);
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
    assert.doesNotMatch(fs.readFileSync(fixture.trace, "utf8"), /^svc:start$/m);
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
    const result = fixture.invoke({}, fixture.recordAllowlist());
    assert.notEqual(result.status, 0);
    const events = fs.readFileSync(fixture.trace, "utf8");
    assert.doesNotMatch(events, /^(systemctl|snap):/m);
    assert.doesNotMatch(events, /^runuser:.*(?:rustup|wasm-pack|cargo|config\.sh)/m);
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
    assert.doesNotMatch(events, /^(curl|apt-get|systemctl|snap):/m);
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
