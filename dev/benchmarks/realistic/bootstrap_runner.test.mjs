import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";

const source = fs.readFileSync(new URL("./bootstrap_runner.sh", import.meta.url), "utf8");
const token = "fixture-token-'$(touch token-evaluated)'\"";
const pins = Object.fromEntries(
  [...source.matchAll(/^readonly (\w+_VERSION)=(.+)$/gm)].map((match) => [match[1], match[2]]),
);

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-bootstrap-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const home = path.join(root, "home");
  const bin = path.join(root, "bin");
  const archives = path.join(root, "archives");
  for (const directory of [home, bin, archives]) fs.mkdirSync(directory);
  const shell = (file, body) => {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, `#!/bin/bash\nset -euo pipefail\n${body}\n`, { mode: 0o755 });
  };
  const trace = `printf '%s:%s\\n' "$(basename "$0")" "$*" >> "$FIXTURE/trace"
if [[ -n "\${RUNNER_TOKEN:-}" ]]; then exit 91; fi
if [[ -n "\${ACTIONS_RUNNER_INPUT_TOKEN:-}" && "$*" != *./config.sh* ]]; then exit 92; fi`;
  const stub = (name, body) => shell(path.join(bin, name), `${trace}\n${body}`);
  stub(
    "id",
    `case "$1" in
  -u) [[ "\${FIXTURE_UID:-1001}" != missing || -f "$FIXTURE/user-created" ]] || exit 1
      echo "\${FIXTURE_UID/missing/1001}" ;;
  -gn) echo fixture-primary-group ;;
  *) exit 93 ;;
esac`,
  );
  stub("getent", 'printf "runner:x:1001:2001::%s:/bin/bash\\n" "$FIXTURE/home"');
  stub("useradd", 'touch "$FIXTURE/user-created"');
  stub("apt-get", '[[ "\${FAIL_COMMAND:-}" != apt-get ]]');
  stub("systemctl", '[[ "\${FAIL_COMMAND:-}" != systemctl ]]');
  stub("snap", '[[ "\${FAIL_COMMAND:-}" != snap ]]');
  stub(
    "curl",
    `while [[ "$1" != --output ]]; do shift; done
output="$2"
name="$(basename "$output")"
[[ "\${MISSING_DOWNLOAD:-}" != "$name" ]] || exit 22
cp "$FIXTURE/archives/$name" "$output"
if [[ "\${CORRUPT_DOWNLOAD:-}" == "$name" ]]; then printf corrupt >> "$output"; fi`,
  );
  stub("tar", 'exec /bin/tar "$@"');
  stub(
    "runuser",
    `[[ "$1" == -u && "$2" == runner && "$3" == -- ]] || exit 94
shift 3
exec "$@"`,
  );
  stub(
    "install",
    `args=()
while (( $# )); do
  case "$1" in -o|-g) shift 2 ;; *) args+=("$1"); shift ;; esac
done
exec /usr/bin/install "\${args[@]}"`,
  );

  shell(
    path.join(archives, "rustup-init"),
    `${trace}
[[ "\${FAIL_COMMAND:-}" != rustup-init ]]
mkdir -p "$HOME/.cargo/bin"
cp "$FIXTURE/rustup" "$HOME/.cargo/bin/rustup"`,
  );
  shell(
    path.join(root, "rustup"),
    `${trace}
[[ "\${FAIL_COMMAND:-}" != rustup ]]`,
  );
  const archive = (name, directory) => {
    const result = spawnSync(
      "/bin/tar",
      ["czf", path.join(archives, name), "-C", directory, ...fs.readdirSync(directory)],
      { encoding: "utf8" },
    );
    assert.equal(result.status, 0, result.stderr);
  };
  const nodeTree = path.join(root, "node", `node-v${pins.NODE_VERSION}-linux-x64`);
  shell(
    path.join(nodeTree, "bin/corepack"),
    `${trace}
[[ "\${FAIL_COMMAND:-}" != corepack ]]`,
  );
  archive("node.tar.gz", path.dirname(nodeTree));
  const wasmTree = path.join(
    root,
    "wasm",
    `wasm-pack-v${pins.WASM_PACK_VERSION}-x86_64-unknown-linux-musl`,
  );
  shell(path.join(wasmTree, "wasm-pack"), 'echo "wasm-pack fixture"');
  archive("wasm-pack.tar.gz", path.dirname(wasmTree));
  const runnerTree = path.join(root, "runner");
  shell(
    path.join(runnerTree, "config.sh"),
    `printf 'config:%s\\n' "$*" >> "$FIXTURE/trace"
[[ -z "\${RUNNER_TOKEN:-}" ]]
printf '%s' "$ACTIONS_RUNNER_INPUT_TOKEN" > "$FIXTURE/config-token"
printf '%s\\n' "$@" > "$FIXTURE/config-argv"
printf '%s\\n' "$ACTIONS_RUNNER_INPUT_TOKEN"
[[ "\${FAIL_COMMAND:-}" != config ]]
cp "$FIXTURE/svc.sh" ./svc.sh
touch .runner`,
  );
  shell(
    path.join(root, "svc.sh"),
    `${trace}
[[ "\${FAIL_COMMAND:-}" != service ]]`,
  );
  archive("runner.tar.gz", runnerTree);

  // The executable fixture changes only root admission, the global Node path,
  // and expected digests. Production has no test mode or path override.
  assert.ok(source.includes('[[ "${EUID}" == 0 ]]'));
  assert.ok(source.includes('readonly NODE_ROOT="/opt/node-v${NODE_VERSION}"'));
  let script = source
    .replace('[[ "\${EUID}" == 0 ]]', "true")
    .replace(
      'readonly NODE_ROOT="/opt/node-v\${NODE_VERSION}"',
      `readonly NODE_ROOT="${root}/installed-node"`,
    );
  for (const [pin, name] of [
    ["RUSTUP", "rustup-init"],
    ["NODE", "node.tar.gz"],
    ["RUNNER", "runner.tar.gz"],
    ["WASM_PACK", "wasm-pack.tar.gz"],
  ]) {
    const digest = createHash("sha256")
      .update(fs.readFileSync(path.join(archives, name)))
      .digest("hex");
    script = script.replace(
      new RegExp(`^readonly ${pin}_SHA256=.+$`, "m"),
      `readonly ${pin}_SHA256=${digest}`,
    );
  }
  const scriptPath = path.join(root, "bootstrap.sh");
  fs.writeFileSync(scriptPath, script);
  return {
    root,
    home,
    run: (overrides = {}) =>
      spawnSync("bash", [scriptPath], {
        cwd: root,
        encoding: "utf8",
        timeout: 10000,
        env: {
          ...process.env,
          PATH: `${bin}:${process.env.PATH}`,
          TMPDIR: root,
          FIXTURE: root,
          FIXTURE_UID: "1001",
          RUNNER_USER: "runner",
          RUNNER_URL: "https://github.com/example/fixture",
          RUNNER_NAME: "fixture-runner",
          RUNNER_TOKEN: token,
          INSTALL_WASM_PACK: "1",
          INSTALL_SSM_AGENT: "0",
          SKIP_HARDENING: "1",
          ...overrides,
        },
      }),
    trace: () =>
      fs.existsSync(path.join(root, "trace"))
        ? fs.readFileSync(path.join(root, "trace"), "utf8")
        : "",
  };
}

test("verified installation uses the non-root account and passes the token only through config's environment", (t) => {
  const f = fixture(t);
  const result = f.run({ FIXTURE_UID: "missing" });
  assert.equal(result.status, 0, result.stderr);
  assert.ok(fs.existsSync(path.join(f.root, "user-created")));
  assert.equal(fs.readFileSync(path.join(f.root, "config-token"), "utf8"), token);
  const argv = fs.readFileSync(path.join(f.root, "config-argv"), "utf8");
  assert.doesNotMatch(argv, /--token|--replace/);
  assert.ok(!argv.includes(token));
  assert.ok(!f.trace().includes(token));
  assert.match(
    f.trace(),
    /runuser:-u runner -- env HOME=.*rustup-init .*--default-toolchain 1\.93\.1/,
  );
  assert.match(
    f.trace(),
    /runuser:-u runner -- .*rustup target add wasm32-unknown-unknown --toolchain 1\.93\.1/,
  );
  assert.match(f.trace(), /install:-o runner -g fixture-primary-group -m 0755 .*wasm-pack/);
  assert.match(f.trace(), /runuser:-u runner -- tar xzf .*runner.tar.gz/);
  assert.match(f.trace(), /svc.sh:install runner\nsvc.sh:start/);
  assert.ok(fs.existsSync(path.join(f.home, ".cargo/bin/wasm-pack")));
  assert.ok(!fs.existsSync(path.join(f.root, "token-evaluated")));
  assert.ok(!result.stdout.includes(token));
  assert.ok(
    !fs.readdirSync(f.root).some((name) => name.startsWith("tmp.")),
    "temporary downloads are removed",
  );
});

for (const name of ["rustup-init", "node.tar.gz", "runner.tar.gz", "wasm-pack.tar.gz"]) {
  test(`corrupt ${name} is rejected before any downloaded payload is consumed`, (t) => {
    const f = fixture(t);
    const result = f.run({ CORRUPT_DOWNLOAD: name });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /download integrity check failed/);
    assert.doesNotMatch(f.trace(), /^(runuser|tar|corepack|config|svc.sh):/m);
  });
}

test("a missing download fails setup before execution", (t) => {
  const f = fixture(t);
  assert.notEqual(f.run({ MISSING_DOWNLOAD: "node.tar.gz" }).status, 0);
  assert.doesNotMatch(f.trace(), /^(runuser|tar|config|svc.sh):/m);
});

test("wasm-pack is optional and SSM is installed when requested", (t) => {
  const f = fixture(t);
  const result = f.run({ INSTALL_WASM_PACK: "0", INSTALL_SSM_AGENT: "1" });
  assert.equal(result.status, 0, result.stderr);
  assert.doesNotMatch(f.trace(), /wasm-pack/);
  assert.match(f.trace(), /snap:wait system seed.loaded/);
  assert.match(f.trace(), /systemctl:enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service/);
});

for (const command of [
  "apt-get",
  "snap",
  "systemctl",
  "rustup-init",
  "rustup",
  "corepack",
  "config",
  "service",
]) {
  test(`required ${command} failure aborts setup`, (t) => {
    const f = fixture(t);
    const result = f.run({ FAIL_COMMAND: command, INSTALL_SSM_AGENT: "1" });
    assert.notEqual(result.status, 0);
    assert.match(f.trace(), new RegExp(`^${command === "service" ? "svc\\.sh" : command}:`, "m"));
    assert.ok(!result.stdout.includes(token));
    assert.ok(!result.stderr.includes(token));
    if (command !== "service") assert.doesNotMatch(f.trace(), /^svc.sh:/m);
    if (command === "config") assert.match(result.stderr, /diagnostic output suppressed/);
  });
}

test("a root runner account is rejected before installation", (t) => {
  const f = fixture(t);
  const result = f.run({ FIXTURE_UID: "0" });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /non-root UID/);
  assert.doesNotMatch(f.trace(), /^(apt-get|curl|runuser):/m);
});

test("settings are treated as data and rejected before installation", (t) => {
  for (const setting of [
    "RUNNER_USER",
    "RUNNER_URL",
    "RUNNER_NAME",
    "RUNNER_LABELS",
    "INSTALL_WASM_PACK",
    "INSTALL_SSM_AGENT",
  ]) {
    const f = fixture(t);
    const result = f.run({ [setting]: "$(touch setting-evaluated)" });
    assert.notEqual(result.status, 0, setting);
    assert.ok(!fs.existsSync(path.join(f.root, "setting-evaluated")));
    assert.doesNotMatch(f.trace(), /^(apt-get|curl|runuser):/m);
  }
});

test("missing token and an existing runner directory are rejected before installation", (t) => {
  const f = fixture(t);
  assert.notEqual(f.run({ RUNNER_TOKEN: "" }).status, 0);
  fs.mkdirSync(path.join(f.home, "actions-runner"));
  const result = f.run();
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /provision a fresh host/);
  assert.doesNotMatch(f.trace(), /^(apt-get|curl|runuser):/m);
});
