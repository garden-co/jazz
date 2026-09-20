#!/usr/bin/env node
// Local disposable acceptance only. Cloud lifecycle is intentionally not automated here.
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { spawn, execFileSync } from "node:child_process";
import { once } from "node:events";
import {
  readFileSync,
  writeFileSync,
  mkdirSync,
  existsSync,
  realpathSync,
  openSync,
  closeSync,
  mkdtempSync,
  readdirSync,
  lstatSync,
  rmSync,
  unlinkSync,
} from "node:fs";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
const ownDir = dirname(fileURLToPath(import.meta.url));
const input = JSON.parse(readFileSync(process.argv[2], "utf8"));
for (const key of [
  "phase",
  "sourceSha",
  "packageVersion",
  "previewRun",
  "project",
  "cli",
  "cliSha256",
  "nativeFingerprint",
  "output",
  "packages",
])
  assert(input[key], `Missing ${key}`);
assert(["baseline", "final-preview", "published"].includes(input.phase));
assert.match(input.sourceSha, /^[0-9a-f]{40}$/);
if (input.phase !== "baseline") assert.match(input.packageVersion, /alpha\.56$/);
const hash = (file) => createHash("sha256").update(readFileSync(file)).digest("hex");
assert.equal(hash(input.cli), input.cliSha256, "CLI digest mismatch");
if (input.phase !== "baseline") {
  assert(
    input.cliProducer,
    "Final receipts require authoritative CLI producer manifest; no inferred source binding",
  );
  assert.equal(
    hash(input.cliProducer.path),
    input.cliProducer.sha256,
    "CLI producer manifest digest",
  );
  const producer = JSON.parse(readFileSync(input.cliProducer.path, "utf8"));
  assert.equal(producer.git?.head, input.sourceSha, "CLI producer source revision");
  assert(
    producer.artifacts?.some(
      (a) => a.file === input.cliProducer.artifact && a.sha256 === input.cliSha256,
    ),
    "CLI not bound to producer artifact inventory",
  );
}
const project = realpathSync(input.project),
  require = createRequire(join(project, "package.json"));
for (const name of ["jazz-tools", "jazz-napi", "jazz-wasm"])
  assert(input.packages[name], `Missing package pin ${name}`);
const packageDirs = new Map();
for (const [name, pin] of Object.entries(input.packages)) {
  assert.equal(hash(pin.tarball), pin.sha256, `${name} tarball digest mismatch`);
  let directory = dirname(require.resolve(name));
  while (!existsSync(join(directory, "package.json"))) {
    const parent = dirname(directory);
    assert.notEqual(parent, directory);
    directory = parent;
  }
  packageDirs.set(name, directory);
  const manifest = JSON.parse(readFileSync(join(directory, "package.json"), "utf8"));
  assert.equal(manifest.version, input.packageVersion, `${name} version`);
  const unpacked = mkdtempSync(join(tmpdir(), "jazz-acceptance-package-"));
  try {
    execFileSync("tar", ["-xzf", resolve(pin.tarball), "-C", unpacked], { stdio: "pipe" });
    const compare = (packed, installed) => {
      const entries = readdirSync(packed);
      for (const extra of readdirSync(installed))
        assert(
          entries.includes(extra) || extra === "node_modules",
          `Unexpected installed package file ${name}/${extra}`,
        );
      for (const entry of entries) {
        const a = join(packed, entry),
          b = join(installed, entry),
          stat = lstatSync(a);
        assert(!stat.isSymbolicLink(), "Packed symlinks are not accepted");
        assert(existsSync(b), `Missing installed package file ${name}/${entry}`);
        assert(!lstatSync(b).isSymbolicLink(), "Installed package file symlink");
        if (stat.isDirectory()) compare(a, b);
        else
          assert.equal(
            hash(a),
            hash(b),
            `Installed bytes differ from pinned tarball: ${name}/${entry}`,
          );
      }
    };
    compare(join(unpacked, "package"), directory);
  } finally {
    rmSync(unpacked, { recursive: true, force: true });
  }
  assert(
    realpathSync(directory).startsWith(`${project}/node_modules/`),
    `${name} must be installed outside workspace`,
  );
}
if (process.platform === "linux") {
  const name = `@garden-co/jazz-napi-linux-${process.arch}-gnu`;
  assert(input.packages[name], "Pin the selected Linux native payload package");
  const native = JSON.parse(
    readFileSync(
      join(packageDirs.get(name), `jazz-napi.linux-${process.arch}-gnu.manifest.json`),
      "utf8",
    ),
  );
  assert.equal(native.git.head, input.sourceSha, "Native producer source revision");
  assert.equal(native.nativeArtifactFingerprint, input.nativeFingerprint);
  assert.equal(native.profile, "release");
  for (const artifact of native.artifacts)
    assert.equal(
      hash(join(packageDirs.get(name), artifact.file)),
      artifact.sha256,
      "Native producer artifact digest",
    );
}
assert.equal(
  require("jazz-napi").nativeArtifactFingerprint(),
  input.nativeFingerprint,
  "Installed native fingerprint mismatch",
);
const output = resolve(input.output);
assert(!existsSync(output), "Output must be new; never reset historical stores");
mkdirSync(output, { recursive: true, mode: 0o700 });
const c = {
  ...input,
  project,
  state: join(output, "state"),
  appId: randomUUID(),
  adminSecret: randomUUID(),
  backendSecret: randomUUID(),
};
mkdirSync(c.state, { mode: 0o700 });
const configPath = join(output, "private-config.json"),
  portPath = join(output, "port");
let server;
const children = new Set();
const emit = (check, detail = {}) =>
  console.log(JSON.stringify({ phase: input.phase, check, ...detail }));
function launch(command, args, log, env = {}) {
  const fd = openSync(join(output, log), "a", 0o600);
  const child = spawn(command, args, {
    cwd: project,
    env: { ...process.env, ...env },
    stdio: ["ignore", fd, fd],
  });
  closeSync(fd);
  children.add(child);
  child.on("exit", () => children.delete(child));
  return child;
}
async function command(command, args, log, env = {}, ms = 120000) {
  const child = launch(command, args, log, env);
  const timer = setTimeout(() => child.kill("SIGKILL"), ms);
  try {
    const [code, signal] = await once(child, "exit");
    assert.equal(code, 0, `${log} failed (${signal ?? code}); inspect private log`);
  } finally {
    clearTimeout(timer);
  }
}
async function stop() {
  if (!server || server.exitCode !== null || server.signalCode !== null) return;
  const child = server;
  const exit = once(child, "exit");
  child.kill("SIGTERM");
  const timer = setTimeout(() => child.kill("SIGKILL"), 15000);
  try {
    await exit;
  } finally {
    clearTimeout(timer);
    server = undefined;
  }
}
async function start(port = 0) {
  if (existsSync(portPath)) unlinkSync(portPath); // Only this run's owned readiness file.
  server = launch(
    input.cli,
    [
      "server",
      c.appId,
      "--port",
      String(port),
      "--data-dir",
      join(c.state, "server"),
      "--bound-port-file",
      portPath,
      "--allow-local-first-auth",
    ],
    "server.log",
    {
      NODE_ENV: "production",
      JAZZ_ADMIN_SECRET: c.adminSecret,
      JAZZ_BACKEND_SECRET: c.backendSecret,
    },
  );
  for (let i = 0; i < 200; i++) {
    assert.equal(server.exitCode, null, "CLI exited before readiness");
    if (existsSync(portPath)) {
      const boundPort = Number(readFileSync(portPath, "utf8").trim());
      if (boundPort) {
        c.serverUrl = `http://127.0.0.1:${boundPort}`;
        try {
          const r = await fetch(`${c.serverUrl}/health`, { signal: AbortSignal.timeout(1000) });
          if (r.status < 500) return boundPort;
        } catch {}
      }
    }
    await delay(50);
  }
  throw new Error("CLI readiness timeout");
}
const watchdog = setTimeout(() => {
  for (const child of children) child.kill("SIGKILL");
  console.error("Whole-run deadline exceeded");
  process.exitCode = 1;
}, 300000);
try {
  emit("provenance", {
    sourceSha: input.sourceSha,
    packageVersion: input.packageVersion,
    previewRun: input.previewRun,
    cliSha256: input.cliSha256,
    nativeFingerprint: input.nativeFingerprint,
    packages: Object.fromEntries(
      Object.entries(input.packages).map(([name, p]) => [name, p.sha256]),
    ),
    server: "real CLI; fresh persistent local store",
  });
  const port = await start();
  writeFileSync(configPath, JSON.stringify(c), { mode: 0o600 });
  // Fixture module lives in the external installed project so TS resolves exact packages.
  const fixture = join(project, `acceptance-fixture-${randomUUID()}`);
  mkdirSync(fixture);
  writeFileSync(
    join(fixture, "schema.ts"),
    `import {schema as s} from 'jazz-tools';\nexport const app=s.defineApp({docs:s.table({label:s.string(),body:s.string(),metadata:s.json()},{}),denied:s.table({value:s.string()},{})});\nexport default app;\n`,
  );
  writeFileSync(
    join(fixture, "permissions.ts"),
    `import {schema as s} from 'jazz-tools';\nimport {app} from './schema';\nexport default s.definePermissions(app,({policy})=>{policy.docs.allowRead.always();policy.docs.allowInsert.always();policy.docs.allowUpdate.always();policy.docs.allowDelete.always();});\n`,
  );
  const cliJS = join(dirname(require.resolve("jazz-tools")), "cli.js");
  await command(process.execPath, [cliJS, "validate", "--schema-dir", fixture], "validate.log");
  for (const attempt of [1, 2])
    await command(
      process.execPath,
      [cliJS, "deploy", c.appId, "--schema-dir", fixture, "--server-url", c.serverUrl],
      `deploy-${attempt}.log`,
      { JAZZ_ADMIN_SECRET: c.adminSecret },
    );
  emit("cli-validate-and-schema-permissions-deploy");
  await command(process.execPath, [join(ownDir, "scenario.mjs"), configPath, "seed"], "seed.log");
  await stop();
  await command(
    process.execPath,
    [join(ownDir, "scenario.mjs"), configPath, "offline"],
    "offline.log",
  );
  await start(port);
  await command(
    process.execPath,
    [join(ownDir, "scenario.mjs"), configPath, "reconnect"],
    "reconnect.log",
  );
  emit("PASS", {
    scope:
      "local CLI validation/deploy, ordinary CRUD, backend/deny-read, large values, offline pending persistence and restart",
  });
  for (const gate of [
    "npm-create-scaffold",
    "external-jwt-and-negative-auth",
    "denied-write",
    "deterministic-in-flight-cancellation",
    "wire-v2",
    "browser-worker",
    "android",
    "ios",
    "cloud-image-pin-crud-sleep-wake-cleanup",
  ])
    emit("NOT_RUN", { gate });
} finally {
  clearTimeout(watchdog);
  await stop();
  for (const child of children) child.kill("SIGKILL");
}
